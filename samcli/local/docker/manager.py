"""
Provides classes that interface with Docker to create, execute and manage containers.
"""

import logging

import sys
from contextlib import contextmanager

import docker
import requests

from samcli.lib.utils.stream_writer import StreamWriter

LOG = logging.getLogger(__name__)

_DEFAULT_POOL_LIMIT = 4

class DockerContainerPool(object):
    def __init__(self, size_limit=_DEFAULT_POOL_LIMIT):
        self._pool_limit = size_limit
        self._keep_empty = size_limit == 0
        self._free_containers = {}
        self._containers = {}

    def register_container(self, runtime, container_id, claimed=False):
        if self._keep_empty:
            return
        LOG.info("Registering new container for runtime %s...", runtime)
        if runtime not in self._free_containers:
            self._free_containers[runtime] = set()
        if not claimed:
            self._free_containers[runtime].add(container_id)
        self._containers[container_id] = {
            'runtime': runtime
        }

    def deregister_container(self, container_id):
        runtime = self._containers.get(container_id, {}).get('runtime')
        if not runtime:
            return
        self._free_containers[runtime].discard(container_id)
        del self._containers[container_id]

    def has_available_containers(self, runtime):
        return len(self._free_containers[runtime]) > 0

    def claim_container(self, runtime):
        LOG.info("Claiming container for runtime %s...", runtime)
        try:
            container_id = self._free_containers[runtime].pop()
            LOG.info("Claiming container for runtime %s: %s...", runtime, container_id)
            return container_id
        except KeyError:
            LOG.info("No container found for runtime %s...", runtime)
            return None

    def release_container(self, container_id):
        runtime = self._containers.get(container_id, {}).get('runtime')
        LOG.info("Releasing container for runtime %s: %s...", runtime, container_id)
        if not runtime:
            return
        self._free_containers[runtime].add(container_id)



class ContainerManager(object):
    """
    This class knows how to interface with Docker to create, execute and manage the container's life cycle. It can
    run multiple containers in parallel, and also comes with the ability to reuse existing containers in order to
    serve requests faster. It is also thread-safe.
    """

    def __init__(self,
                 docker_network_id=None,
                 docker_client=None,
                 skip_pull_image=False):
        """
        Instantiate the container manager

        :param docker_network_id: Optional Docker network to run this container in.
        :param docker_client: Optional docker client object
        :param bool skip_pull_image: Should we pull new Docker container image?
        """

        self.skip_pull_image = skip_pull_image
        self.docker_network_id = docker_network_id
        self.docker_client = docker_client or docker.from_env()

        self._container_pool_size_limit = 0 if not skip_pull_image else _DEFAULT_POOL_LIMIT
        self.container_pool = DockerContainerPool(self._container_pool_size_limit)

    @property
    def is_docker_reachable(self):
        """
        Checks if Docker daemon is running. This is required for us to invoke the function locally

        Returns
        -------
        bool
            True, if Docker is available, False otherwise
        """
        try:
            self.docker_client.ping()

            return True

        # When Docker is not installed, a request.exceptions.ConnectionError is thrown.
        except (docker.errors.APIError, requests.exceptions.ConnectionError):
            LOG.debug("Docker is not reachable", exc_info=True)
            return False

    def run(self, container, input_data=None, warm=False, stdout=None, stderr=None):
        """
        Create and run a Docker container based on the given configuration.

        :param samcli.local.docker.container.Container container: Container to create and run
        :param input_data: Optional. Input data sent to the container through container's stdin.
        :param bool warm: Indicates if an existing container can be reused. Defaults False ie. a new container will
            be created for every request.
        :raises DockerImagePullFailedException: If the Docker image was not available in the server
        """

        if warm:
            raise ValueError("The facility to invoke warm container does not exist")

        image_name = container.image

        is_image_local = self.has_image(image_name)

        # Skip Pulling a new image if: a) Image name is samcli/lambda OR b) Image is available AND
        # c) We are asked to skip pulling the image
        if (is_image_local and self.skip_pull_image) or image_name.startswith('samcli/lambda'):
            LOG.info("Requested to skip pulling images ...\n")
        else:
            try:
                self.pull_image(image_name)
            except DockerImagePullFailedException:
                if not is_image_local:
                    raise DockerImagePullFailedException(
                        "Could not find {} image locally and failed to pull it from docker.".format(image_name))

                LOG.info(
                    "Failed to download a new %s image. Invoking with the already downloaded image.", image_name)

        container_id = self.container_pool.claim_container(container.runtime)
        container.id = container_id

        if not container.is_running():
            self.container_pool.deregister_container(container_id)
            container.delete()
            container.id = None

        if not container.is_created():
            # Create the container first before running.
            # Create the container in appropriate Docker network
            container.network_id = self.docker_network_id
            container.create()
            container.bootstrap()
            self.container_pool.register_container(container.runtime, container.id, claimed=True)

        container.start(input_data=input_data, stdout=stdout, stderr=stderr)

    def stop(self, container):
        """
        Stop and delete the container

        :param samcli.local.docker.container.Container container: Container to stop
        """
        self.container_pool.release_container(container.id)
        if self._container_pool_size_limit == 0:
            container.delete()

    def pull_image(self, image_name, stream=None):
        """
        Ask Docker to pull the container image with given name.

        Parameters
        ----------
        image_name str
            Name of the image
        stream samcli.lib.utils.stream_writer.StreamWriter
            Optional stream writer to output to. Defaults to stderr

        Raises
        ------
        DockerImagePullFailedException
            If the Docker image was not available in the server
        """
        stream_writer = stream or StreamWriter(sys.stderr)

        try:
            result_itr = self.docker_client.api.pull(image_name, stream=True, decode=True)
        except docker.errors.APIError as ex:
            LOG.debug("Failed to download image with name %s", image_name)
            raise DockerImagePullFailedException(str(ex))

        # io streams, especially StringIO, work only with unicode strings
        stream_writer.write(u"\nFetching {} Docker container image...".format(image_name))

        # Each line contains information on progress of the pull. Each line is a JSON string
        for _ in result_itr:
            # For every line, print a dot to show progress
            stream_writer.write(u'.')
            stream_writer.flush()

        # We are done. Go to the next line
        stream_writer.write(u"\n")

    def has_image(self, image_name):
        """
        Is the container image with given name available?

        :param string image_name: Name of the image
        :return bool: True, if image is available. False, otherwise
        """

        try:
            self.docker_client.images.get(image_name)
            return True
        except docker.errors.ImageNotFound:
            return False


class DockerImagePullFailedException(Exception):
    pass
