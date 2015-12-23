"""

"""
import functools
import time

import docker
import jsonselect

from dnswall.backend import *
from dnswall.operations import *


def _supervise(min_seconds=None, max_seconds=None):
    """

    :param min_seconds:
    :param max_seconds:
    :return:
    """

    def decorator(function):
        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            retry_seconds = min_seconds
            next_retry_seconds = retry_seconds
            while True:
                try:
                    return function(*args, **kwargs)
                except:
                    # TODO
                    time.sleep(retry_seconds)

                    next_retry_seconds *= 2
                    if next_retry_seconds > max_seconds:
                        next_retry_seconds = min_seconds
                    retry_seconds = next_retry_seconds

        return wrapped

    return decorator


def loop(backend=None,
         docker_url=None,
         docker_tls_verify=False,
         docker_tls_ca=None,
         docker_tls_cert=None):
    """

    :param backend:
    :param docker_url:
    :param docker_tls_verify:
    :param docker_tls_ca:
    :param docker_tls_cert:
    :return:
    """

    # TODO
    _client = docker.AutoVersionClient(base_url=docker_url)
    _supervise(min_seconds=2, max_seconds=64)(_loop_events)(backend, _client)


def _loop_events(backend, client):
    # loop all containers first.
    _loop_containers(backend, client)

    # consume real time events.
    events = client.events(decode=True, filters={'event': ['destroy', 'die', 'start', 'stop', 'pause']})
    for _ in events:
        # TODO when container destroy, we may be lost the opportunity to unregister the container.
        _loop_containers(backend, client)


def _loop_containers(backend, client):
    containers = client.containers(quiet=True, all=True) \
                 | collect(lambda container: jsonselect.select('.Id', container)) \
                 | collect(lambda container_id: client.inspect_container(container_id))
    for container in containers:
        _handle_container(backend, container)


def _handle_container(backend, container):
    container_environments = jsonselect.select('.Config .Env', container) \
                             | collect(lambda env: env | split(pattern=r'=', maxsplit=1)) \
                             | collect(lambda env: env | as_tuple) \
                             | as_tuple \
                             | as_dict

    container_domain = jsonselect.select('.DOMAIN_NAME', container_environments)
    if not container_domain:
        return

    container_status = jsonselect.select('.State .Status', container)
    if container_status in ['paused', 'exited']:
        _unregister_container(backend, container_domain)
        return

    interesting_networks = jsonselect.select('.DOMAIN_NETWORKS', container_environments) \
                           | split(pattern=r',|\s') \
                           | as_tuple

    if not interesting_networks:
        return

    all_container_networks = jsonselect.select('.NetworkSettings .Networks', container)
    if not all_container_networks:
        return

    container_networks = all_container_networks.items() \
                         | select(lambda item: item[0] in interesting_networks) \
                         | collect(lambda item: item[1]) \
                         | as_list

    if not container_networks:
        return

    _register_container(backend, container_domain, container_networks)


def _register_container(backend, container_domain, container_networks):
    namespecs = container_networks \
                | collect(lambda item: NameSpec(host_ipv4=jsonselect.select('.IPAddress', item),
                                                host_ipv6=jsonselect.select('.GlobalIPv6Address', item))) \
                | as_list

    backend.register(container_domain, namespecs)


def _unregister_container(backend, container_domain):
    backend.unregister(container_domain)
