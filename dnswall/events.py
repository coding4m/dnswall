"""

"""

import docker
import jsonselect

from dnswall import loggers
from dnswall import supervisor
from dnswall.backend import *
from dnswall.commons import *

_logger = loggers.get_logger('d.e.Loop')


def loop(backend=None,
         docker_url=None,
         docker_tls_verify=False,
         docker_tls_ca=None,
         docker_tls_key=None,
         docker_tls_cert=None):
    """

    :param backend:
    :param docker_url:
    :param docker_tls_verify:
    :param docker_tls_ca:
    :param docker_tls_key:
    :param docker_tls_cert:
    :return:
    """

    # TODO
    _client = docker.AutoVersionClient(base_url=docker_url)
    _logger.w('start and supervise event loop.')
    supervisor.supervise(min_seconds=2, max_seconds=64)(_event_loop)(backend, _client)


def _event_loop(backend, client):
    # consume real time events first.
    events = client.events(decode=True, filters={'event': ['start', 'stop', 'pause', 'unpause']})

    # now loop containers.
    _handle_containers(backend, _get_containers(client))
    for event in events:
        # TODO when container destroy, we may lost the opportunity to unregister the container.
        _handle_container(backend, _get_container(client, jsonselect.select('.id', event)))


def _get_containers(client):
    return client.containers(quiet=True, all=True) \
           | collect(lambda container: _jsonselect(container, '.Id')) \
           | collect(lambda container_id: _get_container(client, container_id))


def _handle_containers(backend, containers):
    for container in containers:
        _handle_container(backend, container)


def _get_container(client, container_id):
    return client.inspect_container(container_id)


def _handle_container(backend, container):
    container_environments = _jsonselect(container, '.Config .Env') \
                             | collect(lambda env: env | split(r'=', maxsplit=1)) \
                             | collect(lambda env: env | as_tuple) \
                             | as_tuple \
                             | as_dict

    container_domain = _jsonselect(container_environments, '.DOMAIN_NAME')
    if not container_domain:
        return

    container_status = _jsonselect(container, '.State .Status')
    if container_status in ['paused', 'exited']:
        _unregister_container(backend, container_domain)
        return

    interesting_networks = _jsonselect(container_environments, '.DOMAIN_NETWORKS') \
                           | split(pattern=r',|\s') \
                           | as_tuple

    if not interesting_networks:
        return

    all_container_networks = _jsonselect(container, '.NetworkSettings .Networks')
    if not all_container_networks:
        return

    container_networks = all_container_networks.items() \
                         | select(lambda item: item[0] in interesting_networks) \
                         | collect(lambda item: item[1]) \
                         | as_list

    if not container_networks:
        return

    _register_container(backend, container_domain, container_networks)


def _jsonselect(obj, selector):
    return jsonselect.select(selector, obj)


def _register_container(backend, container_domain, container_networks):
    _logger.w('register container[domain_name=%s] to backend.', container_domain)

    nodes = container_networks \
                | collect(lambda item: (_jsonselect(item, '.IPAddress'),
                                        _jsonselect(item, '.GlobalIPv6Address'),)) \
                | collect(lambda item: NameNode(host_ipv4=item[0], host_ipv6=item[1])) \
                | as_list

    backend.register(container_domain, nodes)


def _unregister_container(backend, container_domain):
    _logger.w('unregister container[domain_name=%s] from backend.', container_domain)
    backend.unregister(container_domain)
