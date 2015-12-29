"""

"""

import docker
import jsonselect

from dnswall import loggers
from dnswall import supervisor
from dnswall.backend import *
from dnswall.commons import *
from dnswall.errors import *

_logger = loggers.getlogger('d.e.Loop')


def loop(backend,
         docker_url,
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
    _events = client.events(decode=True, filters={'event': ['start', 'stop', 'pause', 'unpause']})

    # now loop containers.
    _handle_containers(backend, _get_containers(client))
    for _event in _events:
        # TODO when container destroy, we may lost the opportunity to unregister the container.
        _container = _get_container(client, _jsonselect(_event, '.id'))
        _handle_container(backend, _container)


def _get_containers(client):
    return client.containers(quiet=True, all=True) \
           | collect(lambda it: _jsonselect(it, '.Id')) \
           | collect(lambda it: _get_container(client, it))


def _handle_containers(backend, containers):
    for _container in containers:
        _handle_container(backend, _container)


def _get_container(client, container_id):
    return client.inspect_container(container_id)


def _handle_container(backend, container):
    try:
        container_environments = _jsonselect(container, '.Config .Env') \
                                 | collect(lambda it: it | split(r'=', maxsplit=1)) \
                                 | collect(lambda it: it | as_tuple) \
                                 | as_tuple \
                                 | as_dict

        container_domain = _jsonselect(container_environments, '.DOMAIN_NAME')
        if not container_domain:
            return

        interesting_network = _jsonselect(container_environments, '.DOMAIN_NETWORK')
        if not interesting_network:
            return

        container_network = _jsonselect(container, '.NetworkSettings .Networks .{}'.format(interesting_network))
        if not container_network:
            return

        container_id = _jsonselect(container, '.Id')
        container_status = _jsonselect(container, '.State .Status')
        if container_status not in ['paused', 'exited']:
            _register_container(backend, container_id, container_domain, container_network)
        else:
            _unregister_container(backend, container_id, container_domain, container_network)

    except BackendValueError:
        _logger.ex('handle container occurs BackendValueError, just ignore it.')
    except BackendError as e:
        raise e
    except:
        _logger.ex('handle container occurs error, just ignore it.')


def _jsonselect(obj, selector):
    return jsonselect.select(selector, obj)


def _register_container(backend, container_id, container_domain, container_network):
    _logger.w('register container[id=%s, domain_name=%s] to backend.',
              container_id, container_domain)

    name_item = NameItem(uuid=container_id,
                         host_ipv4=_jsonselect(container_network, '.IPAddress'),
                         host_ipv6=_jsonselect(container_network, '.GlobalIPv6Address'))
    backend.register(container_domain, name_item)


def _unregister_container(backend, container_id, container_domain, container_network):
    _logger.w('unregister container[id=%s, domain_name=%s] from backend.',
              container_id, container_domain)

    name_item = NameItem(uuid=container_id,
                         host_ipv4=_jsonselect(container_network, '.IPAddress'),
                         host_ipv6=_jsonselect(container_network, '.GlobalIPv6Address'))
    backend.unregister(container_domain, name_item)
