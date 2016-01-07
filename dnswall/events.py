"""

"""
import sched
import time

import docker
import jsonselect

from dnswall import loggers
from dnswall import supervisor
from dnswall.backend import *
from dnswall.commons import *
from dnswall.errors import *

_logger = loggers.getlogger('d.e.Loop')


def loop(backend, docker_url):
    """

    :param backend:
    :param docker_url:
    :return:
    """

    # TODO
    _logger.w('start and supervise event loop.')
    client = docker.AutoVersionClient(base_url=docker_url)
    supervisor.supervise(min_seconds=2, max_seconds=64)(_event_loop)(backend, client)


def _event_loop(backend, client):
    _heartbeat_containers(backend, client)

    _schd = sched.scheduler(time.time, time.sleep)
    while True:
        _schd.enter(30, 0, _heartbeat_containers, (backend, client))
        _schd.run()


def _heartbeat_containers(backend, client):
    # list all running containers.
    containers = client.containers(quiet=True) \
                 | collect(lambda it: _jsonselect(it, '.Id')) \
                 | collect(lambda it: client.inspect_container(it))
    for container in containers:
        _heartbeat_container(backend, container)


def _heartbeat_container(backend, container):
    try:

        container_id = _jsonselect(container, '.Id')
        container_status = _jsonselect(container, '.State .Status')

        # ignore tty container.
        is_tty_container = _jsonselect(container, '.Config .Tty')
        if is_tty_container:
            _logger.w('ignore tty container[id=%s, status=%s]', container_id, container_status)
            return

        container_environments = _jsonselect(container, '.Config .Env')
        if not container_environments:
            return

        container_environments = container_environments \
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

        container_network_selector = '.NetworkSettings .Networks .{}'.format(interesting_network)
        container_network = _jsonselect(container, container_network_selector)
        if not container_network:
            return

        _logger.d('heartbeat container[id=%s, domain_name=%s] to backend.', container_id, container_domain)
        name_item = NameItem(uuid=container_id,
                             host_ipv4=_jsonselect(container_network, '.IPAddress'),
                             host_ipv6=_jsonselect(container_network, '.GlobalIPv6Address'))
        backend.register(container_domain, name_item, ttl=60)

    except BackendValueError:
        _logger.ex('heartbeat container occurs BackendValueError, just ignore it.')
    except BackendError as e:
        raise e
    except:
        _logger.ex('heartbeat container occurs error, just ignore it.')


def _jsonselect(obj, selector):
    return jsonselect.select(selector, obj)
