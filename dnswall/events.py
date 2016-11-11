"""

"""
import sched
import time

import docker

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

    _logger.w('start and supervise event loop.')
    client = docker.AutoVersionClient(base_url=docker_url)
    supervisor.supervise(min_seconds=2, max_seconds=64)(_heartbeat)(backend, client)


def _heartbeat(backend, client):
    _heartbeat_containers(backend, client)

    _schd = sched.scheduler(time.time, time.sleep)
    while True:
        _schd.enter(30, 0, _heartbeat_containers, (backend, client))
        _schd.run()


def _heartbeat_containers(backend, client):
    # list all running containers.
    containers = client.containers(quiet=True) \
                 | collect(lambda it: it | select_path('.Id')) \
                 | collect(lambda it: client.inspect_container(it))
    for container in containers:
        _heartbeat_container(backend, container)


def _heartbeat_container(backend, container):
    try:

        container_id = container | select_path('.Id')
        container_status = container | select_path('.State .Status')

        # ignore tty container.
        is_tty_container = container | select_path('.Config .Tty')
        if is_tty_container:
            _logger.w('ignore tty container[id=%s, status=%s]', container_id, container_status)
            return

        container_environments = container | select_path('.Config .Env')
        if not container_environments:
            return

        container_environments = container_environments \
                                 | collect(lambda it: it | split(r'=', maxsplit=1)) \
                                 | collect(lambda it: it | as_tuple) \
                                 | as_tuple \
                                 | as_dict

        container_domain = container_environments | select_path('.DOMAIN_NAME')
        if not container_domain:
            return

        container_ipv4_addr = container_environments | select_path('.DOMAIN_IPV4_ADDR')
        container_ipv6_addr = container_environments | select_path('.DOMAIN_IPV6_ADDR')

        container_network = container_environments | select_path('.DOMAIN_NETWORK')
        if container_network:
            network_ipv4_selector = \
                '.NetworkSettings .Networks .{} .IPAddress'.format(container_network)
            network_ipv6_selector = \
                '.NetworkSettings .Networks .{} .GlobalIPv6Address'.format(container_network)

            container_ipv4_addr = container | select_path(network_ipv4_selector)
            container_ipv6_addr = container | select_path(network_ipv6_selector)

        if not container_ipv4_addr and not container_ipv6_addr:
            _logger.w(
                '''ignore container[id=%s, domain_name=%s] because addrs not found.''',
                container_id,
                container_domain
            )
            return

        _logger.d('heartbeat container[id=%s, domain_name=%s] to backend.', container_id, container_domain)
        name_item = DomainItem(uuid=container_id,
                               host_ipv4=container_ipv4_addr,
                               host_ipv6=container_ipv6_addr)
        backend.register(container_domain, name_item, ttl=60)

    except BackendValueError:
        _logger.ex('heartbeat container occurs BackendValueError, just ignore it.')
    except BackendError as e:
        raise e
    except:
        _logger.ex('heartbeat container occurs error, just ignore it.')
