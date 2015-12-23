"""

"""
import time

import docker
import jsonselect

from dnswall.backend import *
from dnswall.operations import *


class _BackOff(object):
    def __init__(self, min_seconds=None, max_seconds=None, func=None):
        self._min_seconds = min_seconds
        self._max_seconds = max_seconds
        self._func = func

    def __call__(self, *args, **kwargs):

        backoff_seconds = self._min_seconds
        while True:
            try:
                next_backoff_seconds = backoff_seconds * 2
                backoff_seconds = next_backoff_seconds \
                    if next_backoff_seconds <= self._max_seconds \
                    else self._min_seconds
                return self._func(*args, **kwargs)
            except:
                # TODO
                time.sleep(backoff_seconds)


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

    client = docker.AutoVersionClient(base_url=docker_url)
    _BackOff(min_seconds=2, max_seconds=64, func=_loop_event)(backend, client)


def _loop_event(backend, client):
    _events = client.events(decode=True, filters={'event': ['start', 'stop']})
    for _event in _events:

        container = client.inspect_container(jsonselect.select('.id', _event))

        all_environments = jsonselect.select('.Config .Env', container) \
                           | collect(lambda env: env | split(pattern=r'=', maxsplit=1)) \
                           | collect(lambda env: env | as_tuple) \
                           | as_tuple \
                           | as_dict

        container_domain = jsonselect.select('.DOMAIN_NAME', all_environments)
        if not container_domain:
            continue

        event_status = jsonselect.select('.status', _event)
        if event_status == 'stop':
            _unregister_domain(backend, container_domain)
            continue

        all_interesting_networks = jsonselect.select('.DOMAIN_NETWORKS', all_environments) \
                                   | split(pattern=r',|\s') \
                                   | as_tuple

        if not all_interesting_networks:
            continue

        all_container_networks = jsonselect.select('.NetworkSettings .Networks', container)
        if not all_container_networks:
            continue

        container_networks = all_container_networks.items() \
                             | select(lambda item: item[0] in all_interesting_networks) \
                             | collect(lambda item: item[1]) \
                             | as_list

        if not container_networks:
            continue

        _register_domain(backend, container_domain, container_networks)


def _register_domain(backend, container_domain, container_networks):
    namespecs = container_networks \
                | collect(lambda item: NameSpec(host_ipv4=jsonselect.select('.IPAddress', item),
                                                host_ipv6=jsonselect.select('.GlobalIPv6Address', item))) \
                | as_list

    backend.register(container_domain, namespecs)


def _unregister_domain(backend, container_domain):
    backend.unregister(container_domain)
