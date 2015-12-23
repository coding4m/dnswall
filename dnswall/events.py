"""

"""

import docker
import jsonselect

from dnswall.backend import *
from dnswall.operations import *


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

    docker_client = docker.AutoVersionClient(base_url=docker_url)
    try:

        docker_events = docker_client.events(decode=True, filters={'event': ['start', 'stop']})
        for event in docker_events:

            container = docker_client.inspect_container(jsonselect.select('.id', event))

            all_environments = jsonselect.select('.Config .Env', container) \
                               | collect(lambda env: env | split(pattern=r'=', maxsplit=1)) \
                               | collect(lambda env: env | as_tuple) \
                               | as_tuple \
                               | as_dict

            container_domain = jsonselect.select('.DOMAIN_NAME', all_environments)
            if not container_domain:
                continue

            event_status = jsonselect.select('.status', event)
            if event_status == 'stop':
                _unregister_domain(dnswall_url, container_domain)
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

            _register_domain(dnswall_url, container_domain, container_networks)
    except:
        # TODO
        pass


def _register_domain(backend, container_domain, container_networks):
    namespecs = container_networks \
                | collect(lambda item: NameSpec(host_ipv4=jsonselect.select('.IPAddress', item),
                                                host_ipv6=jsonselect.select('.GlobalIPv6Address', item))) \
                | as_list

    backend.register(container_domain, namespecs)


def _unregister_domain(backend, container_domain):
    backend.unregister(container_domain)
