#!/usr/bin/env python

import argparse
import urlparse

from twisted.internet import reactor
from twisted.names import dns, server
from twisted.web.resource import Resource
from twisted.web.server import Site

from dnswall.backend import *
from dnswall.commons import *
from dnswall.errors import *
from dnswall.handler import *
from dnswall.resolver import *
from dnswall.version import current_version

__ADDRPAIR_LEN = 2
__BACKENDS = {"etcd": EtcdBackend}


def get_daemon_args():
    parser = argparse.ArgumentParser(prog='dnswall-daemon', description=current_version.desc)

    parser.add_argument('-backend', dest='backend', required=True,
                        help='which backend to use.')

    parser.add_argument('-nameservers', dest='nameservers', default='119.29.29.29,114.114.114.114',
                        help='nameservers used to forward request. default is 119.29.29.29,114.114.114.114')
    parser.add_argument('-addr', dest='addr', default='0.0.0.0:53',
                        help='address used to serve dns request. default is 0.0.0.0:53.')
    parser.add_argument('-http-addr', dest='http_addr', default='0.0.0.0:9090',
                        help='address used to serve http request. default is 0.0.0.0:9090.')
    # return parser.parse_args(
    #     ['-backend', 'etcd://127.0.0.1:4001/?pattern=workplus.io', '-addr', '0.0.0.0:10053']
    # )
    return parser.parse_args()


def main():
    daemon_args = get_daemon_args()

    backend_url = daemon_args.backend
    backend_scheme = urlparse.urlparse(backend_url).scheme

    backend_cls = __BACKENDS.get(backend_scheme)
    if not backend_cls:
        raise BackendNotFound("backend[type={}] not found.".format(backend_scheme))

    backend = backend_cls(backend_options=backend_url)

    dns_servers = daemon_args.nameservers | split(pattern=r',|\s')
    dns_factory = server.DNSServerFactory(
        clients=[BackendResolver(backend=backend), ProxyResovler(servers=dns_servers)]
    )

    dns_addrpair = daemon_args.addr | split(pattern=r':')
    if len(dns_addrpair) != __ADDRPAIR_LEN:
        raise ValueError("addr must like 0.0.0.0:53 format.")

    # listen for serve dns request.

    dns_port, dns_host = (dns_addrpair[1] | as_int, dns_addrpair[0],)
    reactor.listenUDP(dns_port, dns.DNSDatagramProtocol(controller=dns_factory),
                      interface=dns_host)
    reactor.listenTCP(dns_port, dns_factory, interface=dns_host)

    # listen for serve http request.

    http_addrpair = daemon_args.http_addr | split(pattern=r':')
    if len(http_addrpair) != __ADDRPAIR_LEN:
        raise ValueError("http addr must like 0.0.0.0:9090 format.")

    http_resource = Resource()

    version_resource = VersionResource(name=current_version.package, version=current_version.short())
    http_resource.putChild('', version_resource)
    http_resource.putChild('_version', version_resource)
    http_resource.putChild('names', NameResource(backend=backend))

    http_port, http_host = (http_addrpair[1] | as_int, http_addrpair[0],)
    reactor.listenTCP(http_port, Site(http_resource), interface=http_host)

    reactor.run()


if __name__ == '__main__':
    raise SystemExit(main())
