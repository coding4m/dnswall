#!/usr/bin/env python

import argparse
import sys
import urlparse

from twisted.internet import reactor
from twisted.names import dns, server

from dnswall import loggers
from dnswall.backend import *
from dnswall.commons import *
from dnswall.resolver import *
from dnswall.version import current_version

__ADDRPAIR_LEN = 2
__BACKENDS = {"etcd": EtcdBackend}

_logger = loggers.get_logger('d.Daemon')


def _get_callargs():
    parser = argparse.ArgumentParser(prog='dnswall-daemon', description=current_version.desc)

    parser.add_argument('-backend', dest='backend', required=True,
                        help='which backend to use.')

    parser.add_argument('--addr', dest='addr', default='0.0.0.0:53',
                        help='address used to serve dns request. default is 0.0.0.0:53.')

    parser.add_argument('--servers', dest='servers', default='119.29.29.29:53,114.114.114.114:53',
                        help='nameservers used to forward request. default is 119.29.29.29:53,114.114.114.114:53')

    return parser.parse_args()


def main():
    callargs = _get_callargs()

    backend_url = callargs.backend
    backend_scheme = urlparse.urlparse(backend_url).scheme

    backend_cls = __BACKENDS.get(backend_scheme)
    if not backend_cls:
        print('ERROR: backend[type={}] not found.'.format(backend_scheme))
        sys.exit(1)

    backend = backend_cls(backend_url)

    dns_servers = [(it | split(r':')) for it in (callargs.servers | split(','))]
    dns_servers = [(it[0], it[1] | as_int) for it in dns_servers] | as_list
    dns_factory = server.DNSServerFactory(
        clients=[BackendResolver(backend=backend), ProxyResovler(servers=dns_servers)]
    )

    dns_addr = callargs.addr | split(r':')
    if len(dns_addr) != __ADDRPAIR_LEN:
        print('ERROR: addr must like 0.0.0.0:53 format.')
        sys.exit(1)

    # listen for serve dns request.
    dns_port, dns_host = dns_addr[1] | as_int, dns_addr[0]
    reactor.listenUDP(dns_port, dns.DNSDatagramProtocol(controller=dns_factory),
                      interface=dns_host)
    reactor.listenTCP(dns_port, dns_factory, interface=dns_host)

    _logger.w('waitting request on [tcp/udp] %s.', callargs.addr)
    reactor.run()


if __name__ == '__main__':
    raise SystemExit(main())
