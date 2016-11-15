#!/usr/bin/env python

import argparse
import os
import sys
import urlparse

from twisted.internet import reactor
from twisted.names import dns, server

from dnswall import constants
from dnswall import loggers
from dnswall.backend import *
from dnswall.commons import *
from dnswall.resolver import *

__ADDRPAIR_LEN = 2
__BACKENDS = {"etcd": EtcdBackend}

_logger = loggers.getlogger('d.Daemon')


def _get_callargs():
    parser = argparse.ArgumentParser(prog='dnswall-daemon',
                                     description='dns server for docker containers using key-value backends.')

    parser.add_argument('-backend', dest='backend',
                        default=os.getenv(constants.BACKEND_ENV),
                        help='which backend to use.')

    parser.add_argument('--addr', dest='addr',
                        default=os.getenv(constants.ADDR_ENV, '0.0.0.0:53'),
                        help='address used to serve dns request. default is 0.0.0.0:53.')

    parser.add_argument('--patterns', dest='patterns',
                        default=os.getenv(constants.PATTERNS_ENV, 'dnswall.local'),
                        help='patterns of domain name handle by backend.')

    parser.add_argument('--servers', dest='servers',
                        default=os.getenv(constants.SERVERS_ENV, '119.29.29.29:53,114.114.114.114:53'),
                        help='nameservers used to forward request. default is 119.29.29.29:53,114.114.114.114:53')

    return parser.parse_args()


def main():
    callargs = _get_callargs()

    patterns = callargs.patterns | split('[,;\s]')
    if not patterns:
        _logger.e('patterns must not be empty, daemon exit.')
        sys.exit(1)

    backend_url = callargs.backend
    if not backend_url:
        _logger.e('%s env not set, use -backend instead, daemon exit.', constants.BACKEND_ENV)
        sys.exit(1)

    backend_type = urlparse.urlparse(backend_url | strip).scheme
    backend_cls = __BACKENDS.get(backend_type)
    if not backend_cls:
        _logger.e('backend[type=%s] not found, daemon exit.', backend_type)
        sys.exit(1)

    backend = backend_cls(backend_url, patterns=patterns)
    dns_servers = [(it | split(':')) for it in (callargs.servers | split(','))]
    dns_servers = [(it[0], it[1] | as_int) for it in dns_servers] | as_list
    dns_factory = server.DNSServerFactory(
        clients=[
            BackendResolver(backend=backend),
            ProxyResovler(resolv='/etc/resolv.conf'),
            ProxyResovler(servers=dns_servers)
        ]
    )

    dns_addr = callargs.addr | split(':')
    if len(dns_addr) != __ADDRPAIR_LEN:
        _logger.e('addr must like 0.0.0.0:53 format, daemon exit.')
        sys.exit(1)

    # listen for serve dns request.
    dns_port, dns_host = dns_addr[1] | as_int, dns_addr[0]
    reactor.listenTCP(dns_port, dns_factory, interface=dns_host)
    reactor.listenUDP(dns_port, dns.DNSDatagramProtocol(controller=dns_factory), interface=dns_host)

    _logger.w('waitting request on [tcp/udp] %s.', callargs.addr)
    reactor.run()


if __name__ == '__main__':
    raise SystemExit(main())
