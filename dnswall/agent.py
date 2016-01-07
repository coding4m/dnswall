#!/usr/bin/env python

import argparse
import os
import sys
import urlparse

from dnswall import constants
from dnswall import events
from dnswall import loggers
from dnswall.backend import *

__BACKENDS = {"etcd": EtcdBackend}

_logger = loggers.getlogger('d.Agent')


def _get_callargs():
    parser = argparse.ArgumentParser(prog='dnswall-agent',
                                     description='monitor agent for docker containers.')

    parser.add_argument('-backend', dest='backend',
                        default=os.getenv(constants.BACKEND_ENV),
                        help='which backend to use.')

    parser.add_argument('-docker-url', dest='docker_url',
                        default=os.getenv(constants.DOCKER_URL_ENV, 'unix:///var/run/docker.sock'),
                        help='docker daemon addr, default is unix:///var/run/docker.sock.')

    parser.add_argument('--docker-tlsverify', dest='docker_tls_verify',
                        default=os.getenv(constants.DOCKER_TLSVERIFY_ENV, False), action='store_true')
    parser.add_argument('--docker-tlsca', dest='docker_tls_ca',
                        default=os.getenv(constants.DOCKER_TLSCA_ENV))
    parser.add_argument('--docker-tlskey', dest='docker_tls_key',
                        default=os.getenv(constants.DOCKER_TLSKEY_ENV))
    parser.add_argument('--docker-tlscert', dest='docker_tls_cert',
                        default=os.getenv(constants.DOCKER_TLSCERT_ENV))

    return parser.parse_args()


def main():
    callargs = _get_callargs()

    backend_url = callargs.backend
    if not backend_url:
        _logger.e('%s env not set, use -backend instead, agent exit.', constants.BACKEND_ENV)
        sys.exit(1)

    backend_scheme = urlparse.urlparse(backend_url).scheme
    backend_cls = __BACKENDS.get(backend_scheme)
    if not backend_cls:
        _logger.e('backend[type=%s] not found, agent exit.', backend_scheme)
        sys.exit(1)

    backend = backend_cls(backend_url)
    events.loop(backend, callargs.docker_url)


if __name__ == '__main__':
    raise SystemExit(main())
