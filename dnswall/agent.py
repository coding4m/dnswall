#!/usr/bin/env python

import argparse

from dnswall import events
from dnswall.errors import *
from dnswall.version import current_version


def _get_daemon_args():
    parser = argparse.ArgumentParser(prog='dnswall-agent', description=current_version.desc)

    parser.add_argument('-backend', dest='backend', required=True,
                        help='which backend to use.')

    parser.add_argument('-docker-url', dest='docker_url', default='unix:///var/run/docker.sock',
                        help='')

    parser.add_argument('--docker-tlsverify', dest='docker_tls_verify', default=False, action='store_true')
    parser.add_argument('--docker-tlsca', dest='docker_tls_ca')
    parser.add_argument('--docker-tlskey', dest='docker_tls_key')
    parser.add_argument('--docker-tlscert', dest='docker_tls_cert')
    return parser.parse_args()


def main():
    daemon_args = _get_daemon_args()
    backend_url = daemon_args.backend
    backend_scheme = urlparse.urlparse(backend_url).scheme

    backend_cls = __BACKENDS.get(backend_scheme)
    if not backend_cls:
        raise BackendNotFound("backend[type={}] not found.".format(backend_scheme))

    backend = backend_cls(backend_options=backend_url)
    events.loop(backend=backend,
                docker_url=daemon_args.docker_url,
                docker_tls_verify=daemon_args.docker_tls_verify,
                docker_tls_ca=daemon_args.docker_tls_ca,
                docker_tls_key=daemon_args.docker_tls_key,
                docker_tls_cert=daemon_args.docker_tls_cert)


if __name__ == '__main__':
    raise SystemExit(main())
