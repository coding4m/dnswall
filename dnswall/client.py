#!/usr/bin/env python

import argparse
import json
import os
import sys
import traceback
import urlparse

from dnswall import loggers
from dnswall.backend import *
from dnswall.commons import *

__ADDRPAIR_LEN = 2
__BACKENDS = {"etcd": EtcdBackend}

_logger = loggers.get_logger('d.Client')


def _get_callargs():
    parser = argparse.ArgumentParser(prog='dnswall-client',
                                     epilog='''Run 'dnswall-client COMMAND -h' for more information on a command.''',
                                     description='dnswall client for names operations.')

    parser.add_argument('-backend', dest='backend',
                        help='which backend to use. if not set, use BACKEND env instead.')

    subparsers = parser.add_subparsers(help='avaliables commands.')

    subparser_ls = subparsers.add_parser('ls', help='ls names.')
    subparser_ls.add_argument('name', nargs='?',
                              help='list names start with name. if not set, list all.')
    subparser_ls.set_defaults(action=_backend_ls)

    subparser_rm = subparsers.add_parser('rm', help='rm names use a json format file.')
    subparser_rm.add_argument('json', help='names json file.')
    subparser_rm.set_defaults(action=_backend_rm)

    subparser_add = subparsers.add_parser('add', help='add names use a json format file.')
    subparser_add.add_argument('json', help='names json file.')
    subparser_add.set_defaults(action=_backend_add)
    return parser.parse_args()


def main():
    callargs = _get_callargs()

    backend_url = callargs.backend if callargs.backend else os.getenv('BACKEND')
    if not backend_url:
        print('ERROR: BACKEND env not set, use -backend BACKEND instead.')
        sys.exit(1)

    backend_scheme = urlparse.urlparse(backend_url).scheme

    backend_cls = __BACKENDS.get(backend_scheme)
    if not backend_cls:
        print('ERROR: backend[type={}] not found.'.format(backend_scheme))
        sys.exit(1)

    backend = backend_cls(backend_url)
    callargs.action(backend, callargs)


def _backend_ls(backend, callargs):
    namelists = backend.lookall(name=callargs.name)
    dictlists = namelists | collect(lambda it: it.to_dict()) | as_list
    print json.dumps(dictlists, indent=4, sort_keys=True)


def _backend_rm(backend, callargs):
    try:
        namelist = _parse_namelist(callargs)
        backend.unregister_all(namelist.name, namelist.nodes)
    except:
        traceback.print_exc()
    else:
        print('OK.')


def _parse_namelist(callargs):
    namejson = ''

    with open(callargs.json, 'r') as f:
        while True:
            namedata = f.read(1024)
            if not namedata:
                break
            namejson += namedata

    namelist = NameList.from_dict(json.loads(namejson))
    return namelist


def _backend_add(backend, callargs):
    try:
        namelist = _parse_namelist(callargs)
        backend.register_all(namelist.name, namelist.nodes)
    except:
        traceback.print_exc()
    else:
        print('OK.')


if __name__ == '__main__':
    raise SystemExit(main())
