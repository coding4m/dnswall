#!/usr/bin/env python

import argparse
import atexit

__PROGRAM_NAME = 'dnswall'
__PROGRAM_DESC = 'dnswall python client.'


def main(cmd_args):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=__PROGRAM_NAME, description=__PROGRAM_DESC)
    parser.add_argument('-H', '--host', dest='dnswall_host', help='which host dnswall daemon on.')

    subparsers = parser.add_subparsers()

    lscmd_parser = subparsers.add_parser('ls', help='list name records.')
    lscmd_parser.add_argument('query')
    lscmd_parser.add_argument('-t', '--type', dest="type")

    rmcmd_parser = subparsers.add_parser('rm', help='remove name records.')
    addcmd_parser = subparsers.add_parser('add', help='add new name record.')
    parser.parse_args(['ls', '--help'])
    pass
