from twisted.internet import defer, threads
from twisted.names import dns
from twisted.names.client import Resolver as ProxyResovler

from dnswall import loggers
from dnswall.commons import *

__all__ = ["BackendResolver", "ProxyResovler"]


class BackendResolver(object):
    """

    """

    def __init__(self, backend=None):
        """

        :param backend:
        :return:
        """
        self._backend = backend
        self._logger = loggers.get_logger('d.r.BackendResolver')

    def query(self, query, timeout=None):
        """

        :param query:
        :param timeout:
        :return:
        """

        qname = query.name.name
        qtype = query.type

        if not self._backend.supports(qname):
            self._logger.w('unsupported query name [%s], forward it.', qname)
            return defer.fail(dns.DomainError())

        # only supports A and AAAA qtype.
        if qtype not in (dns.A, dns.AAAA):
            self._logger.w('unsupported query type [%d], forward it.', qtype)
            return defer.fail(dns.DomainError())

        def _lookup_backend(backend, qn, qt):
            """

            :param backend:
            :param qn:
            :param qt:
            :return: three-tuple(answers, authorities, additional)
                        of lists of twisted.names.dns.RRHeader instances.
            """
            namerecord = backend.lookup(qn)
            if not namerecord.specs:
                return [], [], []

            if qt == dns.A:
                answers = namerecord.specs \
                          | select(lambda spec: spec.host_ipv4 is not None) \
                          | collect(lambda spec: dns.Record_A(address=spec.host_ipv4)) \
                          | collect(lambda record_a: dns.RRHeader(name=qn, payload=record_a)) \
                          | as_list

                return answers, [], []

            else:
                answers = namerecord.specs \
                          | select(lambda spec: spec.host_ipv6 is not None) \
                          | collect(lambda spec: dns.Record_AAAA(address=spec.host_ipv6)) \
                          | collect(lambda record_aaaa: dns.RRHeader(name=qn, payload=record_aaaa)) \
                          | as_list

                return answers, [], []

        return threads.deferToThread(_lookup_backend, self._backend, qname, qtype)
