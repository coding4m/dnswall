from twisted.internet import defer, threads
from twisted.names import dns
from twisted.names.client import Resolver as ProxyResovler

from dnswall import loggers
from dnswall.commons import *

__all__ = ["BackendResolver", "ProxyResovler"]

EMPTY_ANSWERS = [], [], []


class BackendResolver(object):
    """

    """

    def __init__(self, backend=None):
        """

        :param backend:
        :return:
        """
        self._backend = backend
        self._logger = loggers.getlogger('d.r.BackendResolver')

    def query(self, query, timeout=None):
        """

        :param query:
        :param timeout:
        :return:
        """

        qname = query.name.name
        qtype = query.type

        if not self._backend.supports(qname):
            self._logger.d('unsupported query name [%s], just forward it.', qname)
            return defer.fail(dns.DomainError())

        if qtype not in (dns.A, dns.AAAA):
            self._logger.d('unsupported query type [%d], just forward it.', qtype)
            return defer.fail(dns.DomainError())

        def _lookup_backend(backend, logger, qn, qt):
            """

            :param backend:
            :param qn:
            :param qt:
            :return: three-tuple(answers, authorities, additional)
                        of lists of twisted.names.dns.RRHeader instances.
            """

            import random
            try:

                name_detail = backend.lookup(qn)
            except:
                logger.ex('lookup name %s occurs error, just ignore and forward it.', qn)
                return EMPTY_ANSWERS

            if not name_detail.items:
                return EMPTY_ANSWERS

            if qt == dns.A:
                answers = name_detail.items \
                          | select(lambda it: it.host_ipv4) \
                          | collect(lambda it: it.host_ipv4) \
                          | as_set \
                          | collect(lambda it: dns.Record_A(address=it)) \
                          | collect(lambda record_a: dns.RRHeader(name=qn, payload=record_a)) \
                          | as_list

                random.shuffle(answers)
                return answers, [], []

            else:
                answers = name_detail.items \
                          | select(lambda it: it.host_ipv6) \
                          | collect(lambda it: it.host_ipv6) \
                          | as_set \
                          | collect(lambda it: dns.Record_AAAA(address=it)) \
                          | collect(lambda record_aaaa: dns.RRHeader(name=qn, payload=record_aaaa)) \
                          | as_list

                random.shuffle(answers)
                return answers, [], []

        return threads.deferToThread(_lookup_backend, self._backend, self._logger, qname, qtype)
