import abc
import json
import urlparse

import etcd
import jsonselect

from dnswall import loggers
from dnswall.commons import *
from dnswall.errors import *

__all__ = ["NameNode", "NameRecord", "Backend", "EtcdBackend"]


class NameNode(object):
    """

    """

    def __init__(self, host_ipv4=None, host_ipv6=None):
        """

        :param host_ipv4:
        :param host_ipv6:
        :return:
        """
        self._host_ipv4 = host_ipv4
        self._host_ipv6 = host_ipv6

    @property
    def host_ipv4(self):
        """

        :return:
        """
        return self._host_ipv4

    @property
    def host_ipv6(self):
        """

        :return:
        """
        return self._host_ipv6

    def to_dict(self):
        return {"host_ipv4": self._host_ipv4, "host_ipv6": self._host_ipv6}

    @staticmethod
    def from_dict(dict_obj):
        host_ipv4 = jsonselect.select('.host_ipv4', dict_obj)
        host_ipv6 = jsonselect.select('.host_ipv6', dict_obj)
        if not host_ipv4 and not host_ipv6:
            raise ValueError('host_ipv4 and host_ipv4 both none or empty.')

        return NameNode(host_ipv4=host_ipv4,
                        host_ipv6=host_ipv6)


class NameRecord(object):
    def __init__(self, name=None, ttl=-1, nodes=None):
        """

        :param name:
        :param nodes:
        :return:
        """

        self._name = name
        self._ttl = ttl if ttl else -1
        self._nodes = nodes if nodes else []

    @property
    def name(self):
        return self._name

    @property
    def ttl(self):
        return self._ttl

    @property
    def nodes(self):
        return self._nodes

    def to_dict(self):
        return {"name": self._name, "ttl": self._ttl,
                "nodes": self._nodes | collect(lambda node: node.to_dict()) | as_list}


class Backend(object):
    """

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, backend_options=None):
        """

        :param backend_options:
        :return:
        """

        backend_url = urlparse.urlparse(backend_options)
        self._url = backend_url
        self._path = backend_url.path
        self._patterns = urlparse.parse_qs(backend_url.query).get('pattern', [])

    def supports(self, name):
        """

        :param name:
        :return:
        """

        if not name:
            return False

        return self._patterns | any(lambda pattern: name.endswith(pattern))

    @abc.abstractmethod
    def register(self, name, nodes, ttl=None):
        """

        :param name:
        :param nodes:
        :param ttl:
        :return:
        """
        pass

    @abc.abstractmethod
    def unregister(self, name):
        """

        :param name: domain name.
        :return:
        """
        pass

    @abc.abstractmethod
    def lookup(self, name):
        """

        :param name: domain name.
        :return: a releative NameRecord.
        """
        pass

    @abc.abstractmethod
    def lookall(self, name=None):
        """

        :param name:
        :return:
        """
        pass


class EtcdBackend(Backend):
    """

    """

    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """

        super(EtcdBackend, self).__init__(*args, **kwargs)

        host_pairs = [(addr | split(r':')) for addr in (self._url.netloc | split(','))]
        host_tuple = [(hostpair[0], int(hostpair[1])) for hostpair in host_pairs] | as_tuple

        self._client = etcd.Client(host=host_tuple, allow_reconnect=True)
        self._logger = loggers.get_logger('d.b.EtcdBackend')

    def _etcdkey(self, name=None):
        """

        :param name: domain format string, like api.dnswall.io
        :return: a etcd key format string, /io/dnswall/api
        """

        if not name:
            return [self._path] | join('/') | replace(r'/+', '/')
        else:
            keys = [self._path] + (name | split(r'\.') | reverse | as_list)
            return keys | join('/') | replace(r'/+', '/')

    def _rawname(self, key):
        """

        :param key: etcd key, like /io/dnswall/api
        :return: domain format string, like api.dnswall.io
        """

        raw_key = key if key.endswith('/') else key + '/'
        raw_names = raw_key | split(r'/') | reverse | as_list
        return raw_names[1:-1] | join('.') | replace('\.+', '.')

    def register(self, name, nodes, ttl=None):
        try:

            nodelist = nodes | collect(lambda node: node.to_dict()) | as_list
            self._client.set(self._etcdkey(name), json.dumps(nodelist), ttl)
        except:
            self._logger.ex('register name=%s, nodes=%s occurs error.', name, nodes)
            raise BackendError

    def unregister(self, name):
        try:

            self._client.delete(self._etcdkey(name))
        except etcd.EtcdKeyError:
            self._logger.w('unregister name=%s occurs etcd key error, just ignore it.', name)
        except:
            self._logger.ex('unregister name=%s occurs error.', name)
            raise BackendError

    def lookup(self, name):

        if not self.supports(name):
            raise BackendError("name=%s unsupport.".format(name))

        try:

            result = self._client.get(self._etcdkey(name))
            if not result.value:
                return NameRecord(name=name)

            return self._as_record(name, result.ttl, json.loads(result.value))
        except etcd.EtcdKeyError:
            self._logger.w('lookup name=%s occurs etcd key error, just ignore it.', name)
            return NameRecord(name=name)
        except:
            self._logger.ex('lookup name=%s occurs error.', name)
            raise BackendError

    def lookall(self, name=None):
        try:

            result = self._client.read(self._etcdkey(name), recursive=True)
            return self._as_records(result)
        except etcd.EtcdKeyError:
            self._logger.w('lookall occurs etcd key error, just ignore it.')
            return []
        except:
            self._logger.ex('lookall occurs error.')
            raise BackendError

    def _as_record(self, name, ttl, nodelist):
        return NameRecord(name=name,
                          ttl=ttl,
                          nodes=nodelist | collect(lambda node: NameNode.from_dict(node)) | as_list)

    def _as_records(self, result):

        records = []
        self._append_records(result, records)

        for child in result.children:
            self._append_records(child, records)
        return records

    def _append_records(self, result, records):

        if result.value:
            nodelist = json.loads(result.value)
            records.append(self._as_record(self._rawname(result.key), result.ttl, nodelist))
