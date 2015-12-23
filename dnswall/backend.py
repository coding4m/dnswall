import abc
import json
import urlparse

import etcd

from dnswall import loggers
from dnswall.commons import *
from dnswall.errors import *

__all__ = ["NameSpec", "NameRecord", "Backend", "EtcdBackend"]


class NameSpec(object):
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
        host_ipv4 = dict_obj.get('host_ipv4', '')
        host_ipv6 = dict_obj.get('host_ipv6', '')
        if not host_ipv4 and not host_ipv6:
            raise ValueError('host_ipv4 and host_ipv4 both none or empty.')

        return NameSpec(host_ipv4=host_ipv4,
                        host_ipv6=host_ipv6)


class NameRecord(object):
    def __init__(self, name=None, ttl=-1, specs=None):
        """

        :param name:
        :param specs:
        :return:
        """

        self._name = name
        self._ttl = ttl if ttl else -1
        self._specs = specs if specs else []

    @property
    def name(self):
        return self._name

    @property
    def ttl(self):
        return self._ttl

    @property
    def specs(self):
        return self._specs

    def to_dict(self):
        return {"name": self._name, "ttl": self._ttl,
                "specs": self._specs | collect(lambda spec: spec.to_dict()) | as_list}


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
        backend_patterns = urlparse.parse_qs(backend_url.query).get('pattern', [])
        self._url = backend_url
        self._patterns = backend_patterns

    def supports(self, name):
        """

        :param name:
        :return:
        """

        if not name:
            return False

        return self._patterns | any(lambda pattern: name.endswith(pattern))

    @abc.abstractmethod
    def register(self, name, namespecs, ttl=None):
        """

        :param name:
        :param namespecs:
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
    def lookall(self):
        """

        :return: all NameRecords.
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
            return '/'

        keys = [self._url.path] + (name | split(r'\.') | reverse | as_list)
        return keys | join('/') | replace(r'/+', '/')

    def _rawname(self, key):
        """

        :param key: etcd key, like /io/dnswall/api
        :return: domain format string, like api.dnswall.io
        """

        raw_key = key if key.endswith('/') else key + '/'
        raw_names = raw_key | split(r'/') | reverse | as_list
        return raw_names[1:-1] | join('.') | replace('\.+', '.')

    def register(self, name, namespecs, ttl=None):
        try:

            speclist = namespecs | collect(lambda spec: spec.to_dict()) | as_list
            self._client.set(self._etcdkey(name), json.dumps(speclist), ttl)
        except:
            self._logger.ex('register name=%s, specs=%s occurs error.', name, namespecs)
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

    def lookall(self):
        try:

            result = self._client.read(self._etcdkey(), recursive=True)
            return self._as_records(result)
        except etcd.EtcdKeyError:
            self._logger.w('lookall occurs etcd key error, just ignore it.')
            return []
        except:
            self._logger.ex('lookall occurs error.')
            raise BackendError

    def _as_record(self, name, ttl, speclist):
        return NameRecord(name=name,
                          ttl=ttl,
                          specs=speclist | collect(lambda spec: NameSpec.from_dict(spec)) | as_list)

    def _as_records(self, result):

        records = []
        self._append_records(result, records)

        for child in result.children:
            self._append_records(child, records)
        return records

    def _append_records(self, result, records):

        if result.value:
            speclist = json.loads(result.value)
            records.append(self._as_record(self._rawname(result.key), result.ttl, speclist))
