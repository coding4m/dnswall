import abc
import json
import re
import urlparse
import etcd
from dnswall.commons import *
from dnswall.errors import *

__all__ = ["NameSpec", "NameRecord", "Backend", "EtcdBackend"]

_ANYKEY = ''


class NameSpec(object):
    """

    """

    def __init__(self, host_ipv4=None, host_ipv6=None, ttl=0):
        """

        :param host_ipv4:
        :param host_ipv6:
        :param ttl:
        :return:
        """
        self._host_ipv4 = host_ipv4
        self._host_ipv6 = host_ipv6
        self._ttl = ttl

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

    @property
    def ttl(self):
        """

        :return:
        """
        return self._ttl

    def to_dict(self):
        return {"host_ipv4": self._host_ipv4, "host_ipv6": self._host_ipv6, "ttl": self._ttl}

    @staticmethod
    def from_dict(dict_obj):
        host_ipv4 = dict_obj.get('host_ipv4', '')
        host_ipv6 = dict_obj.get('host_ipv6', '')
        if not host_ipv4 and not host_ipv6:
            raise ValueError('host_ipv4 and host_ipv4 both none or empty.')

        return NameSpec(host_ipv4=host_ipv4,
                        host_ipv6=host_ipv6,
                        ttl=dict_obj.get("ttl", 0))


class NameRecord(object):
    def __init__(self, name=None, specs=None):
        """

        :param name:
        :param specs:
        :return:
        """

        self._name = name
        self._specs = specs if specs else []

    @property
    def name(self):
        return self._name

    @property
    def specs(self):
        return self._specs

    def to_dict(self):
        return {"name": self._name, "specs": self._specs | collect(lambda spec: spec.to_dict()) | as_list}


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
        backend_name_patterns = urlparse.parse_qs(backend_url.query).get('pattern', [])
        self._url = backend_url
        self._patterns = backend_name_patterns

    def supports(self, name):
        """

        :param name:
        :return:
        """

        if not name:
            return False

        return self._patterns | any(lambda pattern: name.endswith(pattern))

    @abc.abstractmethod
    def register(self, name, namespecs):
        """

        :param name: domain name.
        :param namespecs: a NameSpec list.
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
        host_pairs = [re.split(r':', addr) for addr in re.split(r',', self._url.netloc)]
        host_tuple = [(hostpair[0], int(hostpair[1])) for hostpair in host_pairs] | as_tuple
        self._client = etcd.Client(host=host_tuple, allow_reconnect=True)

    def _etcdkey(self, name):
        """

        :param name: domain format string, like api.dnswall.io
        :return: a etcd key format string, /io/dnswall/api
        """

        keys = [self._url.path] + (name | split(pattern=r'\.') | reverse | as_list)
        return keys | join(separator='/') | replace(pattern=r'/+', replacement='/')

    def _rawname(self, key):
        """

        :param key: etcd key, like /io/dnswall/api
        :return: domain format string, like api.dnswall.io
        """

        raw_key = key if key.endswith('/') else key + '/'
        raw_names = raw_key | split(pattern=r'/') | reverse | as_list
        return raw_names[1:-1] | join(separator='.') | replace(pattern='\.+', replacement='.')

    def register(self, name, namespecs):

        if not isinstance(namespecs, (list, tuple)):
            raise ValueError('namespecs must be list or tuple.')

        if not self.supports(name):
            raise BackendError("name={} unsupport.".format(name))

        try:

            speclist = namespecs | collect(lambda spec: spec.to_dict()) | as_list
            self._client.set(self._etcdkey(name), json.dumps(speclist))
        except Exception as e:
            # TODO
            print(e)
            raise BackendError

    def unregister(self, name):

        if not name:
            raise ValueError('name must not be none or empty.')

        try:

            self._client.delete(self._etcdkey(name))
        except etcd.EtcdKeyError:
            pass
        except:
            # TODO
            raise BackendError

    def lookup(self, name):

        if not self.supports(name):
            raise BackendError("name={} unsupport.".format(name))

        try:

            result = self._client.get(self._etcdkey(name))
            if not result.value:
                return NameRecord(name=name)

            return self._as_record(name, json.loads(result.value))
        except etcd.EtcdKeyError:
            return NameRecord(name=name)
        except Exception as e:
            # TODO
            print(e)
            raise BackendError

    def _as_record(self, name, speclist):
        return NameRecord(name=name,
                          specs=speclist | collect(lambda spec: NameSpec.from_dict(spec)) | as_list)

    def lookall(self):
        try:

            result = self._client.read(self._etcdkey(_ANYKEY), recursive=True)
            return self._as_records(result)
        except etcd.EtcdKeyError:
            return []
        except Exception as e:
            print(e)
            pass

    def _as_records(self, result):

        records = []
        if result.value:
            speclist = json.loads(result.value)
            records.append(self._as_record(self._rawname(result.key), speclist))

        for child in result.children:
            self._as_children_records(child, records)
        return records

    def _as_children_records(self, child, records):

        if child.value:
            speclist = json.loads(child.value)
            records.append(self._as_record(self._rawname(child.key), speclist))
