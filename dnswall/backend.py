import abc
import json
import urlparse

import etcd
import jsonselect

from dnswall import loggers
from dnswall.commons import *
from dnswall.errors import *

__all__ = ["NameItem", "NameDetail", "Backend", "EtcdBackend"]


class NameItem(object):
    """

    """

    def __init__(self, uuid=None, host_ipv4=None, host_ipv6=None):
        self._uuid = uuid
        self._host_ipv4 = host_ipv4
        self._host_ipv6 = host_ipv6

    def __eq__(self, other):
        if self is other:
            return True

        if not isinstance(other, NameItem):
            return False

        return (self._host_ipv4, self._host_ipv6,) == \
               (other._host_ipv4, other._host_ipv6,)

    def __ne__(self, other):
        if self is other:
            return False

        if not isinstance(other, NameItem):
            return True

        return (self._host_ipv4, self._host_ipv6,) != \
               (other._host_ipv4, other._host_ipv6,)

    def __hash__(self):
        return hash((self._host_ipv4, self._host_ipv6,))

    @property
    def uuid(self):
        return self._uuid

    @property
    def host_ipv4(self):
        return self._host_ipv4

    @property
    def host_ipv6(self):
        return self._host_ipv6

    def to_dict(self):
        return {'uuid': self._uuid,
                'host_ipv4': self._host_ipv4,
                'host_ipv6': self._host_ipv6}

    @staticmethod
    def from_dict(dict_obj):
        uuid = jsonselect.select('.uuid', dict_obj)
        host_ipv4 = jsonselect.select('.host_ipv4', dict_obj)
        host_ipv6 = jsonselect.select('.host_ipv6', dict_obj)
        return NameItem(uuid=uuid,
                        host_ipv4=host_ipv4,
                        host_ipv6=host_ipv6)


class NameDetail(object):
    def __init__(self, name, items=None):
        self._name = name
        self._items = (items | as_set | as_list) if items else []

    @property
    def name(self):
        return self._name

    @property
    def items(self):
        return self._items

    def to_dict(self):
        return {"name": self._name,
                "items": self._items | collect(lambda it: it.to_dict()) | as_list}

    @staticmethod
    def from_dict(dict_obj):
        name = jsonselect.select('.name', dict_obj)
        items = jsonselect.select('.items', dict_obj)
        return NameDetail(name,
                          items=items | collect(lambda it: NameItem.from_dict(it)) | as_list)


class Backend(object):
    """

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, backend_options, patterns=None):
        """

        :param backend_options:
        :return:
        """

        backend_url = urlparse.urlparse(backend_options)
        self._url = backend_url
        self._path = backend_url.path
        self._patterns = patterns if patterns else []

    def supports(self, name):
        """

        :param name:
        :return:
        """

        if not name:
            return False

        if not self._patterns:
            return True

        return self._patterns | any(lambda pattern: name.endswith(pattern))

    @abc.abstractmethod
    def register(self, name, item, ttl=None):
        """

        :param name:
        :param item:
        :param ttl:
        :return:
        """
        pass

    @abc.abstractmethod
    def unregister(self, name, item):
        """

        :param name:
        :param item:
        :return:
        """
        pass

    @abc.abstractmethod
    def lookup(self, name):
        """

        :param name: domain name.
        :return: a releative NameDetail.
        """
        pass

    @abc.abstractmethod
    def lookall(self):
        """

        :return:
        """
        pass


class EtcdBackend(Backend):
    """

    """

    NODES_KEY = '@items'

    def __init__(self, *args, **kwargs):
        super(EtcdBackend, self).__init__(*args, **kwargs)

        host_pairs = [(it | split(r':')) for it in (self._url.netloc | split(','))]
        host_tuple = [(it[0], it[1] | as_int) for it in host_pairs] | as_tuple

        self._client = etcd.Client(host=host_tuple, allow_reconnect=True)
        self._logger = loggers.getlogger('d.b.EtcdBackend')

    def _etcdkey(self, name, uuid=None):

        if not uuid:
            uuid = ''

        nameparts = name | split(r'\.') | reverse | as_list
        keyparts = [self._path] + nameparts + [EtcdBackend.NODES_KEY, uuid]
        return keyparts | join('/') | replace(r'/+', '/')

    def _rawkey(self, etcd_key):

        keyparts = etcd_key | split(r'/') | reverse | as_list
        if self._path and not self._path == '/':
            keyparts = keyparts[:-1]

        keypattern = '[^.]*\.*{}\.*'.format(EtcdBackend.NODES_KEY)
        return keyparts[1:-1] \
               | join('.') \
               | replace('\.+', '.') \
               | replace(keypattern, '')

    def _etcdvalue(self, raw_value):
        return json.dumps(raw_value.to_dict(), sort_keys=True)

    def _rawvalue(self, etcd_value):
        return NameItem.from_dict(json.loads(etcd_value))

    def register(self, name, item, ttl=None):

        self._check_name(name)
        self._check_item(item)

        etcd_key = self._etcdkey(name, uuid=item.uuid)
        try:
            etcd_value = self._etcdvalue(item)
            self._client.set(etcd_key, etcd_value, ttl=ttl)
        except:
            self._logger.ex('register occur error.')
            raise BackendError

    def _check_name(self, name):
        if not self.supports(name):
            raise BackendValueError('name {} unsupported.'.format(name))

    def _check_item(self, item):
        if not item or not item.uuid:
            raise BackendValueError('item or item.uuid must not be none or empty.')

    def unregister(self, name, item):

        self._check_name(name)
        self._check_item(item)

        etcd_key = self._etcdkey(name, uuid=item.uuid)
        try:

            self._client.delete(etcd_key)
        except etcd.EtcdKeyError:
            self._logger.d('unregister key %s not found, just ignore it', etcd_key)
        except:
            self._logger.ex('unregister occur error.')
            raise BackendError

    def lookup(self, name):

        self._check_name(name)
        etcd_key = self._etcdkey(name)
        try:

            etcd_result = self._client.read(etcd_key, recursive=True)
            result_items = etcd_result.leaves \
                           | select(lambda it: it.value) \
                           | collect(lambda it: (self._rawkey(it.key), it.value)) \
                           | select(lambda it: it[0] == name) \
                           | collect(lambda it: self._rawvalue(it[1])) \
                           | as_list

            return NameDetail(name, items=result_items)
        except etcd.EtcdKeyError:
            self._logger.d('key %s not found, just ignore it.', etcd_key)
            return NameDetail(name)
        except:
            self._logger.ex('lookup key %s occurs error.', etcd_key)
            raise BackendError

    def lookall(self):

        etcd_key = self._path
        try:

            etcd_result = self._client.read(etcd_key, recursive=True)
            return self._to_namedetails(etcd_result)
        except etcd.EtcdKeyError:
            self._logger.d('key %s not found, just ignore it.', etcd_key)
            return []
        except:
            self._logger.ex('lookall key %s occurs error.', etcd_key)
            raise BackendError

    def _to_namedetails(self, result):

        results = {}
        self._collect_namedetails(result, results)

        for child in result.leaves:
            self._collect_namedetails(child, results)

        return results.items() \
               | collect(lambda it: NameDetail(it[0], items=it[1])) \
               | as_list

    def _collect_namedetails(self, result, results):

        if not result.value:
            return

        name = self._rawkey(result.key)
        item = self._rawvalue(result.value)

        if not self.supports(name):
            return

        if results.get(name):
            results[name].append(item)
        else:
            results[name] = [item]
