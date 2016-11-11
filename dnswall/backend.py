import abc
import json
import re
import urlparse

import etcd
import jsonselect

from dnswall import loggers
from dnswall.commons import *
from dnswall.errors import *

__all__ = ["DomainItem", "DomainDetail", "Backend", "EtcdBackend"]

DOMAIN_REGEX = re.compile(r'^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}$')
DOMAIN_WILDCARD_REGEX = re.compile(r'^\*\.([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}$')


def _is_valid_domain(name):
    if not name:
        return False
    return [DOMAIN_REGEX, DOMAIN_WILDCARD_REGEX] | any(lambda it: it.match(name))


class DomainItem(object):
    """

    """

    def __init__(self, uuid=None, host_ipv4=None, host_ipv6=None):
        self._uuid = uuid
        self._host_ipv4 = host_ipv4
        self._host_ipv6 = host_ipv6

    def __eq__(self, other):
        if self is other:
            return True

        if not isinstance(other, DomainItem):
            return False

        return (self._host_ipv4, self._host_ipv6,) == \
               (other._host_ipv4, other._host_ipv6,)

    def __ne__(self, other):
        return not self.__eq__(other)

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
        return DomainItem(uuid=uuid,
                          host_ipv4=host_ipv4,
                          host_ipv6=host_ipv6)


class DomainDetail(object):
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
        return DomainDetail(name,
                            items=items | collect(lambda it: DomainItem.from_dict(it)) | as_list)


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
        if not backend_url.path or backend_url.path == '/':
            self._path = '/dnswall'
        else:
            self._path = backend_url.path

        self._patterns = patterns if patterns else []

    def supports(self, name):
        """

        :param name:
        :return:
        """

        if not _is_valid_domain(name):
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
        :return: a releative DomainDetail.
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

    ITEMS_KEY = '@items'
    WILDCARD_SYMBOL = "*"
    WILDCARD_NAME = "__wildcard__"

    def __init__(self, *args, **kwargs):
        super(EtcdBackend, self).__init__(*args, **kwargs)

        host_pairs = [(it | split(r':')) for it in (self._url.netloc | split(','))]
        host_tuple = [(it[0], it[1] | as_int) for it in host_pairs] | as_tuple

        self._client = etcd.Client(host=host_tuple, allow_reconnect=True)
        self._logger = loggers.getlogger('d.b.EtcdBackend')

    def _etcdkey(self, name, uuid=None, with_items_key=True):

        if not uuid:
            uuid = ''

        items_key = EtcdBackend.ITEMS_KEY
        if not with_items_key:
            items_key = ''

        # startswith *.xxx.io, replace to xxx.io

        if EtcdBackend.WILDCARD_SYMBOL in name:
            name = name.replace(EtcdBackend.WILDCARD_SYMBOL, EtcdBackend.WILDCARD_NAME)

        nameparts = name | split(r'\.') | reverse | as_list
        keyparts = [self._path] + nameparts + [items_key, uuid]
        return keyparts | join('/') | replace(r'/+', '/')

    def _rawkey(self, etcd_key):

        if EtcdBackend.WILDCARD_NAME in etcd_key:
            etcd_key = etcd_key.replace(EtcdBackend.WILDCARD_NAME, EtcdBackend.WILDCARD_SYMBOL)

        keyparts = etcd_key | split(r'/') | reverse | as_list
        if self._path and not self._path == '/':
            keyparts = keyparts[:-1]

        keypattern = '[^.]*\.*{}\.*'.format(EtcdBackend.ITEMS_KEY)

        return keyparts[1:-1] \
               | join('.') \
               | replace('\.+', '.') \
               | replace(keypattern, '')

    def _etcdvalue(self, raw_value):
        return json.dumps(raw_value.to_dict(), sort_keys=True)

    def _rawvalue(self, etcd_value):
        return DomainItem.from_dict(json.loads(etcd_value))

    def _can_wildcard_lookback(self, name):
        if EtcdBackend.WILDCARD_SYMBOL in name:
            return False
        name_list = name | split(r'\.') | as_list
        return len(name_list) > 2

    def _get_wildcard_lookback(self, name):
        name_list = name | split(r'\.') | as_list
        return ([EtcdBackend.WILDCARD_SYMBOL] + name_list[1:]) | join('.')

    def register(self, name, item, ttl=None):

        name_list = name | split(r'[,|;]') | as_list
        name_list = name_list | collect(lambda it: self._check_name(it))
        name_item = self._check_item(item)

        etcd_keys = name_list | collect(lambda it: self._etcdkey(it, uuid=name_item.uuid))
        try:
            etcd_value = self._etcdvalue(name_item)
            for etcd_key in etcd_keys:
                self._client.set(etcd_key, etcd_value, ttl=ttl)
        except:
            self._logger.ex('register occur error.')
            raise BackendError

    def _check_name(self, name):
        if not self.supports(name):
            raise BackendValueError('name {} unsupported.'.format(name))
        return name

    def _check_item(self, item):
        if not item or not item.uuid:
            raise BackendValueError('item or item.uuid must not be none or empty.')
        return item

    def unregister(self, name, item):

        name_list = name | split(r'[,|;]') | as_list
        name_list = name_list | collect(lambda it: self._check_name(it))
        name_item = self._check_item(item)

        etcd_keys = name_list | collect(lambda it: self._etcdkey(it, uuid=name_item.uuid))
        for etcd_key in etcd_keys:
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
            etcd_items = etcd_result.leaves \
                         | select(lambda it: it.value) \
                         | collect(lambda it: (self._rawkey(it.key), it.value)) \
                         | select(lambda it: it[0] == name) \
                         | collect(lambda it: self._rawvalue(it[1])) \
                         | as_list

            return DomainDetail(name, items=etcd_items)
        except etcd.EtcdKeyError:
            if not self._can_wildcard_lookback(name):
                return DomainDetail(name)

            return self.lookup(self._get_wildcard_lookback(name))
        except:
            self._logger.ex('lookup key %s occurs error.', etcd_key)
            raise BackendError

    def lookall(self, name=None):

        etcd_key = self._etcdkey(name, with_items_key=False) if name else self._path
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
               | collect(lambda it: DomainDetail(it[0], items=it[1])) \
               | as_list

    def _collect_namedetails(self, result, results):

        if not result.value:
            return

        name = self._rawkey(result.key)
        item = self._rawvalue(result.value)

        if not self.supports(name):
            return

        if name in results:
            results[name].append(item)
        else:
            results[name] = [item]
