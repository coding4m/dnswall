import abc
import json
import urlparse

import etcd
import jsonselect

from dnswall import loggers
from dnswall.commons import *
from dnswall.errors import *

__all__ = ["NameNode", "NameList", "Backend", "EtcdBackend"]


class NameNode(object):
    """

    """

    def __init__(self, uuid=None, host_ipv4=None, host_ipv6=None):
        self._uuid = uuid
        self._host_ipv4 = host_ipv4
        self._host_ipv6 = host_ipv6

    def __eq__(self, other):
        if self is other:
            return True

        if not isinstance(other, NameNode):
            return False

        return self._uuid == other._uuid

    def __ne__(self, other):
        if self is other:
            return False

        if not isinstance(other, NameNode):
            return True

        return self._uuid != other._uuid

    def __hash__(self):
        return hash(self._uuid)

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
        return NameNode(uuid=uuid,
                        host_ipv4=host_ipv4,
                        host_ipv6=host_ipv6)


class NameList(object):
    def __init__(self, name=None, nodes=None):
        self._name = name
        self._nodes = nodes if nodes else []

    @property
    def name(self):
        return self._name

    @property
    def nodes(self):
        return self._nodes

    def to_dict(self):
        return {"name": self._name,
                "nodes": self._nodes | collect(lambda node: node.to_dict()) | as_list}

    @staticmethod
    def from_dict(dict_obj):
        name = jsonselect.select('.name', dict_obj)
        nodes = jsonselect.select('.nodes', dict_obj)
        return NameList(name=name,
                        nodes=nodes | collect(lambda it: NameNode.from_dict(it)) | as_list)


class Backend(object):
    """

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, backend_options, patterns=None):
        """

        :param backend_options:
        :param patterns:
        :return:
        """

        backend_url = urlparse.urlparse(backend_options)
        self._url = backend_url
        self._path = backend_url.path

        if patterns:
            self._patterns = patterns
        else:
            self._patterns = urlparse.parse_qs(backend_url.query).get('pattern', [])

    def supports(self, name):
        """

        :param name:
        :return:
        """

        if not name:
            return False

        return self._patterns | any(lambda pattern: name.endswith(pattern))

    def register_all(self, name, nodes):
        """

        :param name:
        :param nodes:
        :return:
        """
        for node in nodes:
            self.register(name, node)

    @abc.abstractmethod
    def register(self, name, node):
        """

        :param name:
        :param node:
        :return:
        """
        pass

    def unregister_all(self, name, nodes):
        """

        :param name:
        :param nodes:
        :return:
        """
        for node in nodes:
            self.unregister(name, node)

    @abc.abstractmethod
    def unregister(self, name, node):
        """

        :param name:
        :param node:
        :return:
        """
        pass

    @abc.abstractmethod
    def lookup(self, name):
        """

        :param name: domain name.
        :return: a releative NameList.
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

    NODES_SUBPATH = '@nodes'

    def __init__(self, *args, **kwargs):
        super(EtcdBackend, self).__init__(*args, **kwargs)

        host_pairs = [(it | split(r':')) for it in (self._url.netloc | split(','))]
        host_tuple = [(it[0], it[1] | as_int) for it in host_pairs] | as_tuple

        self._client = etcd.Client(host=host_tuple, allow_reconnect=True)
        self._logger = loggers.get_logger('d.b.EtcdBackend')

    def _etcdkey(self, node_name, node_id=None):

        if not node_id:
            node_id = ''

        keys = [self._path] + \
               (node_name | split(r'\.') | reverse | as_list) + \
               [EtcdBackend.NODES_SUBPATH, node_id]
        return keys | join('/') | replace(r'/+', '/')

    def _rawkey(self, etcd_key):

        node_parts = etcd_key | split(r'/') | reverse | as_list
        if self._path and not self._path == '/':
            node_parts = node_parts[:-1]

        return node_parts[1:-1] \
               | join('.') \
               | replace('\.+', '.') \
               | replace('[^.]*\.*{}\.*'.format(EtcdBackend.NODES_SUBPATH), '')

    def _etcdvalue(self, raw_value):
        return json.dumps(raw_value.to_dict())

    def _rawvalue(self, etcd_value):
        return NameNode.from_dict(json.loads(etcd_value))

    def register(self, name, node):

        if not name:
            raise BackendValueError('name must not be none or empty.')

        if not node or not node.uuid:
            raise BackendValueError('node or node.uuid must not be none or empty.')

        etcd_key = self._etcdkey(name, node_id=node.uuid)
        try:
            etcd_value = self._etcdvalue(node)
            self._client.set(etcd_key, etcd_value)
        except:
            self._logger.ex('register occur error.')
            raise BackendError

    def unregister(self, name, node):

        if not name:
            raise BackendValueError('name must not be none or empty.')

        if not node or not node.uuid:
            return

        etcd_key = self._etcdkey(name, node_id=node.uuid)
        try:

            self._client.delete(etcd_key)
        except etcd.EtcdKeyError:
            self._logger.d('unregister key %s not found, just ignore it', etcd_key)
        except:
            self._logger.ex('unregister occur error.')
            raise BackendError

    def lookup(self, name):

        if not self.supports(name):
            raise BackendValueError("unsupport name %s.".format(name))

        etcd_key = self._etcdkey(name)
        try:

            etcd_result = self._client.read(etcd_key, recursive=True)
            result_nodes = etcd_result.leaves \
                           | select(lambda it: it.value) \
                           | collect(lambda it: (self._rawkey(it.key), it.value)) \
                           | select(lambda it: it[0] == name) \
                           | collect(lambda it: self._rawvalue(it[1])) \
                           | as_list

            return NameList(name=name, nodes=result_nodes)
        except etcd.EtcdKeyError:
            self._logger.d('key %s not found, just ignore it.', etcd_key)
            return NameList(name=name)
        except:
            self._logger.ex('lookup key %s occurs error.', etcd_key)
            raise BackendError

    def lookall(self, name=None):

        etcd_key = self._etcdkey(name) if name else self._path
        try:

            etcd_result = self._client.read(etcd_key, recursive=True)
            return self._as_namelists(etcd_result)
        except etcd.EtcdKeyError:
            self._logger.d('key %s not found, just ignore it.', etcd_key)
            return []
        except:
            self._logger.ex('lookall key %s occurs error.', etcd_key)
            raise BackendError

    def _as_namelists(self, result):

        results = {}
        self._append_namelist(result, results)

        for child in result.leaves:
            self._append_namelist(child, results)

        return results.items() \
               | collect(lambda it: NameList(name=it[0], nodes=it[1])) \
               | as_list

    def _append_namelist(self, result, results):

        if not result.value:
            return

        name = self._rawkey(result.key)
        node = self._rawvalue(result.value)
        if results.get(name):
            results[name].append(node)
        else:
            results[name] = [node]
