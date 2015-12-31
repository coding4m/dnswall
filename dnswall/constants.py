"""

"""
import sys


class ConstantError(TypeError):
    pass


class _Constants(object):
    def __setattr__(self, name, value):
        if self.__dict__.has_key(name):
            raise ConstantError("Can't rebind constant (%s)" % name)

        self.__dict__[name] = value


_constants = _Constants()
_constants.ADDR_ENV = 'DNSWALL_ADDR'
_constants.BACKEND_ENV = 'DNSWALL_BACKEND'
_constants.SERVERS_ENV = 'DNSWALL_SERVERS'
_constants.PATTERNS_ENV = 'DNSWALL_PATTERNS'
_constants.DOCKER_URL_ENV = 'DNSWALL_DOCKER_URL'
_constants.DOCKER_TLSCA_ENV = 'DNSWALL_DOCKER_TLSCA'
_constants.DOCKER_TLSKEY_ENV = 'DNSWALL_DOCKER_TLSKEY'
_constants.DOCKER_TLSCERT_ENV = 'DNSWALL_DOCKER_TLSCERT'
_constants.DOCKER_TLSVERIFY_ENV = 'DNSWALL_DOCKER_TLSVERIFY'
sys.modules[__name__] = _constants
