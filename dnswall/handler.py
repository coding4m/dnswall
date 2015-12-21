import json

from twisted.python.compat import nativeString
from twisted.web import resource, error, _responses
from twisted.web.error import UnsupportedMethod

from dnswall.backend import NameSpec
from dnswall.commons import *

__all__ = ['VersionResource', 'NameResource']


class WebResource(resource.Resource):
    """

    """

    def render(self, request):
        """

        :param request:
        :return:
        """

        m = getattr(self, 'render_' + nativeString(request.method.lower()), None)
        if not m:
            try:
                allowed_methods = self.allowedMethods
            except AttributeError:
                allowed_methods = resource._computeAllowedMethods(self)
            raise UnsupportedMethod(allowed_methods)
        return m(request)

    def render_head(self, request):
        return self.render_get(request)


class VersionResource(WebResource):
    def __init__(self, name=None, version=None):
        WebResource.__init__(self)
        self._name = name
        self._version = version

    def render_get(self, request):
        request.responseHeaders.addRawHeader(b"content-type", b"application/json")
        return json.dumps({'name': self._name, "version": self._version})


class NameResource(WebResource):
    """

    """

    def __init__(self, backend=None):
        WebResource.__init__(self)
        self._backend = backend

    def render_get(self, request):
        """

        :param request:
        :return:
        """

        self._set_response_headers(request)
        qname = request.args.get('name', []) | first
        if not qname:
            recordlist = self._backend.lookall()
            return json.dumps(recordlist | collect(lambda record: record.to_dict()) | as_list)

        namerecord = self._backend.lookup(qname)
        if not namerecord:
            return json.dumps({})
        else:
            return json.dumps(namerecord.to_dict())

    def render_post(self, request):
        """

        :param request:
        :return:
        """

        self._set_response_headers(request)

        qname = request.args.get('name', []) | first
        if not qname:
            raise error.Error(_responses.BAD_REQUEST, message='missing name query param.')

        specdicts = json.dumps(request.content.getvalue())
        if not isinstance(specdicts, list):
            raise error.Error(_responses.BAD_REQUEST, message='body must be a json array.')

        self._backend.register(qname,
                               specdicts | collect(lambda specdict: NameSpec.from_dict(specdict)) | as_list)

    def render_delete(self, request):
        """

        :param request:
        :return:
        """

        self._set_response_headers(request)

        qname = request.args.get('name', []) | first
        if not qname:
            raise error.Error(_responses.BAD_REQUEST, message='missing name query param.')

        self._backend.unregister(qname)

    def _set_response_headers(self, request):
        request.responseHeaders.addRawHeader(b"content-type", b"application/json")
