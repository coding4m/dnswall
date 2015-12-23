"""

"""

__all__ = ["WallError", "BackendError", "BackendNotFound"]


class WallError(Exception):
    """

    """

    def __init__(self, errcode=None, errmsg=None):
        self._errcode = errcode
        self._errmsg = errmsg

    def __str__(self):
        pass

    def __repr__(self):
        pass


class BackendError(Exception):
    """

    """
    pass


class BackendNotFound(BackendError):
    """

    """
    pass
