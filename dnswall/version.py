"""
    Versions for Python packages.
"""

__all__ = ['Version', 'current_version']


class _Inf(object):
    """
    An object that is bigger than all other objects.
    """

    def __cmp__(self, other):
        """
        @param other: Another object.
        @type other: any

        @return: 0 if other is inf, 1 otherwise.
        @rtype: C{int}
        """
        if other is _inf:
            return 0
        return 1


_inf = _Inf()


class IncomparableVersions(TypeError):
    """
    Two versions could not be compared.
    """


class Version(object):
    """
    An object that represents a three-part version number.
    """

    def __init__(self, package, major, minor, micro, desc=None, prerelease=None):
        """
        @param package: Name of the package that this is a version of.
        @type package: C{str}
        @param major: The major version number.
        @type major: C{int}
        @param minor: The minor version number.
        @type minor: C{int}
        @param micro: The micro version number.
        @type micro: C{int}
        @param prerelease: The prerelease number.
        @type prerelease: C{int}
        """
        self.package = package
        self.major = major
        self.minor = minor
        self.micro = micro
        self.desc = desc
        self.prerelease = prerelease

    def short(self):
        """
        Return a string in canonical short version format,
        <major>.<minor>.<micro>
        """
        return '%d.%d.%d' % (self.major,
                             self.minor,
                             self.micro)

    def base(self):
        """
        Like L{short}, but without the +rSVNVer.
        """
        if self.prerelease is None:
            pre = ""
        else:
            pre = "pre%s" % (self.prerelease,)
        return '%s %d.%d.%d%s' % (self.package,
                                  self.major,
                                  self.minor,
                                  self.micro,
                                  pre)

    def __repr__(self):
        if self.prerelease is None:
            prerelease = ""
        else:
            prerelease = ", prerelease=%r" % (self.prerelease,)
        return '%s(%r, %d, %d, %d%s)' % (
            self.__class__.__name__,
            self.package,
            self.major,
            self.minor,
            self.micro,
            prerelease)

    def __str__(self):
        return '[%s, version %s]' % (
            self.package,
            self.short())

    def __cmp__(self, other):
        """
        Compare two versions, considering major versions, minor versions, micro
        versions, then prereleases.

        A version with a prerelease is always less than a version without a
        prerelease. If both versions have prereleases, they will be included in
        the comparison.

        @param other: Another version.
        @type other: L{Version}

        @return: NotImplemented when the other object is not a Version, or one
            of -1, 0, or 1.

        @raise IncomparableVersions: when the package names of the versions
            differ.
        """
        if not isinstance(other, self.__class__):
            return NotImplemented
        if self.package != other.package:
            raise IncomparableVersions("%r != %r"
                                       % (self.package, other.package))

        if self.prerelease is None:
            prerelease = _inf
        else:
            prerelease = self.prerelease

        if other.prerelease is None:
            otherpre = _inf
        else:
            otherpre = other.prerelease

        x = cmp((self.major,
                 self.minor,
                 self.micro,
                 prerelease),
                (other.major,
                 other.minor,
                 other.micro,
                 otherpre))
        return x


current_version = Version('dnswall', 1, 0, 0, desc='dns server for docker using key-value backends.')
