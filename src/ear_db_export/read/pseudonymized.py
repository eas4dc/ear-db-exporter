"""A source whose user names come out as pseudonyms, so no layout ever sees them in clear.

:class:`Pseudonymized` wraps any source. Filters still compare clear names
(``--users alice``): the wrapped source applies them, before the wrapper
replaces the names in the rows it yields.
"""
from ..clean import USER_COLUMNS, USER_FIELDS, pseudonym, pseudonymize


def fingerprint(key):
    """Identify a salt without revealing it: the pseudonym of a fixed text (a bundle's ``salt_fingerprint``)."""
    return pseudonym(key, "ear-db-export")


class Pseudonymized:
    """Any source, with user fields replaced by ``HMAC-SHA256(key, value)`` pseudonyms in every row it yields.

    Every member other than the four read methods is the wrapped source's
    (``profile``, ``tags``, ``info()``, ...), except :attr:`salt_fingerprint`.

    :param source: the source to wrap.
    :param key: the secret key (contents of ``--salt-file``), bytes.

    Example::

        src = Pseudonymized(SqlSource(dbconf), b"secret")
        jobs = concat(src.scan("jobs", [Cond("user_id", "in", ["alice"])]))
        jobs["user_id"]   # ["4360c67bc8102511", ...]: alice's rows, the name replaced
    """

    def __init__(self, source, key):
        """Wrap ``source``; its user fields will be pseudonymised with ``key``."""
        self.source, self.key = source, key
        #: Identifies the key without revealing it (:func:`fingerprint`).
        self.salt_fingerprint = fingerprint(key)

    def __getattr__(self, name):
        """Return any other member from the wrapped source."""
        return getattr(self.source, name)

    def scan(self, table, where=(), order=None, limit=None):
        """The wrapped source's ``scan``, with user fields pseudonymised."""
        for t in self.source.scan(table, where, order, limit):
            yield pseudonymize(t, self.key, USER_FIELDS)

    def window(self, table, start, end, where=()):
        """The wrapped source's ``window``, with user fields pseudonymised."""
        for t in self.source.window(table, start, end, where):
            yield pseudonymize(t, self.key, USER_FIELDS)

    def lookup(self, table, column, keys):
        """The wrapped source's ``lookup``, with user fields pseudonymised."""
        for t in self.source.lookup(table, column, keys):
            yield pseudonymize(t, self.key, USER_FIELDS)

    def dump(self, table):
        """The wrapped source's ``dump``, with every physical user column pseudonymised."""
        for t in self.source.dump(table):
            yield pseudonymize(t, self.key, USER_COLUMNS)
