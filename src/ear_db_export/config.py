"""Read EAR's ``ear.conf``: database credentials and the node → tag map.

The database keys are read the way EAR reads them (``cluster_conf_db.c``), so
this package connects where ``eacct`` connects: keys are case-insensitive,
``MariaDB*`` is an alias of ``DB*``, the last occurrence wins, a value ends at
the next ``=`` and loses all its spaces, and a missing key takes EAR's default.

:func:`load` returns EAR's read-only ``DBCommandsUser`` account, the one
``eacct`` uses; this package never reads ``DBUser``, the account EAR writes with.

The ``Island=`` lines give each node's ``tag``, which the datasets carry as
``node_tag``; their ``Nodes=`` field is a Slurm-style hostlist
(``cpu12[55-56,59-60],gpu01``), expanded by :func:`expand_hostlist`.
"""
import logging
import os
import re
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

#: The ear.conf database keys this package reads, upper-cased as EAR compares
#: them: ``{key: (name in EAR's template, EAR's default)}``.
DB_KEYS = {
    "DBIP": ("DBIp", "127.0.0.1"),
    "DBPORT": ("DBPort", "3306"),
    "DBDATABASE": ("DBDatabase", "EAR"),
    "DBCOMMANDSUSER": ("DBCommandsUser", "ear_commands"),
    "DBCOMMANDSPASSW": ("DBCommandsPassw", ""),
}


class ConfigError(Exception):
    """Bad configuration, invalid profile or schema mismatch.

    The CLI turns it into a one-line error and exit code ``2``.
    """


@dataclass(frozen=True)
class DbConf:
    """Connection settings for one EAR database.

    :ivar host: database host (``DBIp``).
    :ivar port: database port (``DBPort``).
    :ivar database: database name (``DBDatabase``).
    :ivar user: account to log in with (``DBCommandsUser`` when read from ear.conf).
    :ivar password: its password. Left out of ``repr()`` so it never reaches a log.
    """

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)  # never logged


def default_conf_path():
    """Return the path where EAR looks for ``ear.conf``.

    :returns: ``$EAR_ETC/ear/ear.conf`` if ``EAR_ETC`` is set, else ``/etc/ear/ear.conf``.
    """
    etc = os.environ.get("EAR_ETC")
    return os.path.join(etc, "ear", "ear.conf") if etc else "/etc/ear/ear.conf"


def parse(text):
    """Split the text of an ear.conf into settings and ``Island=`` lines.

    As in EAR, a line starting with ``#`` is a comment (a ``#`` inside a value
    may be part of a password), keys are upper-cased, and when a key appears
    twice the last value wins. A ``MariaDB`` key prefix is EAR's old name for
    ``DB`` and is returned as ``DB`` (``MariaDBIp`` → ``DBIP``).

    :param text: the full contents of an ear.conf file.
    :returns: ``(scalars, islands)``. ``scalars`` is a dict ``{KEY: raw value}``
        of the ``key=value`` lines (everything after the first ``=``).
        ``islands`` has one dict per ``Island=`` line with all its ``k=v``
        tokens, keys upper-cased, for example
        ``{"ISLAND": "0", "NODES": "a[1-2]", "TAG": "x"}``.
    """
    scalars, islands = {}, []
    for line in text.splitlines():
        if line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.upper()
        if key.startswith("MARIADB"):
            key = "DB" + key[len("MARIADB"):]
        if key == "ISLAND":
            # "Island=0 Nodes=a[1-2] tag=x": every token is a k=v pair.
            islands.append({k.upper(): v for k, v in
                            (tok.split("=", 1) for tok in line.split() if "=" in tok)})
        else:
            scalars[key] = value  # last occurrence wins, like EAR
    return scalars, islands


def _db_value(raw):
    """Return a database setting as EAR reads it: up to the next ``=``, without spaces.

    EAR takes the value with ``strtok(NULL, "=")`` and removes every space, so
    ``DBCommandsPassw= a b`` is the password ``ab``.
    """
    return next((t for t in raw.split("=") if t), "").replace(" ", "")


def node_tags(islands):
    """Map every node listed in the Island lines to its tag.

    :param islands: the Island dicts returned by :func:`parse`.
    :returns: ``{node name: tag}`` for the Island lines that set both ``Nodes=``
        and ``tag=``. Nodes of Island lines without a tag are left out.
    :raises ValueError: if a ``Nodes=`` hostlist cannot be expanded
        (see :func:`expand_hostlist`).
    """
    tags = {}
    for isl in islands:
        if "TAG" in isl and "NODES" in isl:
            for node in expand_hostlist(isl["NODES"]):
                tags[node] = isl["TAG"]
    return tags


def load(path):
    """Read the read-only DB credentials (``DBCommandsUser``) and the node tags from an ear.conf file.

    A database key missing from the file takes EAR's default (:data:`DB_KEYS`),
    as EAR does; each default used is logged.

    :param path: path to ear.conf.
    :returns: ``(DbConf, node_tags)``; ``node_tags`` is ``{node: tag}``.
    :raises ConfigError: if the file cannot be read, or ``DBPort`` is not a number.
    """
    try:
        with open(path) as f:
            scalars, islands = parse(f.read())
    except OSError as e:
        raise ConfigError("cannot read %s: %s" % (path, e))
    values = {}
    for key, (name, default) in DB_KEYS.items():
        if key in scalars:
            values[key] = _db_value(scalars[key])
        else:
            values[key] = default
            log.info("%s has no %s: using EAR's default %r", path, name, default)
    try:
        port = int(values["DBPORT"])
    except ValueError:
        raise ConfigError("%s: DBPort=%r is not a number" % (path, values["DBPORT"]))
    db = DbConf(values["DBIP"], port, values["DBDATABASE"], values["DBCOMMANDSUSER"],
                values["DBCOMMANDSPASSW"])
    return db, node_tags(islands)


# == Hostlists (the Nodes= field of Island lines) =================================

# prefix [ranges ] suffix, with exactly one bracket group
_RANGED = re.compile(r"([^\[\]]*)\[([0-9,\-]+)\]([^\[\]]*)")


def _split_top_level(hostlist):
    """Split a hostlist on the commas that are not inside brackets.

    :param hostlist: for example ``a[1-2,5],b``.
    :returns: the non-empty tokens, stripped: ``["a[1-2,5]", "b"]``.
    """
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(hostlist):
        depth += {"[": 1, "]": -1}.get(ch, 0)  # bracket nesting level
        if ch == "," and depth == 0:
            parts.append(hostlist[start:i])
            start = i + 1
    parts.append(hostlist[start:])
    return [p.strip() for p in parts if p.strip()]


def expand_hostlist(hostlist):
    """Return the node names of a Slurm-style hostlist, in the order they are written.

    Each token is a plain name or has one bracket group with ranges and single
    numbers, optionally with a prefix and a suffix. Zero padding is kept:
    ``gcn[001-002]`` gives ``gcn001`` and ``gcn002``.

    :param hostlist: comma-separated names and ranges, for example ``a[1-3],b05``.
    :returns: list of node names.
    :raises ValueError: on a token with more than one bracket group, or on a
        descending range such as ``[5-3]``.
    """
    nodes = []
    for token in _split_top_level(hostlist):
        if "[" not in token and "]" not in token:  # plain node name
            nodes.append(token)
            continue
        m = _RANGED.fullmatch(token)
        if not m:  # e.g. two bracket groups: never seen in ear.conf, so refuse
            raise ValueError("unsupported hostlist token %r" % token)
        prefix, ranges, suffix = m.groups()
        for part in ranges.split(","):
            lo, _, hi = part.partition("-")
            hi = hi or lo  # a single number is a range of one
            width = len(lo) if lo.startswith("0") else 0  # keep zero padding ("05")
            if int(hi) < int(lo):
                raise ValueError("descending range %r in %r" % (part, token))
            nodes += ["%s%0*d%s" % (prefix, width, i, suffix) for i in range(int(lo), int(hi) + 1)]
    return nodes
