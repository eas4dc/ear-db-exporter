"""ear.conf: read like EAR reads it, and Slurm hostlists."""
import os

import pytest

from ear_db_export import config

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "ear.conf")


@pytest.mark.parametrize("text, expected", [
    ("n1", ["n1"]),
    ("n[1-3],n[10-11]", ["n1", "n2", "n3", "n10", "n11"]),
    ("n[9-10]", ["n9", "n10"]),
    ("cpu12[55-56,59-60]", ["cpu1255", "cpu1256", "cpu1259", "cpu1260"]),
    ("cpu13[05-06]", ["cpu1305", "cpu1306"]),       # zero padding kept
    ("gpu[001-002]", ["gpu001", "gpu002"]),
    ("node1,node2", ["node1", "node2"]),
    ("a[1-2]-ib", ["a1-ib", "a2-ib"]),
])
def test_expand(text, expected):
    """Slurm hostlists expand in order, keeping zero padding."""
    assert config.expand_hostlist(text) == expected


def test_expand_rejects_unsupported():
    """Two bracket groups or a descending range are refused."""
    with pytest.raises(ValueError):
        config.expand_hostlist("r[1-2]n[1-2]")


def test_load_fixture():
    """ear.conf gives the read-only account and the node tags."""
    db, tags = config.load(FIXTURE)
    assert (db.host, db.database, db.port, db.user) == ("db.example.org", "EAR", 3306, "ear_commands")
    assert db.password == "pa#ss"
    assert "pa#ss" not in repr(db)  # password hidden from logs
    assert tags["cpu001"] == tags["cpu014"] == tags["cpu104"] == "cpu_a"
    assert "cpu011" not in tags  # gap in cpu[001-010,013-014]
    assert tags["gpu04"] == "gpu_a" and tags["misc2"] == "cpu_a"
    assert "login1" not in tags  # Island without tag=
    assert len(tags) == 12 + 4 + 4 + 2


def test_hash_in_value_is_not_a_comment():
    """A # inside a value (a password) is kept, as EAR reads it."""
    scalars, _ = config.parse("# comment\nDBCommandsPassw=ab#cd\n")
    assert scalars == {"DBCOMMANDSPASSW": "ab#cd"}


def load_text(tmp_path, text, **kw):
    """Write ear.conf text to a file and load it."""
    p = tmp_path / "ear.conf"
    p.write_text(text)
    return config.load(str(p), **kw)[0]


def test_keys_are_read_like_ear(tmp_path):
    """Keys are case-insensitive, MariaDB* is DB*, the last one wins, spaces go."""
    # case-insensitive keys, MariaDB* alias, last occurrence wins, value ends at the
    # next '=' and loses its spaces
    db = load_text(tmp_path, "dbip=old\nMariaDBIp=db1\nDBCommandsUser= reader \n"
                             "DBCommandsPassw=p a ss=tail\nDBDatabase=EAR60\n")
    assert (db.host, db.user, db.password, db.database, db.port) == \
        ("db1", "reader", "pass", "EAR60", 3306)


def test_missing_keys_take_ear_defaults(tmp_path):
    """A missing database key takes EAR's default."""
    db = load_text(tmp_path, "\n")
    assert (db.host, db.port, db.database, db.user, db.password) == \
        ("127.0.0.1", 3306, "EAR", "ear_commands", "")


def test_never_the_daemon_account(tmp_path):
    """DBUser (EAR's account that writes) is never read."""
    text = "DBUser=writer\nDBPassw=w\nDBCommandsUser=reader\nDBCommandsPassw=r\n"
    assert (load_text(tmp_path, text).user, load_text(tmp_path, text).password) == ("reader", "r")


def test_bad_port(tmp_path):
    """A non-numeric DBPort is a configuration error."""
    with pytest.raises(config.ConfigError, match="DBPort"):
        load_text(tmp_path, "DBPort=x\n")
