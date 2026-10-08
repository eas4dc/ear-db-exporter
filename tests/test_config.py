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
    assert config.expand_hostlist(text) == expected


def test_expand_rejects_unsupported():
    with pytest.raises(ValueError):
        config.expand_hostlist("r[1-2]n[1-2]")


def test_load_fixture():
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
    scalars, _ = config.parse("# comment\nDBCommandsPassw=ab#cd\n")
    assert scalars == {"DBCOMMANDSPASSW": "ab#cd"}


def load_text(tmp_path, text, **kw):
    p = tmp_path / "ear.conf"
    p.write_text(text)
    return config.load(str(p), **kw)[0]


def test_keys_are_read_like_ear(tmp_path):
    # case-insensitive keys, MariaDB* alias, last occurrence wins, value ends at the
    # next '=' and loses its spaces
    db = load_text(tmp_path, "dbip=old\nMariaDBIp=db1\nDBCommandsUser= reader \n"
                             "DBCommandsPassw=p a ss=tail\nDBDatabase=EAR60\n")
    assert (db.host, db.user, db.password, db.database, db.port) == \
        ("db1", "reader", "pass", "EAR60", 3306)


def test_missing_keys_take_ear_defaults(tmp_path):
    db = load_text(tmp_path, "\n")
    assert (db.host, db.port, db.database, db.user, db.password) == \
        ("127.0.0.1", 3306, "EAR", "ear_commands", "")


def test_daemon_account_on_request(tmp_path):
    text = "DBUser=writer\nDBPassw=w\nDBCommandsUser=reader\nDBCommandsPassw=r\n"
    assert load_text(tmp_path, text).user == "reader"
    db = load_text(tmp_path, text, account="daemon")
    assert (db.user, db.password) == ("writer", "w")
    with pytest.raises(config.ConfigError, match="account"):
        load_text(tmp_path, text, account="root")


def test_bad_port(tmp_path):
    with pytest.raises(config.ConfigError, match="DBPort"):
        load_text(tmp_path, "DBPort=x\n")
