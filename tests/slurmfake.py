"""A fake slurmdbd database on SQLite: one Slurm release's tables (tests/fixtures/slurm), with a few records.

:class:`SlurmFakeEngine` is :class:`fakedb.FakeEngine` (the mariadb functions,
running the real generated SQL) over the DDL of a Slurm release, for one
cluster, ``lenox.hu`` (a dot, as on the test cluster). The records, around
September 2026, cover the cases the export must keep: a job still pending and
one cancelled before it started (``time_start = 0``), a deleted row, an array
job, a requeued GPU job with no time limit, batch and normal steps, events,
reservations and the dimension tables.

SQLite stores signed 64-bit integers only, so no record sets ``mem_req``'s top
bit (``MEM_PER_CPU``) or ``NO_VAL64``: test_slurm_clean covers those.
"""
import datetime as dt
import itertools
import os
import sqlite3

from fakedb import DBCONF, FakeEngine

CLUSTER = "lenox.hu"
FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "slurm")
#: 2026-09-01 00:00 UTC.
T0 = int(dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc).timestamp())
H, D = 3600, 86400
_DATABASES = itertools.count()


def ddl(release):
    """Parse ``slurm-<release>.sql`` into ``{table: [(column, type, nullable, in primary key)]}``.

    Table names keep their ``{cluster}`` placeholder. The type is cut before
    ``not null``, ``default`` and ``auto_increment``, as ``COLUMN_TYPE`` gives it.
    """
    tables, table, pk = {}, None, ()
    with open(os.path.join(FIXTURES, "slurm-%s.sql" % release)) as f:
        for line in f:
            line = line.strip().rstrip(",")
            if line.startswith("CREATE TABLE"):
                table = line.split("`")[1]
                tables[table] = []
            elif line.startswith("PRIMARY KEY"):
                pk = {c.strip() for c in line[line.index("(") + 1:line.rindex(")")].split(",")}
                tables[table] = [(c, t, n, c in pk) for c, t, n, _ in tables[table]]
            elif table and line and not line.startswith((")", "--")):
                col, rest = line.split(None, 1)
                low = rest.lower()
                cut = min([i for i in (low.find(" not null"), low.find(" default"),
                                       low.find(" auto_increment")) if i >= 0] or [len(rest)])
                tables[table].append((col, low[:cut], "not null" not in low, False))
    return tables


class SlurmFakeEngine(FakeEngine):
    """An engine "module" over an in-memory SQLite database with one Slurm release's tables.

    :param release: ``"17.11"``, ``"20.02"`` or ``"25.05"``.
    :param version: the ``convert_version_table.version`` to store, or ``None``
        for no row (detection by columns).
    :param drop: ``{table: [columns]}`` to leave out of the DDL (logical table, e.g. ``"job"``).
    """

    def __init__(self, release="25.05", version=16, drop=None):
        """Create the release's tables for :data:`CLUSTER` and fill them with :func:`records`."""
        self.__name__ = "fake"
        self.profile, self.step, self.queries, self.streaming = None, 1, [], set()
        drop = {"{cluster}_%s_table" % t: set(c) for t, c in (drop or {}).items()}
        self.ddl = {name.format(cluster=CLUSTER): [c for c in cols if c[0] not in drop.get(name, ())]
                    for name, cols in ddl(release).items()}
        self.uri = "file:slurm%d?mode=memory&cache=shared" % next(_DATABASES)
        self.db = sqlite3.connect(self.uri, uri=True)
        for table, cols in self.ddl.items():
            self.db.execute("CREATE TABLE `%s` (%s)" % (table, ", ".join("`%s`" % c[0] for c in cols)))
        for table, rows in records(version).items():
            self.add(table, rows)

    def add(self, table, rows):
        """Insert rows (dicts with today's column names) into ``<cluster>_<table>`` or a global table.

        Columns the release lacks are left out; ``het_job_*`` become
        ``pack_job_*`` where the release has those instead (before 20.02).
        """
        name = "%s_%s" % (CLUSTER, table) if "%s_%s" % (CLUSTER, table) in self.ddl else table
        if name not in self.ddl:
            return
        have = [c[0] for c in self.ddl[name]]
        for r in rows:
            r = {("pack" + k[3:] if k.startswith("het_job") and k not in have else k): v
                 for k, v in r.items()}
            r = {k: v for k, v in r.items() if k in have}
            self.db.execute("INSERT INTO `%s` (%s) VALUES (%s)" % (
                name, ", ".join("`%s`" % k for k in r), ", ".join("?" * len(r))), list(r.values()))
        self.db.commit()


def job(inx, submit, **kw):
    """A job_table row: job ``1000 + inx`` of alice, submitted at ``submit``, plus ``kw``."""
    row = dict(job_db_inx=inx, mod_time=submit, deleted=0, account="physics", cpus_req=4,
               exit_code=0, flags=0, job_name="job%d" % inx, id_assoc=2, id_array_job=0,
               id_array_task=0xfffffffe, id_job=1000 + inx, id_qos=1, id_resv=0, id_wckey=0,
               id_user=1000, id_group=100, het_job_id=0, het_job_offset=0xfffffffe,
               kill_requid=None, state_reason_prev=0, constraints="", mem_req=8000,
               nodelist="None assigned", nodes_alloc=0, node_inx="", partition="cpu",
               priority=100, script_hash_inx=0, state=0, timelimit=120, time_submit=submit,
               time_eligible=submit, time_start=0, time_end=0, time_suspended=0,
               submit_line="sbatch job.sh", tres_req="1=4,2=8000,4=1", tres_alloc="")
    row.update(kw)
    return row


def ran(start, end, **kw):
    """The columns of a job that ran from ``start`` to ``end`` on n1."""
    return dict(time_start=start, time_end=end, nodelist="n1", nodes_alloc=1, node_inx="0",
                tres_alloc="1=4,2=8000,4=1,5=4", **kw)


def step(inx, id_step, start, end, **kw):
    """A step_table row of job ``inx``."""
    row = dict(job_db_inx=inx, deleted=0, exit_code=0, id_step=id_step, step_het_comp=0xfffffffe,
               nodelist="n1", nodes_alloc=1, state=3, step_name="s", task_cnt=1, task_dist=0,
               time_start=start, time_end=end, time_suspended=0, user_sec=10, user_usec=5,
               sys_sec=1, sys_usec=2, tres_alloc="1=4,2=8000,4=1",
               tres_usage_in_ave="2=100", tres_usage_in_max="2=200", tres_usage_in_tot="2=300",
               tres_usage_out_ave="", tres_usage_out_max="", tres_usage_out_tot="", max_rss=200)
    row.update(kw)
    return row


def records(version=16):
    """The records, ``{table without the cluster prefix: [row dicts]}``.

    Jobs by ``job_db_inx``: 1 before September; 2 ran in September (script 1);
    3 is an array task still pending; 4 was cancelled before it started; 5 is
    deleted; 6 is a requeued GPU job in October without a time limit; 7 is after
    2026-10-13 (outside the test window).
    """
    t = T0
    return {
        "convert_version_table": [] if version is None else [dict(mod_time=t, version=version)],
        "cluster_table": [dict(creation_time=t, deleted=0, name=CLUSTER, federation="", fed_state=0),
                          dict(creation_time=t, deleted=1, name="gone", federation="", fed_state=0)],
        "tres_table": [dict(creation_time=t, deleted=0, id=i, type=ty, name=n) for i, ty, n in
                       ((1, "cpu", ""), (2, "mem", ""), (3, "energy", ""), (4, "node", ""),
                        (5, "billing", ""), (1001, "gres", "gpu"))],
        "qos_table": [dict(creation_time=t, deleted=0, id=1, name="normal")],
        "assoc_table": [
            dict(creation_time=t, deleted=0, id_assoc=1, user="", acct="root", parent_acct="",
                 id_parent=0, lineage="/", is_def=0),
            dict(creation_time=t, deleted=0, id_assoc=2, user="alice", acct="physics",
                 parent_acct="", id_parent=1, lineage="/physics/0-alice/", is_def=1)],
        "job_table": [
            job(1, t - H, **ran(t - H + 60, t + H, state=3)),
            job(2, t + 10 * H, time_eligible=t + 10 * H + 60, script_hash_inx=1,
                **ran(t + 10 * H + 600, t + 10 * H + 4200, state=3)),
            job(3, t + D + 5, id_array_job=1003, id_array_task=0),
            job(4, t + 2 * D, state=4, time_end=t + 2 * D + 30),
            job(5, t + 14 * D, deleted=1, **ran(t + 14 * D + 1, t + 14 * D + 2, state=5)),
            job(6, t + 31 * D, id_user=1001, timelimit=0xffffffff, state=1024,
                **dict(ran(t + 31 * D + 10, t + 31 * D + 100),
                       tres_alloc="1=8,2=64000,4=1,1001=2")),
            job(7, t + 43 * D)],
        "step_table": [step(1, 0, t, t + 1), step(2, -5, t + 10 * H + 600, t + 10 * H + 4200),
                       step(2, 0, t + 10 * H + 700, t + 10 * H + 4100),
                       step(6, 0, t + 31 * D + 10, t + 31 * D + 100)],
        "event_table": [dict(time_start=t - 2 * D, time_end=t - D, node_name="n1", reason="old",
                             reason_uid=0, state=1, cluster_nodes="", tres="1=4", deleted=0),
                        dict(time_start=t + 4 * D, time_end=0, node_name="n2", reason="bad dimm",
                             reason_uid=1000, state=1, cluster_nodes="", tres="1=4", deleted=0)],
        "resv_table": [dict(id_resv=1, deleted=0, assoclist="2", flags=0, nodelist="n1",
                            node_inx="0", resv_name="maint", time_start=t + 9 * D,
                            time_end=t + 9 * D + H, tres="1=4", unused_wall=0.0)],
        "assoc_usage_hour_table": [dict(creation_time=t, mod_time=t, deleted=0, id=2, id_tres=1,
                                        time_start=t + 10 * H, alloc_secs=14400)],
        "job_script_table": [dict(deleted=0, hash_inx=1, last_used=None,  # SQLite returns TIMESTAMP as text
                                  script_hash="abc", batch_script="#!/bin/sh\nsrun hostname\n")],
    }


__all__ = ["CLUSTER", "DBCONF", "SlurmFakeEngine", "T0", "ddl"]
