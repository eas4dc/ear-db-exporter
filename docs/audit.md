# What the tool does to your database

This page is for the administrators who run the tool on their site and need
to check that it only reads, does not overload the database, and that they
know what leaves the site. Everything here can be checked with the tool
itself (`check`, `--dry-run`) or by reading three short files.

## It only reads

- **Account.** It logs in with EAR's read-only account, `DBCommandsUser` /
  `DBCommandsPassw` from `ear.conf` (the account `eacct` uses), or with the
  account you give in `--db-user` / `--db-password-file`. The tool never
  reads `DBUser`, EAR's account that can write.
- **Read-only session.** Right after connecting it runs `SET SESSION
  TRANSACTION READ ONLY`, so the server refuses any write in that session even
  if the account could write. It also sets the session time zone to UTC and
  `net_write_timeout` to 10 minutes (so a long streamed read is not cut while
  the tool writes a file); both are session settings.
- **No writes of any kind**: no `INSERT`, `UPDATE`, `DELETE`, temporary
  tables, indexes, locks or stored procedures. The package cannot open a
  writable session at all (its tests check that no code can).

Check it on your database:

```console
$ ear-db-export check
database EAR, session read-only: yes
grants:
  GRANT USAGE ON *.* TO `ear_commands`@`%`
  GRANT SELECT ON `EAR`.* TO `ear_commands`@`%`
profile ear6 vs EAR:
  ...
```

`grants` is the server's own answer to `SHOW GRANTS` (any password hash is cut
off), and `session read-only` is what the server reports for the session.

## The queries it runs

Only single-table `SELECT`s. No joins: rows are combined in memory afterwards.
A run opens at most two read-only connections: one for the streamed reads
below (`Jobs`, `Loops`, tables without an integer key), sent to the tool in
batches of 100 000 rows, and one for everything else, one query at a time, so
the links of each chunk of job steps are looked up while the `Jobs` read is
still open. Lists of keys go in `IN (…)` batches of at most 5000.

| When | Query | Access |
|---|---|---|
| Every run | `information_schema.COLUMNS` (which tables and columns exist; compare with the profile); `SHOW GRANTS`, `@@transaction_read_only` (`check`) | — |
| Job steps (`raw` with filters, `job_nodes`, `power_signatures`, `eacct`) | `Jobs`: `SELECT … WHERE <filters>` (time, users, …), with `--limit` `ORDER BY end_time DESC LIMIT n` | **full scan** of `Jobs` (small), except `--jobs`: primary key |
| Their links, 5000 job steps at a time | `Applications` by `job_id IN (…)`; `Power_signatures`, `Signatures`, `GPU_signatures` by `id IN (…)`; `@@auto_increment_increment` | primary key |
| `raw` with `Loops` (filtered) | `Loops`: one `SELECT … [WHERE job_id IN (…)] [AND node_id IN (…)]` per run | **full scan**: `Loops` has no primary key and no index, and can be the largest table. One read stays open while it streams |
| `Periodic_metrics`, `Events`, `Periodic_aggregations` by time | `MIN(id), MAX(id)`, then two binary searches of one-row lookups by `id` (about log₂ of the table's rows each, ±1 day of margin) to find the window's ids, then `SELECT … WHERE id BETWEEN … AND <time> … [AND node/job IN (…)] ORDER BY id LIMIT 1000000`, page after page | primary key |
| `Global_energy` | `SELECT … WHERE time >= … AND time < …` | primary key (`time`) |
| A table without a filter (`export raw`, `Learning_*`) | `SELECT <columns> ORDER BY id LIMIT 1000000`, then `… WHERE id > <last> …` page after page; tables without a single integer key: one streamed `SELECT` | primary key |
| `export raw` | `SHOW CREATE TABLE` of each table; every export: `SELECT VERSION()` | — |

`Periodic_metrics`, `Events` and `Periodic_aggregations` have no time index in
EAR's schema (on some sites `Periodic_metrics` has one, on others not), so a
day is mapped to a range of ids by binary search on the primary key and read
with `id BETWEEN`: the cost is that of the day, not of the table. Because each
page is a short query, the server never holds a long read open on those
tables; `Loops` and `Jobs` are the exceptions above. A run over a month reads
the month once, page by page; the tool never re-reads a table per day.

`--dry-run` shows every query of a run with its `EXPLAIN` (and, when
filtered, its row count) and writes nothing. Queries that depend on the rows
of earlier ones (the links of the job steps) are only described, since a
dry-run reads no rows:

```console
$ ear-db-export export node_metrics --service x --from 2026-09-01 --to 2026-09-01 --dry-run --out /tmp/x
$ ear-db-export export raw --users alice --dry-run --out /tmp/x
```

To read the code that talks to the database: `src/ear_db_export/read/sql/mariadb.py`
is the only file that imports the database driver and runs queries;
`read/sql/__init__.py` and `read/sql/queries.py` build them.

## What leaves your site

Nothing, by itself. The tool writes only into the output directory (`--out`):
the files described in {doc}`data`, `OUT/checkpoint.json` (the end of the
last window exported) and an empty `OUT/.ear_export.lock`. It opens no
network connection other than to the database. Copying the files anywhere is
a separate step you control.

A bundle's `manifest.json` describes it: the layout, the tool version, the database name,
server version and EAR schema profile, the filters of the run (with users,
accounts and groups pseudonymised), the time windows the bundle holds
(`coverage`), the node tags of ear.conf, the files with
their row counts and checksums, and a fingerprint of the salt (an HMAC of a
fixed text, which shows whether two bundles share a salt but does not reveal
it).

## Personal data

| Field | In the files |
|---|---|
| User name, Slurm account, Unix group (`user_id`, `user_acc`, `user_group`; in a bundle also `user_name`, `pod_user_id`) | **Pseudonymised**: `HMAC-SHA256(salt, value)`, first 16 hex digits |
| Job / application name (`app_name`, `Jobs.app_id` before EAR 7) | **In clear**. Job names sometimes contain project or person names (or home directory paths): review them before sharing |
| Node names, energy tags, policies, all measurements | In clear |
| In a bundle: every other column of every table (events, loops, …) | In clear |

The salt is a secret file you create once and keep (`--salt-file`). The same
user gets the same pseudonym in every file made with the same salt, so the
files can be joined; without the salt the pseudonyms cannot be linked back to
users. The tool refuses to write user fields without `--salt-file` unless you
pass `--no-pseudonymize`, and refuses to add files to a bundle made with
another salt.
