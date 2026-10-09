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

Only single-table `SELECT`s, one at a time, on one connection. No joins: rows
are combined in memory afterwards. Large results are streamed in batches of
100 000 rows. Lists of keys go in `IN (…)` batches of at most 5000.

| When | Query | Access |
|---|---|---|
| Every run | `information_schema.COLUMNS` (which tables and columns exist; compare with the profile); `SHOW GRANTS`, `@@transaction_read_only` (`check`) | — |
| Job steps (filters, or datasets) | `Jobs`: `SELECT … WHERE <filters>` (time, users, …) | **full scan** of `Jobs` (small), except `--jobs`: primary key |
| Their links | `Applications` by `job_id IN (…)`; `Power_signatures`, `Signatures`, `GPU_signatures` by `id IN (…)`; `@@auto_increment_increment` | primary key |
| `--tables loops` | `Loops`: one `SELECT … [WHERE job_id IN (…)]` | **full scan**: `Loops` has no primary key and no index, and can be the largest table. One read stays open while it streams |
| `Periodic_metrics`, `Events`, `Periodic_aggregations` by time | `MIN(id), MAX(id)`, about 25 one-row lookups by `id` to find the window's ids, then `SELECT … WHERE id BETWEEN … AND <time> … [AND node/job IN (…)] ORDER BY id LIMIT 1000000`, page after page | primary key |
| `Global_energy` | `SELECT … WHERE time >= … AND time < …` | primary key (`time`) |
| A table without a filter (`export --tables all`, `Learning_*`) | `SELECT <columns> ORDER BY id LIMIT 1000000`, then `… WHERE id > <last> …` page after page; tables without a single integer key: one streamed `SELECT` | primary key |
| `export` | `SHOW CREATE TABLE` of each table, `SELECT VERSION()` | — |

`Periodic_metrics`, `Events` and `Periodic_aggregations` have no time index in
EAR's schema (on some sites `Periodic_metrics` has one, on others not), so a
day is mapped to a range of ids by binary search on the primary key and read
with `id BETWEEN`: the cost is that of the day, not of the table. Because each
page is a short query, the server never holds a long read open on those
tables; `Loops` and `Jobs` are the exceptions above. With `--every`, each
period repeats its queries (one `Jobs` scan, and one `Loops` scan if asked,
per day or month).

`--dry-run` shows every query of a run with its `EXPLAIN` (and, when
filtered, its row count) and writes nothing. Queries that depend on the rows
of earlier ones (the links of the job steps) are only described, since a
dry-run reads no rows:

```console
$ ear-db-export view node_metrics --every day --from 2026-09-01 --to 2026-09-01 --dry-run
$ ear-db-export export --users alice --dry-run
```

To read the code that talks to the database: `src/ear_db_export/read/sql/mariadb.py`
is the only file that imports the database driver and runs queries;
`read/sql/__init__.py` and `read/sql/queries.py` build them.

## What leaves your site

Nothing, by itself. The tool writes only into the output directory (`--out`):
the files described in {doc}`data`, `OUT/.ear_export_state.json` (the last
day `--incremental` exported) and an empty `OUT/.ear_export.lock`. It opens no
network connection other than to the database. Copying the files anywhere is
a separate step you control.

A bundle's `manifest.json` describes it: the tool version, the database name,
server version and EAR schema profile, the filters of the run (with users,
accounts and groups pseudonymised), the node tags of ear.conf, the files with
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
