# What the tool does to your database

This page is for the administrators who run the tool on their site and need
to check that it only reads, does not overload the database, and that they
know what leaves the site. Everything here can be checked with the tool
itself (`check-schema`, `--dry-run`) or by reading three short files.

## It only reads

- **Account.** It logs in with EAR's read-only account, `DBCommandsUser` /
  `DBCommandsPassw` from `ear.conf` (the account `eacct` uses), or with the
  account you give in `--db-user` / `--db-password-file`. The commands of this
  tool never read `DBUser`, EAR's account that can write.
- **Read-only session.** Right after connecting it runs `SET SESSION
  TRANSACTION READ ONLY`, so the server refuses any write in that session even
  if the account could write. It also sets the session time zone to UTC.
- **No writes of any kind**: no `INSERT`, `UPDATE`, `DELETE`, temporary
  tables, indexes, locks or stored procedures.

Check it on your database:

```console
$ ear-db-export check-schema
account ear_commands on db.example.org:3306/EAR, session read-only: yes
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
100 000 rows.

| When | Query | Index used |
|---|---|---|
| Every run | `information_schema.COLUMNS` (compare the schema with the profile); `SHOW GRANTS`, `@@transaction_read_only` (`check-schema`) | — |
| `periodic`, per day | `Periodic_metrics`: `MIN(id), MAX(id)`, then about 25 one-row lookups to find the day's ids, then `SELECT … WHERE id BETWEEN … AND end_time >= … AND end_time < …` | primary key |
| `periodic`, signature commands, per day or month | `Jobs`: `SELECT … WHERE end_time >= … AND end_time < …` | none: a full scan of `Jobs`, which is small |
| idem | `Applications`, `Power_signatures`, `Signatures`, `GPU_signatures`: `SELECT … WHERE id IN (…)` or `job_id IN (…)`, at most 5000 keys per query; `@@auto_increment_increment` | primary key |
| `dump`, per table | `SELECT <columns> ORDER BY <key> LIMIT 1000000`, then `… WHERE <key> > <last> …` page after page; tables without a single integer key: one `SELECT <columns>`. Also `SHOW CREATE TABLE`, `SELECT VERSION()` | primary key |

Because each `dump` page is a short query, the server never holds a long
read open. `--dry-run` shows every query of a run with its `EXPLAIN` (and,
for the export commands, its row count) and writes nothing:

```console
$ ear-db-export --dry-run periodic --from 2026-09-01 --to 2026-09-01
$ ear-db-export --dry-run dump --tables Periodic_metrics
```

To read the code that talks to the database: `src/ear_db_export/read/sql/mariadb.py`
is the only file that imports the database driver and runs queries;
`read/sql/queries.py` builds the export queries and `dump.py` the dump queries.

## What leaves your site

Nothing, by itself. The tool writes only into the output directory (`--out`):
the files described in {doc}`data`, `OUT/.ear_export_state.json` (the last
day `periodic` exported) and an empty `OUT/.ear_export.lock`. It opens no
network connection other than to the database. Copying the files anywhere is
a separate step you control.

## Personal data

| Field | In the files |
|---|---|
| User name, Slurm account, Unix group (`user_id`, `user_acc`, `user_group`; in a dump also `user_name`, `pod_user_id`) | **Pseudonymised**: `HMAC-SHA256(salt, value)`, first 16 hex digits |
| Job / application name (`app_name`, `Jobs.app_id` before EAR 7) | **In clear**. Job names sometimes contain project or person names: review them before sharing |
| Node names, energy tags, policies, all measurements | In clear |
| In a dump: every other column of every table (events, loops, …) | In clear |

The salt is a secret file you create once and keep (`--salt-file`). The same
user gets the same pseudonym in every file made with the same salt, so the
files can be joined; without the salt the pseudonyms cannot be linked back to
users. The export commands refuse to run without `--salt-file` unless you
pass `--no-pseudonymize`.
