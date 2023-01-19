# Logical Replication

Tables with single (primary) or multiple columns (composite)
keys are supported. When replication is started, a snapshot of each
table in the publication is made from a transaction identifier. On
completion, streaming replication causes each change to be immediately
applied onto a local ETS table replica.

Concurrent publications are supported each using their own replication
slot within [PostgreSQL][postgresql-org].

To enable replication your `postgresql.conf` must contain:

```bash
wal_level = logical
```

Please follow the [Quick Setup][postgresql-org-log-rep-quick-setup]
for logical replication. Only the `publication` needs to be created,
`pgmp` will create the necessary slots and synchronise the published
data, applying any changes thereafter automatically.

Or with a local `postgres` via `docker`:

```shell
docker run \
    --rm \
    --name postgres \
    -d \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    postgres:14 \
    -c shared_buffers=256MB \
    -c max_connections=200 \
    -c wal_level=logical
```

You can then run a SQL shell with:

```shell
docker exec --interactive --tty postgres psql postgres postgres

psql (14.4 (Debian 14.4-1.pgdg110+1))
Type "help" for help.

postgres=# 
```

The [dev.config](/dev.config) is used to configure `pgmp`, with
various connection parameters (database hostname, user and password).

To start `pgmp` run:

```shell
make shell
```

## Replication Process Overview

A replication slot is created by
[pgmp_mm_rep_log](src/pgmp_mm_rep_log.erl), from which a snapshot is
created by the database. Using this snapshot,
[pgmp_rep_log_ets](src/pgmp_rep_log_ets.erl) retrieves all data from
the tables in the publication, in a single transaction (using extended
query with batched execute). Once the initial data has been collected,
streaming replication is then started, receiving changes that have
applied since the transaction snapshot to ensure no loss of data.

## Primary Key

```sql
create table xy (x integer primary key, y text);
insert into xy values (1, 'foo');
```

Create a [PostgreSQL][postgresql-org] publication for that table:

```sql
create publication xy for table xy;
```

Configure `pgmp` to replicate the `xy` publication, via the stanza:

```erlang
 {pgmp, [...
         {replication_logical_publication_names, <<"xy">>},
         ...]}
```

You should restart `pgmp` if you change any of the publication names in
[dev.config](/dev.config), or if you create the publication in SQL
after `pgmp` has started.

The current state of the table is replicated into an ETS table also called `xy`:

```erlang
1> ets:i(xy).
<1   > {1, <<"foo">>}
EOT  (q)uit (p)Digits (k)ill
```

Introspection on the [PostgreSQL][postgresql-org] metadata is done
automatically by `pgmp` so that `x` is used as the primary key for the
replicated ETS table.

Thereafter, CRUD changes on the underlying
[PostgreSQL][postgresql-org] table will be automatically pushed to
`pgmp` and reflected in the ETS table.

## Composite Key

An example of logical replication of a single table with a composite key:

```sql
create table xyz (x integer, y integer, z integer, primary key (x, y));
insert into xyz values (1, 1, 3);
```

Create a [PostgreSQL][postgresql-org] publication for that table:

```sql
create publication xyz for table xyz;
```

With `pgmp` configured for replication, the stanza:

```erlang
 {pgmp, [...
         {replication_logical_publication_names, <<"xy,xyz">>},
         ...]}
```

Where `replication_logical_publication_names` is a comma separated
list of publication names for `pgmp` to replicate. The contents of the
[PostgreSQL][postgresql-org] table is replicated into an ETS table of
the same name.

```erlang
1> ets:i(xyz).
<1   > {{1, 1}, 3}
EOT  (q)uit (p)Digits (k)ill
```

Note that replication introspects the [PostgreSQL][postgresql-org]
table metadata so that `{1, 1}` (x, y) is automatically used as the
composite key.

Making some CRUD within [PostgreSQL][postgresql-org]:

```sql
insert into xyz values (1, 2, 3), (1, 3, 5), (1, 4, 5);
update xyz set z = 4 where x = 1 and y = 1;
delete from xyz where x = 1 and y = 3;
```

Changes are applied to the ETS table in real-time.

Replication slots can be viewed via:

```sql
select slot_name,plugin,slot_type,active,xmin,catalog_xmin,restart_lsn from pg_replication_slots;

 slot_name |  plugin  | slot_type | active | xmin | catalog_xmin | restart_lsn 
-----------+----------+-----------+--------+------+--------------+-------------
 pgmp_xyz  | pgoutput | logical   | t      |      |    533287257 | A1/CEAA6AA8
 pgmp_xy   | pgoutput | logical   | t      |      |    533287257 | A1/CEAA6AA8
(2 rows)
```

## PostgreSQL version 15 Logical Replication Features

The logical replication features in the [PostgreSQL 15
release][postgresql-org-15-release], are: [row
filters][postgresql-org-rep-row-filter] and [column
lists][postgresql-org-log-rep-col-lists]. Their usage with
[pgmp][pgmp] are described in this
[article][shortishly-com-pgmp-logical-replication-in-postgresql-fifteen].

[pgmp]: https://github.com/shortishly/pgmp
[postgresql-org-15-release]: https://www.postgresql.org/about/news/postgresql-15-released-2526/
[postgresql-org-log-rep-col-lists]: https://www.postgresql.org/docs/15/logical-replication-col-lists.html
[postgresql-org-log-rep-quick-setup]: https://www.postgresql.org/docs/current/logical-replication-quick-setup.html
[postgresql-org-rep-row-filter]: https://www.postgresql.org/docs/15/logical-replication-row-filter.html
[postgresql-org]: https://www.postgresql.org/
[shortishly-com-pgmp-logical-replication-in-postgresql-fifteen]: https://shortishly.com/blog/pgmp-log-rep-postgresql-fifteen/
