# PostgreSQL Message Protocol (PGMP)

An implementation of the [PostgreSQL Message
Protocol](https://www.postgresql.org/docs/current/protocol.html) for
[Erlang/OTP 25](https://www.erlang.org).

Features:

- [Simple Query](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4)
- [Extended Query](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
- [Logical Streaming Replication](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- Binary protocol (preferred) and textual when required.
- Binary protocol for parse, bind and execute during an [Extended
  Query](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY).
- Asynchronous requests using
  [send_request](https://www.erlang.org/doc/man/gen_statem.html#send_request-4).
  Available for the client to use, and used internally within the
  implementation.
- A [connection](src/pgmp_connection.erl) pool that is aware of the underlying
  statement state (a simple or extended query, outside or within a
  transaction block).

## Simple Query

An asynchronous simple query request using [`receive_response`](https://www.erlang.org/doc/man/gen_statem.html#receive_response-1) to wait for the response:

```erlang
1> gen_statem:receive_response(pgmp_connection:query(#{sql => <<"select 2*3">>})).
{reply, [{row_description, [<<"?column?">>]},
         {data_row, [6]},
         {command_complete, {select, 1}}]}
```

## Extended Query

An extended query uses `parse`, `bind` and `execute` functions. A
statement created by `parse` can be reused, and rebound with different
parameter values and executed repeatedly. Parameters are correctly escaped
including quoted strings where appropriate. You can page through
results with a maximum number of rows being returned, rather than
receiving all rows in one go.

To parse SQL into a prepared statement:

```erlang
1> gen_statem:receive_response(pgmp_connection:parse(#{sql => <<"select $1 * 3">>})).
{reply, [{parse_complete, []}]}
```

Where `$1` is a parameter that will be bound during the `bind` phase.

To `bind` parameters to the prepared statement:

```erlang
1> gen_statem:receive_response(pgmp_connection:bind(#{args => [2]})).
{reply, [{bind_complete, []}]}
```

Finally, execute the prepared statement:

```erlang
1> gen_statem:receive_response(pgmp_connection:execute(#{})).
{reply, [{row_description, [<<"?column?">>]},
         {data_row, [6]},
         {command_complete, {select, 1}}]}
```

Execute by default will return all rows. To page the results instead:

```sql
postgres=# create table xy (x integer primary key, y text);
postgres=# insert into xy values (1, 'abc'), (2, 'foo'), (3, 'bar'), (4, 'baz'), (5, 'bat');
```

Prepare a statement to return all values:

```erlang
1> gen_statem:receive_response(pgmp_connection:parse(#{sql => <<"select * from xy">>})).
{reply, [{parse_complete, []}]}
```

Bind the statement with no parameters:

```erlang
1> gen_statem:receive_response(pgmp_connection:bind(#{args => []})).
{reply, [{bind_complete, []}]}
```

Finally, execute the prepared statement, returning a maximum of only 2 rows:

```erlang
1> gen_statem:receive_response(pgmp_connection:execute(#{max_rows => 2})).
{reply, [{row_description, [<<"x">>, <<"y">>]},
         {data_row, [1, <<"abc">>]},
         {data_row, [2, <<"foo">>]},
         {portal_suspended, []}]}
```

Note that `portal_suspended` is being returned (rather than
`command_complete` previously), which indicates that more rows are
available. Repeat the `execute` to get the next page of results:

```erlang
1> gen_statem:receive_response(pgmp_connection:execute(#{max_rows => 2})).
{reply, [{row_description, [<<"x">>, <<"y">>]},
         {data_row, [3, <<"bar">>]},
         {data_row, [4, <<"baz">>]},
         {portal_suspended, []}]}
```

The final execute will return the last row, and `command_complete`:

```erlang
1> gen_statem:receive_response(pgmp_connection:execute(#{max_rows => 2})).
{reply, [{row_description, [<<"x">>, <<"y">>]},
         {data_row, [5, <<"bat">>]},
         {command_complete, {select, 1}}]}
```

Note that as above, only the unnamed statement and portal should be used
with a connection pool to ensure continuity.


## Logical Replication

Tables with single column (primary) or composite (multiple columns)
keys are supported. When replication is started, a snapshot of each
table in the publication is made from a transaction identifier. On
completion, streaming replication causes each change to be immediately
applied onto a local ETS table replica.

Concurrent publications are supported each using their own replication
slot within PostgreSQL.

To enable replication your `postgresql.conf` must contain:

```
wal_level = logical
```

Please follow the [Quick
Setup](https://www.postgresql.org/docs/current/logical-replication-quick-setup.html)
for logical replication. Only the `publication` needs to be created,
`pgmp` will create the necessary slots and synchronise the published
data, applying any changes thereafter automatically.

### Primary Key

```sql
postgres=# create table xy (x integer primary key, y text);
postgres=# insert into xy values (1, 'foo');
```

Create a PostgreSQL publication for that table:

```sql
postgres=# create publication xy for table xy;
```

Configure `pgmp` to replicate the `xy` publication, via the stanza:

```erlang
 {pgmp, [...
         {replication_logical_publication_names, <<"xy">>},
         ...]}
```

The current state of the table is replicated into an ETS table also called `xy`:

```erlang
1> ets:i(xy).
<1   > {1, <<"foo">>}
EOT  (q)uit (p)Digits (k)ill
```

Introspection on the PostgreSQL metadata is done automatically by
`pgmp` so that `x` is used as the primary key for the replicated ETS
table.

Thereafter, CRUD changes on the underlying PostgreSQL table will be
automatically pushed to `pgmp` and reflected in the ETS table.

### Composite Key

An example of logical replication of a single table with a composite key:

```sql
postgres=# create table xyz (x integer, y integer, z integer, primary key (x, y));
postgres=# insert into xyz values (1, 1, 3);
```

Create a PostgreSQL publication for that table:

```sql
postgres=# create publication xyz for table xyz;
```

With `pgmp` configured for replication, the stanza:

```erlang
 {pgmp, [...
         {replication_logical_publication_names, <<"xy,xyz">>},
         ...]}
```

Where `replication_logical_publication_names` is a comma separated
list of publication names for `pgmp` to replicate. The contents of the
PostgreSQL table is replicated into an ETS table of the same name.

```erlang
1> ets:i(xyz).
<1   > {{1, 1}, 3}
EOT  (q)uit (p)Digits (k)ill
```

Note that replication introspects the PostgreSQL table metadata so that `{1, 1}` (x, y) is automatically used as the composite key.

Making some CRUD within PostgreSQL:

```sql
postgres=# insert into xyz values (1, 2, 3), (1, 3, 5), (1, 4, 5);
postgres=# update xyz set z = 4 where x = 1 and y = 1;
postgres=# delete from xyz where x = 1 and y = 3;
```

The changes are streamed to `pgmp` and applied to the ETS table automatically.

Replication slots can be viewed via:

```sql
postgres=# select slot_name,plugin,slot_type,active,xmin,catalog_xmin,restart_lsn from pg_replication_slots;
 slot_name |  plugin  | slot_type | active | xmin | catalog_xmin | restart_lsn 
-----------+----------+-----------+--------+------+--------------+-------------
 pgmp_xyz  | pgoutput | logical   | t      |      |    533287257 | A1/CEAA6AA8
 pgmp_xy   | pgoutput | logical   | t      |      |    533287257 | A1/CEAA6AA8
(2 rows)
```

## JSON/XML

Any encoder or decoder for JSON/XML can be used, providing they
implement `decode/1` and `encode/1`.

To use [jsx](https://github.com/talentdeficit/jsx), apply the following configuration:

```erlang
 {pgmp, [...
         {codec_json, jsx},
         ...]}
```



## Internals

The implementation uses some recently introduced features of Erlang/OTP:

- [socket](https://www.erlang.org/doc/man/socket.html) for all
  communication
- [`send_request`](https://www.erlang.org/doc/man/gen_statem.html#send_request-4)
  for asynchronous request and response.
- [`change_callback_module`](https://www.erlang.org/doc/man/gen_statem.html#type-action)
  to switch between startup, various authentication implementations,
  simple and extended query modes.
