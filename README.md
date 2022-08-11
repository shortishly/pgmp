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
  
## Asynchronous Requests

There are two mechanisms for making asynchronous requests to `pmgp`
which are described in the following.

### send_request/2 with receive_response/1

You can immediately wait for a response (via
[receive_response/1](https://www.erlang.org/doc/man/gen_statem.html#receive_response-1)). The
examples in the following sections use this method, purely because it
is simpler as a command line example.

```erlang
1> gen_statem:receive_response(pgmp_connection:query(#{sql => <<"select 2*3">>})).
{reply, [{row_description, [<<"?column?">>]},
         {data_row, [6]},
         {command_complete, {select, 1}}]}
```

Effectively the above turns an asynchronous call into an asynchronous
request that immediately blocks the current process until it receives
the reply.

The module `pgmp_connection_sync` wraps the above:

```erlang
1> pgmp_connection_sync:query(#{sql => <<"select 2*3">>})).
[{row_description, [<<"?column?">>]},
 {data_row, [6]},
 {command_complete, {select, 1}}]
```


### send_request/4 with check_response/3

However, it is likely that you can continue doing some other important
work, e.g., responding to other messages, while waiting for the
response from `pgmp`. In which case using the
[send_request/4](https://www.erlang.org/doc/man/gen_statem.html#send_request-4)
and
[check_response/3](https://www.erlang.org/doc/man/gen_statem.html#check_response-3)
pattern is preferred.

If you're using `pgmp` within another `gen_*` behaviour (`gen_server`,
`gen_statem`, etc), this is very likely to be the option to
choose. So using another `gen_statem` as an example:

The following `init/1` sets up some state with a [request id
collection](https://www.erlang.org/doc/man/gen_statem.html#type-request_id_collection)
to maintain our outstanding asynchronous requests.

```erlang
init([]) ->
    {ok, ready, #{requests => gen_statem:reqids_new()}}.
```

You can then use the `label` and `requests` parameter to `pgmp` to
identify the response to your asynchronous request as follows:
```sql
handle_event(internal, commit = Label, _, _) ->
    {keep_state_and_data,
      {next_event, internal, {query, #{label => Label, sql => <<"commit">>}}}};
     
     
handle_event(internal, {query, Arg}, _, #{requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := pgmp_connection:query(Arg#{requests => Requests})}};
```

A call to any of `pgmp_connection` functions: `query/1`, `parse/1`,
`bind/1`, `describe/1` or `execute/1` take a map of parameters. If
that map includes both a `label` and `requests` then the request is
made using
[send_request/4](https://www.erlang.org/doc/man/gen_statem.html#send_request-4). The
response will be received as an `info` message as follows:

```erlang
handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            # You have a response with a Label so that you stitch it
            # back to the original request...
            do_something_with_response(Reply, Label),
            {keep_state, Data#{requests := Updated}};

        ...deal with other cases...
    end.
```

The internals of `pgmp` use this pattern,
[pgmp_rep_log_ets](src/pgmp_rep_log_ets.erl) being an example that
uses `pgmp_connection` itself to manage logical replication into ETS
tables.


## Simple Query

An asynchronous simple query request using [receive_response](https://www.erlang.org/doc/man/gen_statem.html#receive_response-1) to wait for the response:

```erlang
1> pgmp_connection_sync:query(#{sql => <<"select 2*3">>}).
[{row_description, [<<"?column?">>]},
 {data_row, [6]},
 {command_complete, {select, 1}}]
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
1> pgmp_connection_sync:parse(#{sql => <<"select $1 * 3">>}).
[{parse_complete, []}]
```

Where `$1` is a parameter that will be bound during the `bind` phase.

To `bind` parameters to the prepared statement:

```erlang
1> pgmp_connection_sync:bind(#{args => [2]}).
[{bind_complete, []}]
```

Finally, execute the prepared statement:

```erlang
1> pgmp_connection_sync:execute(#{}).
[{row_description, [<<"?column?">>]},
 {data_row, [6]},
 {command_complete, {select, 1}}]
```

Execute by default will return all rows. To page the results instead:

```sql
create table xy (x integer primary key, y text);
insert into xy values (1, 'abc'), (2, 'foo'), (3, 'bar'), (4, 'baz'), (5, 'bat');
```

Prepare a statement to return all values:

```erlang
1> pgmp_connection_sync:parse(#{sql => <<"select * from xy">>}).
[{parse_complete, []}]
```

Bind the statement with no parameters:

```erlang
1> pgmp_connection_sync:bind(#{args => []}).
[{bind_complete, []}]
```

Finally, execute the prepared statement, returning a maximum of only 2 rows:

```erlang
1> pgmp_connection_sync:execute(#{max_rows => 2}).
[{row_description, [<<"x">>, <<"y">>]},
 {data_row, [1, <<"abc">>]},
 {data_row, [2, <<"foo">>]},
 {portal_suspended, []}]
```

Note that `portal_suspended` is being returned (rather than
`command_complete` previously), which indicates that more rows are
available. Repeat the `execute` to get the next page of results:

```erlang
1> pgmp_connection_sync:execute(#{max_rows => 2}).
[{row_description, [<<"x">>, <<"y">>]},
 {data_row, [3, <<"bar">>]},
 {data_row, [4, <<"baz">>]},
 {portal_suspended, []}]
```

The final execute will return the last row, and `command_complete`:

```erlang
1> pgmp_connection_sync:execute(#{max_rows => 2}).
[{row_description, [<<"x">>, <<"y">>]},
 {data_row, [5, <<"bat">>]},
 {command_complete, {select, 1}}]
```

`pgmp` has its own connection pool that is aware of extended
query. Extended query connections are reserved by the process that
initiated the `parse`. This is to ensure continuity of the unnamed
prepared statement or portal, otherwise another process could prepare
or bind (and overwrite) another statement or portal.

After an extended query, you can release the connection by:

```erlang
1> pgmp_connection_sync:sync(#{}).
ok
```

Alternatively, the initiating process can call `query` to returning
back to simple query mode, which will also release the connection in
the pool.

Without a call to `sync`, the completion of the transaction, or a
simple query, that connection will remain reserved for the initiating
process and not returned to the connection pool.

### Named Prepared Statements

A named statement can be created in any connection by using
`parse`. However, the named statement is only created in that
connection. Once the connection is released (via `sync`), you may not
get the same connection back from the pool.

Named prepared statements that are available in all pooled connection,
can be setup in Application configuration in [dev.config](/dev.config):

```erlang
 {pgmp, [...
         {named_statements,
          #{<<"now">> => <<"select now()">>,
            <<"cursors">> => <<"select * from pg_catalog.pg_cursors">>,
            <<"prepared">> => <<"select * from pg_catalog.pg_prepared_statements">>,
            <<"yesterday">> => <<"select timestamp 'yesterday'">>,
            <<"allballs">> => <<"select time 'allballs'">>}},
         ...]}
```

The named statements can be used in any connection:

```erlang
pgmp_connection_sync:bind(#{name => <<"allballs">>}).

pgmp_connection_sync:execute(#{}).
[{row_description, [<<"time">>]},
 {data_row, [{0, 0, 0}]},
 {command_complete,{select,1}}]
```


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

### Replication Process Overview

A replication slot is created by
[pgmp_mm_rep_log](src/pgmp_mm_rep_log.erl), from which a transactional
snapshot is created by the database. Using this snapshot,
[pgmp_rep_log_ets](src/pgmp_rep_log_ets.erl) retrieves all data from
the tables in the publication, in a single transaction (using extended
query with batched execute). Once the initial data has been collected,
streaming replication is then started, receiving changes that have
applied since the transaction snapshot to ensure no loss of data.

### Primary Key

```sql
create table xy (x integer primary key, y text);
insert into xy values (1, 'foo');
```

Create a PostgreSQL publication for that table:

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
[dev.config](/dev.config), or if you create the publication in PSQL
after `pgmp` has started.

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
create table xyz (x integer, y integer, z integer, primary key (x, y));
insert into xyz values (1, 1, 3);
```

Create a PostgreSQL publication for that table:

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
PostgreSQL table is replicated into an ETS table of the same name.

```erlang
1> ets:i(xyz).
<1   > {{1, 1}, 3}
EOT  (q)uit (p)Digits (k)ill
```

Note that replication introspects the PostgreSQL table metadata so that `{1, 1}` (x, y) is automatically used as the composite key.

Making some CRUD within PostgreSQL:

```sql
insert into xyz values (1, 2, 3), (1, 3, 5), (1, 4, 5);
update xyz set z = 4 where x = 1 and y = 1;
delete from xyz where x = 1 and y = 3;
```

The changes are streamed to `pgmp` and applied to the ETS table automatically.

Replication slots can be viewed via:

```sql
select slot_name,plugin,slot_type,active,xmin,catalog_xmin,restart_lsn from pg_replication_slots;

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
