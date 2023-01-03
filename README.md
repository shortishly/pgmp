# PostgreSQL Message Protocol (PGMP)

An implementation of the [PostgreSQL Message
Protocol][postgresql-org-protocol] for [Erlang/OTP 25][erlang-org].

Features:

- [Simple Query][postgresql-org-simple-query].
- [Extended Query][postgresql-org-extended-query].
- [Logical Streaming
  Replication][postgresql-org-logical-streaming-replication] including
  [column lists][postgresql-org-log-rep-col-lists] and [row
  filters][postgresql-org-rep-row-filter].
- Binary protocol (preferred) and textual when required.
- Binary protocol for parse, bind and execute during an [Extended Query][postgresql-org-extended-query].
- Asynchronous requests using
  [send_request][erlang-org-send-request-4]. Available for the client
  to use, and used internally within the implementation.
- A statement aware [connection](src/pgmp_connection.erl) pool
  (simple or extended query, outside or within a transaction block).
- Instrumented with [Telemetry][telemetry].
  
Notes:

- [PGEC][github-com-pgec] is an example OTP Application [using PGMP
  and logical replication][shortishly-com-postgresql-edge-cache].
- PGMP uses [property based
  testing][shortishly-com-property-testing-a-database-driver] with
  [PropEr][github-com-proper].

![main](https://github.com/shortishly/pgmp/actions/workflows/main.yml/badge.svg)

## Asynchronous Requests

The following two sections show how to make an asynchronous request.

### send_request/2 with receive_response/1

You can immediately wait for a response (via
[receive_response/1][erlang-org-receive-response-1]). For simplicity,
the following examples use this method:

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
1> pgmp_connection_sync:query(#{sql => <<"select 2*3">>}).
[{row_description, [<<"?column?">>]},
 {data_row, [6]},
 {command_complete, {select, 1}}]
```

### send_request/4 with check_response/3

The [send_request/4][erlang-org-send-request-4] and
[check_response/3][erlang-org-check-response-3] pattern allows you to
respond to other messages rather than blocking. Use this option, when writing
another `gen_*` behaviour (`gen_server`, `gen_statem`, etc) is calling `pgmp`.

So using another `gen_statem` as an example:

The following `init/1` sets up some state with a [request id
collection][erlang-org-request-id-collection] to maintain our
outstanding asynchronous requests.

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
made using [send_request/4][erlang-org-send-request-4]. The response
will be received as an `info` message as follows:

```erlang
handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            %% You have a response with a Label so that you stitch it
            %% back to the original request...
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

An asynchronous simple query request using
[receive_response][erlang-org-receive-response-1] to wait for the
response:

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
connection. Once the connection is released, you may not get the same
connection back from the pool.

Named prepared statements that are available in all pooled connections,
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

### Replication Process Overview

A replication slot is created by
[pgmp_mm_rep_log](src/pgmp_mm_rep_log.erl), from which a snapshot is
created by the database. Using this snapshot,
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

### Composite Key

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

### PostgreSQL version 15 Logical Replication Features

The logical replication features in the [PostgreSQL 15
release][postgresql-org-15-release], are: [row
filters][postgresql-org-rep-row-filter] and [column
lists][postgresql-org-log-rep-col-lists]. Their usage with
[pgmp][pgmp] are described in this
[article][shortishly-com-pgmp-logical-replication-in-postgresql-fifteen].

## JSON/XML

Any encoder or decoder for JSON/XML can be used, providing they
implement `decode/1` and `encode/1`.

To use [jsx][github-com-jsx], apply the following configuration:

```erlang
 {pgmp, [...
         {codec_json, jsx},
         ...]}
```

## Telemetry

Instrumentation is provided via [Telemetry][telemetry] to plug into a
monitoring system of your choice.

For example, to observe the data that is available, you can attach a
simple logger in a terminal:

```erlang
Logger = fun (EventName, Measurements, Metadata, Config) ->
  logger:alert(#{event_name => EventName,
                 measurements => Measurements,
                 metadata => Metadata,
                 config => Config})
end.
```

Then use one or more of the `attach_many` calls to `telemetry` in the
following sections to observe instrumentation events.

### Socket

Low level socket operations are observed by attaching to the following
events:

- [pgmp, socket, connect],
- [pgmp, socket, open],
- [pgmp, socket, recv],
- [pgmp, socket, send],
- [pgmp, socket, tag_msg]

Attaching to the `logger` defined above:

```erlang
telemetry:attach_many(
  socket,
  [[pgmp, socket, connect],
   [pgmp, socket, open],
   [pgmp, socket, recv],
   [pgmp, socket, send],
   [pgmp, socket, tag_msg]],
   Logger,
   []).
```

#### Socket: Connect

A measurement of the count of socket `connect` events. The metadata
includes the remote address/port together with the `peer` (middleman)
using this `socket`.

```erlang
event_name: [pgmp,socket,connect]
measurements: #{count => 1}
metadata: #{peer => <0.907.0>,
            socket => {'$socket',#Ref<0.772806049.528613377.230969>},
            telemetry =>
                #{db => #{system => postgresql},
                  net =>
                      #{peer => #{name => {127,0,0,1},port => 5432},
                        sock => #{family => inet}}}}
```

#### Socket: Open

A measurement of the count of socket `open` events. The metadata does
not include the address or port at this stage, but does include the
`peer` (middleman) using this `socket`.

```erlang
event_name: [pgmp,socket,open]
measurements: #{count => 1}
metadata: #{peer => <0.907.0>,
            socket => {'$socket',#Ref<0.772806049.528613377.230969>},
            telemetry => #{db => #{system => postgresql}}}
```

#### Socket: Recv

A measurement of the number of bytes received on this socket.

```erlang
measurements: #{bytes => 24}
metadata: #{handle => #Ref<0.772806049.528482305.230978>,
            peer => <0.907.0>,
            socket => {'$socket',#Ref<0.772806049.528613377.230969>},
            telemetry =>
                #{db => #{system => postgresql},
                  net =>
                      #{peer => #{name => {127,0,0,1},port => 5432},
                        sock => #{family => inet}}}}
```

#### Socket: Send

A measurement of the number of bytes sent on this socket.

```erlang
event_name: [pgmp,socket,send]
measurements: #{bytes => 59}
metadata: #{peer => <0.907.0>,
            socket => {'$socket',#Ref<0.772806049.528613377.230969>},
            telemetry =>
                #{db => #{system => postgresql},
                  net =>
                      #{peer => #{name => {127,0,0,1},port => 5432},
                        sock => #{family => inet}}}}
```

#### Socket: TagMsg

A measurement of the number of tagged messages received on this
socket, with the number of encoded message bytes in the packet which
will be handled by the middleman.

```erlang
measurements: #{bytes => 19,count => 1}
metadata: #{peer => <0.907.0>,
            socket => {'$socket',#Ref<0.772806049.528613377.230969>},
            tag => authentication,
            telemetry =>
                #{db => #{system => postgresql},
                  net =>
                      #{peer => #{name => {127,0,0,1},port => 5432},
                        sock => #{family => inet}}}}
```

### Middleman

Higher level protocol events are observed by attaching to the
middleman.

Simple events:

- [pgmp, mm, execute],
- [pgmp, mm, parse],
- [pgmp, mm, recv],

Spans for the major SQL commands:

- [pgmp, mm, bind, start],
- [pgmp, mm, bind, stop],
- [pgmp, mm, describe, start],
- [pgmp, mm, describe, stop],
- [pgmp, mm, execute, start],
- [pgmp, mm, execute, stop],
- [pgmp, mm, parse, start],
- [pgmp, mm, parse, stop],
- [pgmp, mm, query, start],
- [pgmp, mm, query, stop],
- [pgmp, mm, sync, stop],
- [pgmp, mm, sync, stop]],

Using the example `logger` from above:

```erlang
telemetry:attach_many(
  mm,
  [[pgmp, mm, execute],
   [pgmp, mm, parse],
   [pgmp, mm, recv],

   [pgmp, mm, bind, start],
   [pgmp, mm, bind, stop],

   [pgmp, mm, describe, start],
   [pgmp, mm, describe, stop],

   [pgmp, mm, execute, start],
   [pgmp, mm, execute, stop],

   [pgmp, mm, parse, start],
   [pgmp, mm, parse, stop],

   [pgmp, mm, query, start],
   [pgmp, mm, query, stop],

   [pgmp, mm, sync, stop],
   [pgmp, mm, sync, stop]],
  Logger,
  []).
```

#### Middleman: Execute

A measurement of the count of execute statement requests received by
this middleman. The metadata includes the max rows to be returned,
with the `portal` name (`<<>>` is the unnamed portal), and the
`socket` being used.

```erlang
event_name: [pgmp,mm,execute]
measurements: #{count => 1}
metadata: #{args => #{max_rows => 0,
                      portal => <<>>},
            socket => <0.111.0>}
```

#### Middleman: Parse

A measurement of the count of parse statement requests received by
this middleman. The metadata includes the prepared statement `name`
(`<<>>` is the unnamed statement), the `sql` being parsed, and the
`socket` being used.

```erlang
event_name: [pgmp, mm, parse]
measurements: #{count => 1}
metadata: #{args => #{name => <<>>,
                      sql => <<"select $1::varchar::tsvector">>},
            socket => <0.1178.0>}
```

#### Middleman: Recv

A measurement of the number of tagged messages received by this
middleman. The metadata contains the `tag` together with the `socket`
the message was received from.

```erlang
event_name: [pgmp, mm, recv]
measurements: #{count => 1}
metadata: #{socket => <0.747.0>, tag => parse_complete}
```

#### Middleman Span: Parse

Executing the following `parse` statement:

```erlang
pgmp_connection_sync:parse(#{sql => <<"select $1::varchar::tsvector">>}).
```

The `start` of the span will be logged as follows:

```erlang
event_name: [pgmp,mm,parse,start]
measurements: #{monotonic_time => -576460198818053690}
metadata: #{args => #{name => <<>>,
                      sql => <<"select $1::varchar::tsvector">>},
            socket => <0.1178.0>}

```

Followed by a `stop` event:

```erlang
event_name: [pgmp,mm,parse,stop]
measurements: #{duration => 1643103,monotonic_time => -576460198816410587}
metadata: #{args => #{name => <<>>,
                      sql => <<"select $1::varchar::tsvector">>},
            socket => <0.1178.0>}
```

#### Middleman Span: Bind

With the following `bind` statement

```erlang
1> pgmp_connection_sync:bind(
     #{args => [<<"a fat cat sat on a mat and ate a fat rat">>]}).
```

The `start` of the span will be logged as follows:

```erlang
event_name: [pgmp,mm,bind,start]
measurements: #{monotonic_time => -576460056393535331}
metadata: #{args => #{format => #{parameter => binary,
                                  result => binary},
                      portal => <<>>,
                      statement => <<>>,
                      values =>
                        [<<"a fat cat sat on a mat and ate a fat rat">>]},
            socket => <0.1178.0>}
```

Followed by a `stop` event:

```erlang
event_name: [pgmp,mm,bind,stop]
measurements: #{duration => 1547498,monotonic_time => -576460056391987833}
metadata: #{args => #{format => #{parameter => binary,
                                  result => binary},
                      portal => <<>>,
                      statement => <<>>,
                      values =>
                        [<<"a fat cat sat on a mat and ate a fat rat">>]},
            socket => <0.1178.0>}
```

#### Middleman Span: Execute

With the following `execute` statement:

```erlang
1> pgmp_connection_sync:execute(#{}).
```

The `start` of the span will be logged as follows:

```erlang
event_name: [pgmp,mm,execute,start]
measurements: #{monotonic_time => -576459578157988475}
metadata: #{args => #{max_rows => 0,
                      portal => <<>>},
            socket => <0.1178.0>}
```

Followed by a `stop` event. Note that, the measurements include the
number of `rows` returned by the query, and that the metadata includes
the type of `command` performed.

```erlang
event_name: [pgmp,mm,execute,stop]
measurements: #{duration => 2592141,
                monotonic_time => -576459578155396334,
                rows => 1}
metadata: #{args => #{max_rows => 0,
                      portal => <<>>},
            command => select,
            socket => <0.1178.0>}
```

### Replication

Logical replication is observed by attaching to the following events:

- [pgmp, mm, rep, begin_transaction],
- [pgmp, mm, rep, insert],
- [pgmp, mm, rep, update],
- [pgmp, mm, rep, delete],
- [pgmp, mm, rep, truncate],
- [pgmp, mm, rep, commit],
- [pgmp, mm, rep, keepalive]

Using the `logger` defined above:

```erlang
telemetry:attach_many(
  rep,
  [[pgmp, mm, rep, begin_transaction],
   [pgmp, mm, rep, insert],
   [pgmp, mm, rep, update],
   [pgmp, mm, rep, delete],
   [pgmp, mm, rep, truncate],
   [pgmp, mm, rep, commit],
   [pgmp, mm, rep, keepalive]],
  Logger,
  []).
```

#### Replication: Begin Transaction

```erlang
event_name: [pgmp, mm, rep, begin_transaction]
measurements: #{count => 1,
                wal => #{applied => 28560848,
                         clock => 726080906045932,
                         flushed => 28560848,
                         received => 28560848}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}
```

#### Replication: Insert

Note the `relation` for the insert is in the `metadata`:

```erlang
event_name: [pgmp, mm, rep, insert]
measurements: #{count => 1,
                wal => #{applied => 28562032,
                         clock => 726081314090114,
                         flushed => 28562032,
                         received => 28562032}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            relation => <<"xy">>,
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}
```

#### Replication: Update

Note the `relation` being updated in the `metadata`:

```erlang
event_name: [pgmp,mm,rep,update]
measurements: #{count => 1,
                wal => #{applied => 28562264,
                         clock => 726081521146454,
                         flushed => 28562264,
                         received => 28562264}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            relation => <<"xy">>,
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}
```

#### Replication: Delete

Note the `relation` being used for the delete in the `metadata`:

```erlang
event_name: [pgmp,mm,rep,delete]
measurements: #{count => 1,
                wal => #{applied => 28562680,
                         clock => 726081614227882,
                         flushed => 28562680,
                         received => 28562680}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            relation => <<"xy">>,
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}
```

#### Replication: Truncate

Note the `relations` being truncated in the `metadata`:

```erlang
event_name: [pgmp,mm,rep,truncate]
measurements: #{count => 1,
                wal => #{applied => 28560848,
                         clock => 726080906046112,
                         flushed => 28560848,
                         received => 28560848}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            relations => [<<"xy">>],
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}
```

#### Replication: Commit

```erlang
event_name: [pgmp,mm,rep,commit]
measurements: #{count => 1,
                wal => #{applied => 28561264,
                         clock => 726080906046134,
                         flushed => 28561264,
                         received => 28561264}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}
```

#### Replication: Keep alive

```erlang
event_name: [pgmp,mm,rep,keepalive]
measurements: #{count => 1,
                wal => #{applied => 28561264,
                         clock => 726081100794392,
                         flushed => 28561264,
                         received => 28561552}}
metadata: #{identify_system =>
                #{<<"dbname">> => <<"postgres">>,
                  <<"systemid">> => <<"7179987566950137894">>,
                  <<"timeline">> => 1,
                  <<"xlogpos">> => <<"0/1B34000">>},
            replication_slot =>
                #{<<"consistent_point">> => <<"0/1B342F0">>,
                  <<"output_plugin">> => <<"pgoutput">>,
                  <<"slot_name">> => <<"pgmp_xy">>,
                  <<"snapshot_name">> => <<"00000003-0001109B-1">>},
            socket => <0.113.0>}

```

## Internals

The implementation uses some recently introduced features of Erlang/OTP:

- [socket][erlang-org-socket] for all communication
- [send_request][erlang-org-send-request-4] for asynchronous request and response.
- [change_callback_module][erlang-org-change-callback-module] to
  switch between startup, various authentication implementations,
  simple and extended query modes.
- [pbkdf2_hmac][erlang-org-pbkdf2-hmac] in [SCRAM](src/pgmp_scram.erl)
  authentication.

[erlang-org-change-callback-module]: https://www.erlang.org/doc/man/gen_statem.html#type-action
[erlang-org-check-response-3]: https://www.erlang.org/doc/man/gen_statem.html#check_response-3
[erlang-org-pbkdf2-hmac]: https://www.erlang.org/doc/man/crypto.html#pbkdf2_hmac-5
[erlang-org-receive-response-1]: https://www.erlang.org/doc/man/gen_statem.html#receive_response-1
[erlang-org-request-id-collection]: https://www.erlang.org/doc/man/gen_statem.html#type-request_id_collection
[erlang-org-send-request-4]: https://www.erlang.org/doc/man/gen_statem.html#send_request-4
[erlang-org-socket]: https://www.erlang.org/doc/man/socket.html
[erlang-org]: https://www.erlang.org
[github-com-jsx]: https://github.com/talentdeficit/jsx
[github-com-pgec]: https://github.com/shortishly/pgec
[github-com-proper]: https://github.com/proper-testing/proper
[pgmp]: https://github.com/shortishly/pgmp
[postgresql-org-15-release]: https://www.postgresql.org/about/news/postgresql-15-released-2526/
[postgresql-org-extended-query]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
[postgresql-org-log-rep-col-lists]: https://www.postgresql.org/docs/15/logical-replication-col-lists.html
[postgresql-org-log-rep-quick-setup]: https://www.postgresql.org/docs/current/logical-replication-quick-setup.html
[postgresql-org-logical-streaming-replication]: https://www.postgresql.org/docs/current/protocol-logical-replication.html
[postgresql-org-protocol]: https://www.postgresql.org/docs/current/protocol.html
[postgresql-org-rep-row-filter]: https://www.postgresql.org/docs/15/logical-replication-row-filter.html
[postgresql-org-simple-query]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[postgresql-org]: https://www.postgresql.org/
[shortishly-com-pgmp-logical-replication-in-postgresql-fifteen]: https://shortishly.com/blog/pgmp-log-rep-postgresql-fifteen/
[shortishly-com-postgresql-edge-cache]: https://shortishly.com/blog/postgresql-edge-cache/
[shortishly-com-property-testing-a-database-driver]: https://shortishly.com/blog/property-testing-a-database-driver/
[telemetry]: https://github.com/beam-telemetry/telemetry
