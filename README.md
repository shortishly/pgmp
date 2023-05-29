# PostgreSQL Message Protocol (PGMP)

An implementation of the [PostgreSQL Message
Protocol][postgresql-org-protocol] for [Erlang/OTP 25+][erlang-org].

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
- a [GitHub Codespace](docs/codespaces.md) for easy development and evaluation.
- Support for [JSON, JSONB and XML](docs/json.md)
- Details of [logical replication support](docs/logical-replication.md).
- Instrumented with [telemetry](docs/telemetry.md).
  
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

[erlang-org-check-response-3]: https://www.erlang.org/doc/man/gen_statem.html#check_response-3
[erlang-org-receive-response-1]: https://www.erlang.org/doc/man/gen_statem.html#receive_response-1
[erlang-org-request-id-collection]: https://www.erlang.org/doc/man/gen_statem.html#type-request_id_collection
[erlang-org-send-request-4]: https://www.erlang.org/doc/man/gen_statem.html#send_request-4
[erlang-org]: https://www.erlang.org
[github-com-pgec]: https://github.com/shortishly/pgec
[github-com-proper]: https://github.com/proper-testing/proper
[postgresql-org-extended-query]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
[postgresql-org-log-rep-col-lists]: https://www.postgresql.org/docs/15/logical-replication-col-lists.html
[postgresql-org-logical-streaming-replication]: https://www.postgresql.org/docs/current/protocol-logical-replication.html
[postgresql-org-protocol]: https://www.postgresql.org/docs/current/protocol.html
[postgresql-org-rep-row-filter]: https://www.postgresql.org/docs/15/logical-replication-row-filter.html
[postgresql-org-simple-query]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[shortishly-com-postgresql-edge-cache]: https://shortishly.com/blog/postgresql-edge-cache/
[shortishly-com-property-testing-a-database-driver]: https://shortishly.com/blog/property-testing-a-database-driver/
