# Postgresql Message Protocol (PGMP)

An implementation of the [Postgresql Message
Protocol](https://www.postgresql.org/docs/current/protocol.html) for
[Erlang/OTP 25](https://www.erlang.org).

Features:

- [Simple Query](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4)
- [Extended Query](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
- Binary protocol (preferred) and textual when required.
- Binary protocol for parse, bind and execute during an [Extended
  Query](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY).
- Asynchronus requests using
  [send_request](https://www.erlang.org/doc/man/gen_statem.html#send_request-4).
  Available for the client to use, and used internally within the
  implementation.
- A [connection](src/pgmp_connection.erl) pool that is aware of the underlying
  statement state (a simple or extended query, outside or within a
  transaction block).
- [Logical Streaming Replication](https://www.postgresql.org/docs/current/protocol-logical-replication.html)

## Query Example:

An asynchronous simple query request using [`receive_response`](https://www.erlang.org/doc/man/gen_statem.html#receive_response-1) to wait for the response:

```erlang
1> gen_statem:receive_response(pgmp_connection:query(#{sql => <<"select 2*3">>})).
{reply, [{row_description, [<<"?column?">>]},
         {data_row, [6]},
         {command_complete, {select, 1}}]}
```

## Replication Example:

An example of logical replication of a single table with a composite key:

```sql
create table xyz (x integer, y integer, z integer, primary key (x, y));
insert into xyz values (1, 1, 3);
```

Create a Postgresql publication for that table:

```sql
create publication pub for table xyz;
```

With `pgmp` running:

```erlang
1> ets:i(xyz).
<1   > {{1, 1}, 3}
EOT  (q)uit (p)Digits (k)ill
```

Note that replication introspects the Postgresql table metadata so that `{1, 1}` (x, y) is automatically used as the composite key.

Making some data within Postgresql:

```sql
insert into xyz values (1, 2, 3), (1, 3, 5), (1, 4, 5);
update xyz set z = 4 where x = 1 and y = 1;
delete from xyz where x = 1 and y = 3;
```

The changes are streamed to `pgmp` and applied to the ETS table automatically.


## Internals

The implementation uses the following recently introduced features of Erlang/OTP:

- [socket](https://www.erlang.org/doc/man/socket.html) for all
  communication
- [`send_request`](https://www.erlang.org/doc/man/gen_statem.html#send_request-4)
  for asynchronous request and response.
- [`change_callback_module`](https://www.erlang.org/doc/man/gen_statem.html#type-action)
  to switch between startup, various authentication implementations,
  simple and extended query modes.
