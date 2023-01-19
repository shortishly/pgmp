# Telemetry

Instrumentation is provided via [Telemetry][telemetry] to plug into a
monitoring system of your choice.

Telemetry may be configured using application properties, please refer
to [dev.config](/dev.config) for an example.

```erlang
%% telemetry function must have arity 4.
{telemetry_module, pgmp_telemetry_logger},
{telemetry_function, handle},

%% consult the filename, attaching all events found:
{telemetry_event_names, "priv/telemetry.terms"},

%% final supplied parameter to the telemetry handler
{telemetry_config, {hello,world}},
```

Then use one or more of the `attach_many` calls to `telemetry` in the
following sections to observe instrumentation events.

## Socket

Low level socket operations are observed by attaching to the following
events:

- [pgmp, socket, connect]
- [pgmp, socket, open]
- [pgmp, socket, recv]
- [pgmp, socket, send]
- [pgmp, socket, tag_msg]

For debug, attaching in an Erlang shell:

```erlang
telemetry:attach_many(
  socket,
  [[pgmp, socket, connect],
   [pgmp, socket, open],
   [pgmp, socket, recv],
   [pgmp, socket, send],
   [pgmp, socket, tag_msg]],
   fun pgmp_telemetry_logger:handle/4,
   []).
```

### Socket: Connect

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

### Socket: Open

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

### Socket: Recv

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

### Socket: Send

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

### Socket: TagMsg

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

## Middleman

Higher level protocol events are observed by attaching to the
middleman.

Simple events:

- [pgmp, mm, execute]
- [pgmp, mm, parse]
- [pgmp, mm, recv]

Spans for the major SQL commands:

- [pgmp, mm, bind, start]
- [pgmp, mm, bind, stop]
- [pgmp, mm, describe, start]
- [pgmp, mm, describe, stop]
- [pgmp, mm, execute, start]
- [pgmp, mm, execute, stop]
- [pgmp, mm, parse, start]
- [pgmp, mm, parse, stop]
- [pgmp, mm, query, start]
- [pgmp, mm, query, stop]
- [pgmp, mm, sync, stop]
- [pgmp, mm, sync, stop]

For debug, attaching in an Erlang shell:

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
  fun pgmp_telemetry_logger:handle/4,
  []).
```

### Middleman: Execute

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

### Middleman: Parse

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

### Middleman: Recv

A measurement of the number of tagged messages received by this
middleman. The metadata contains the `tag` together with the `socket`
the message was received from.

```erlang
event_name: [pgmp, mm, recv]
measurements: #{count => 1}
metadata: #{socket => <0.747.0>, tag => parse_complete}
```

### Middleman Span: Parse

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

### Middleman Span: Bind

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

### Middleman Span: Execute

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

## Replication

Logical replication is observed by attaching to the following events:

- [pgmp, mm, rep, begin_transaction]
- [pgmp, mm, rep, insert]
- [pgmp, mm, rep, update]
- [pgmp, mm, rep, delete]
- [pgmp, mm, rep, truncate]
- [pgmp, mm, rep, commit]
- [pgmp, mm, rep, keepalive]

For debug, attaching in an Erlang shell:

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
  fun pgmp_telemetry_logger:handle/4,
  []).
```

### Replication: Begin Transaction

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

### Replication: Insert

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

### Replication: Update

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

### Replication: Delete

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

### Replication: Truncate

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

### Replication: Commit

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

### Replication: Keep alive

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

[telemetry]: https://github.com/beam-telemetry/telemetry
