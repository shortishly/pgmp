%% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


-module(pgmp_mm_rep_log).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_data_row, [decode/3]).
-import(pgmp_statem, [cancel_generic_timeout/1]).
-import(pgmp_statem, [generic_timeout/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


callback_mode() ->
    [handle_event_function, state_enter].


terminate(Reason, State, Data) ->
    pgmp_mm_common:terminate(Reason, State, Data).


handle_event(internal,
             {recv, {copy_both_response, CopyBothRespone}},
             _,
             _) ->
    ?LOG_DEBUG(#{copy_both_response => CopyBothRespone}),
    keep_state_and_data;

handle_event(internal, {recv, {copy_data, {Tag, _} = TM}}, _, _)
  when Tag == keepalive; Tag == x_log_data ->
    ?LOG_DEBUG(#{copy_data => TM}),
    {keep_state_and_data,
     [nei(TM),
      generic_timeout(replication_ping),
      cancel_generic_timeout(replication_ping_no_reply)]};

handle_event(internal,
             {x_log_data,
              #{clock := Clock,
                end_wal := EndWAL,
                start_wal := StartWAL,
                stream := {Command, Arg}}},
             _,
             #{wal := WAL} = Data) ->
    {keep_state,
     Data#{wal := WAL#{received := EndWAL,
                       flushed := EndWAL,
                       clock := Clock,
                       applied := EndWAL}},
     nei({Command,
          Arg#{x_log => #{clock => pgmp_calendar:decode(Clock),
                          end_wal => EndWAL,
                          start_wal => StartWAL}}})};

handle_event(internal,
             {optional_callback, F, A},
             _,
             #{config := #{module := M}}) ->
    case pgmp_util:is_exported(M, F, 1) of
        true ->
            {keep_state_and_data, nei({callback, F, A})};

        false ->
            keep_state_and_data
    end;

handle_event(internal,
             {callback, F, A},
             _,
             #{config := #{manager := Manager,
                           module := M},
               requests := Requests} = Data) ->
    {keep_state,
     Data#{requests => M:F(A#{server_ref => Manager,
                              label => F,
                              requests => Requests})}};

handle_event(internal, {begin_prepare = Change, Arg}, _, _) ->
    ?LOG_DEBUG(#{Change => Arg}),
    {keep_state_and_data,
     nei({rep_telemetry, Change, #{count => 1}})};

handle_event(internal, {prepare = Change, Arg}, _, _) ->
    ?LOG_DEBUG(#{Change => Arg}),
    {keep_state_and_data,
     nei({rep_telemetry, Change, #{count => 1}})};

handle_event(internal, {commit_prepared = Change, Arg}, _, _) ->
    ?LOG_DEBUG(#{Change => Arg}),
    {keep_state_and_data,
     nei({rep_telemetry, Change, #{count => 1}})};

handle_event(internal, {rollback_prepared = Change, Arg}, _, _) ->
    ?LOG_DEBUG(#{Change => Arg}),
    {keep_state_and_data,
     nei({rep_telemetry, Change, #{count => 1}})};

handle_event(internal,
             {begin_transaction =  Change, Arg},
             _,
             _) ->
    ?LOG_DEBUG(#{Change => Arg}),
    {keep_state_and_data,
     [nei({optional_callback,
           Change,
           maps:with([commit_timestamp,
                      final_lsn,
                      xid,
                      x_log], Arg)}),

      nei({rep_telemetry, Change, #{count => 1}})]};

handle_event(internal,
             {insert = Change,
              #{relation := Relation,
                x_log := XLog,
                tuple := Values} = Arg},
             _,
             #{relations := Relations,
               config := Config,
               parameters := Parameters}) ->
    ?LOG_DEBUG(#{Change => Arg}),
    #{Relation := #{columns := Columns} = Detail}  = Relations,
    {keep_state_and_data,
     [nei({callback,
           Change,
           #{relation => relation(Detail),
             x_log => XLog,
             tuple => row_tuple(Config, Parameters, Columns, Values)}}),

      nei({rep_telemetry,
           Change,
           #{count => 1},
           #{relation => relation(Detail)}})]};

handle_event(internal,
             {update = Change,
              #{relation := Relation,
                x_log := XLog,
                new := Values} = Arg},
             _,
             #{relations := Relations,
               config := Config,
               parameters := Parameters}) ->
    ?LOG_DEBUG(#{Change => Arg}),
    #{Relation := #{columns := Columns} = Detail} = Relations,
    {keep_state_and_data,
     [nei({callback,
           Change,
           #{relation => relation(Detail),
             x_log => XLog,
             tuple => row_tuple(Config, Parameters, Columns, Values)}}),

      nei({rep_telemetry,
           Change,
           #{count => 1},
           #{relation => relation(Detail)}})]};

handle_event(internal,
             {delete = Change,
              #{relation := Relation, x_log := XLog, key := Values} = Arg},
             _,
             #{relations := Relations,
               config := Config,
               parameters := Parameters}) ->
    ?LOG_DEBUG(#{Change => Arg}),
    #{Relation := #{columns := Columns} = Detail} = Relations,
    {keep_state_and_data,
     [nei({callback,
           Change,
           #{relation => relation(Detail),
             x_log => XLog,
             tuple => row_tuple(Config, Parameters, Columns, Values)}}),

      nei({rep_telemetry,
           Change,
           #{count => 1},
           #{relation => relation(Detail)}})]};

handle_event(internal,
             {truncate = Change,
              #{x_log := XLog,
                relations := Truncates} = Arg},
             _,
             #{relations := Relations}) ->
    ?LOG_DEBUG(#{Change => Arg}),
    Names = lists:map(
              fun
                  (Relation) ->
                      #{Relation := Detail} = Relations,
                      relation(Detail)
              end,
              Truncates),
    {keep_state_and_data,
     [nei({callback, Change, #{relations => Names, x_log => XLog}}),

      nei({rep_telemetry, Change, #{count => 1}, #{relations => Names}})]};

handle_event(internal, {commit = Change, Arg}, _, _) ->
    ?LOG_DEBUG(#{Change => Arg}),
    {keep_state_and_data,
     [nei({optional_callback,
           Change,
           maps:with([commit_lsn,
                      commit_timestamp,
                      end_lsn],
                     Arg)}),

      nei({rep_telemetry, Change, #{count => 1}})]};

handle_event(internal,
             {relation, #{id := Id} = Relation},
             _,
             #{relations := Relations} = Data) ->
    ?LOG_DEBUG(#{relation => Relation}),
    {keep_state,
     Data#{relations := Relations#{Id => maps:without([id], Relation)}}};

handle_event(internal,
             {keepalive = Change,
              #{clock := Clock,
                end_wal := EndWAL,
                reply := true}},
             _,
             #{wal := WAL} = Data) ->
    {keep_state,
     Data#{wal := WAL#{clock := Clock, received := EndWAL}},
     [nei(ping),

      nei({rep_telemetry, Change, #{count => 1}})]};

handle_event(internal, {keepalive, _}, _, _) ->
    keep_state_and_data;

handle_event({timeout, replication_ping}, _, replication, _) ->
    {keep_state_and_data,
     [nei(ping), generic_timeout(replication_ping_no_reply)]};

handle_event(internal,
             ping,
             _,
             #{wal := #{received := ReceivedWAL,
                        clock := Clock,
                        flushed :=  FlushedWAL,
                        applied := AppliedWAL}}) ->
    {keep_state_and_data,
     nei({standby_status_update,
          #{received_wal => ReceivedWAL,
            flushed_wal => FlushedWAL,
            applied_wal => AppliedWAL,
            clock => Clock,
            reply => true}})};

handle_event(internal,
             {response, #{label := Change, reply := ok}},
             _,
             _)
  when Change == insert;
       Change == update;
       Change == delete;
       Change == truncate;
       Change == begin_transaction;
       Change == commit ->
       keep_state_and_data;

handle_event(internal,
             {response,
              #{label := snapshot,
                reply :=  {error, Reason}}},
             waiting_for_snapshot_completion,
             _) ->
    {stop, Reason};

handle_event(internal,
             {lsn, LSN},
             _,
             Data) when is_integer(LSN) ->
    {keep_state,
     Data#{lsn => pgmp_lsn:encode(LSN),
           wal => #{received => LSN,
                    clock => 0,
                    flushed => LSN,
                    applied => LSN}}};

handle_event(internal,
             {response,
              #{label := snapshot,
                reply :=  ok}},
             waiting_for_snapshot_completion,
             Data) ->
    {next_state,
     replication,
     Data,
     [nei({lsn, 0}),
      nei(start_replication),
      generic_timeout(replication_ping)]};

handle_event(
  internal,
  {response, #{label := lsn, reply := {ok, LSN}}},
  waiting_for_lsn,
  Data) when is_integer(LSN) ->
    {next_state,
     replication,
     Data,
     [nei({lsn, LSN}),
      nei(start_replication),
      generic_timeout(replication_ping)]};

handle_event(
  internal,
  {response, #{label := lsn, reply := not_found}},
  waiting_for_lsn,
  Data) ->
    {next_state,
     replication,
     Data,
     [nei({lsn, 0}),
      nei(start_replication),
      generic_timeout(replication_ping)]};

handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             waiting_for_types,
             #{types_ready := false} = Data) ->
    {next_state,
     identify_system,
     Data#{types_ready := true},
     [nei(manager), nei(identify_system)]};

handle_event(internal,
             server_version,
             _,
             #{parameters := #{<<"server_version">> := ServerVersion}} = Data) ->
    {keep_state,
     Data#{server_version => pgmp_util:semantic_version(ServerVersion)}};

handle_event(internal,
             bootstrap_complete,
             _,
             #{types_ready := false} = Data) ->
    {next_state, waiting_for_types, Data, nei(server_version)};

handle_event(internal,
             bootstrap_complete,
             _,
             #{types_ready := true} = Data) ->
    {next_state,
     identify_system,
     Data,
     [nei(server_version),
      nei(manager),
      nei(identify_system)]};

handle_event(internal,
             manager,
             _,
             #{config := Config} = Data) ->
    [_, LogicalSup | _] = get('$ancestors'),
    case pgmp_sup:get_child(LogicalSup, manager) of
        {_, Manager, worker, _} when is_pid(Manager) ->
            {keep_state,
             Data#{config := Config#{manager => Manager,
                                     module => pgmp_config:replication(
                                                 logical, module)}}}
    end;

handle_event(internal, identify_system, _, _) ->
    {keep_state_and_data, nei({query, <<"IDENTIFY_SYSTEM">>})};

handle_event(internal,
             {recv, {command_complete, {copy, 0}}},
             _,
             _) ->
    %% Sent during DB shutdown to indicate that the replication
    %% process is finishing.
    stop;

handle_event(internal,
             {recv, {command_complete, Command}},
             _,
             Data) when Command == identify_system;
                        Command == create_replication_slot ->
    {keep_state, maps:without([columns], Data)};

handle_event(internal,
             {recv, {command_complete, select}},
             State,
             #{server_version := #{major :=  12}} = Data)
  when State == identify_system;
       State == replication_slot ->
    {keep_state, maps:without([columns], Data)};


handle_event(internal,
             {recv, {ready_for_query, idle}},
             identify_system,
             Data) ->
    %% logger:set_module_level([pgmp_mm_rep_log, pgmp_codec], debug),
    {next_state, replication_slot, Data, nei(create_replication_slot)};

handle_event(
  internal,
  {recv, {ready_for_query, idle}},
  replication_slot,
  #{replication_slot := #{<<"snapshot_name">> := Snapshot}} = Data) ->
    {next_state,
     waiting_for_snapshot_completion,
     Data,
     nei({callback, snapshot, #{id => Snapshot}})};

handle_event(
  internal,
  {recv, {ready_for_query, idle}},
  replication_slot,
  Data) ->
    {next_state,
     waiting_for_lsn,
     Data,
     nei({callback, lsn, #{}})};

handle_event(
  internal,
  {recv, {error_response, _} = TM},
  replication_slot,
  Data) ->
    case pgmp_error_notice_fields:map(TM) of
        {error_response, #{code := <<"42710">>}} ->
            keep_state_and_data;

        {Tag, Message} ->
            {next_state,
             limbo,
             Data,
             [nei({telemetry,
                   error,
                   #{count => 1},
                   maps:merge(
                     #{event => Tag},
                     maps:with(
                       [code, message, severity],
                       Message))}),
              {state_timeout,
               timer:seconds(
                 backoff:rand_increment(
                   pgmp_config:backoff(rand_increment))),
               {backoff, #{action => Tag, reason => Message}}}]}
    end;

handle_event(internal,
             create_replication_slot = Command,
             _,
             #{config := #{publication := Publication}}) ->
    {keep_state_and_data,
     nei({Command, pgmp_rep_log:slot_name(Publication)})};

handle_event(internal,
             {create_replication_slot, SlotName},
             _,
             #{server_version := ServerVersion}) ->
    {keep_state_and_data,
     nei({query,
          lists:join(
            " ",
            ["CREATE_REPLICATION_SLOT",
             ["\"", SlotName, "\""],
             ["TEMPORARY" || pgmp_config:replication(logical, temporary)],
             "LOGICAL",
             "pgoutput",
             create_slot_options(
               protocol_version(ServerVersion))])})};

handle_event(internal,
             start_replication,
             _,
             #{config := #{publication := PublicationNames},
               server_version := ServerVersion}) ->
    {keep_state_and_data,
     nei({start_replication,
          pgoutput_options(
            protocol_version(ServerVersion),
            PublicationNames)})};

handle_event(internal,
             {start_replication, Options},
             _,
             #{lsn := LSN,
               config := #{publication := Publication}} = Data) ->
    {keep_state,
     Data#{relations => #{}},
     nei({query,
          lists:join(
            " ",
            ["START_REPLICATION SLOT",
             pgmp_rep_log:slot_name(Publication),
             "LOGICAL",
             LSN,
             ["(",
              maps:fold(
                fun
                    (K, V, A) ->
                        [A,
                         [", " || A /= []],
                         atom_to_list(K),
                         " ",
                         "'",
                         any:to_list(V),
                         "'"]
                end,
                [],
                Options),
              ")"]])})};

handle_event(internal, {recv, {row_description, Columns}}, _, Data) ->
    ?LOG_DEBUG(#{row_description => Columns}),
    {keep_state, Data#{columns => Columns}};

handle_event(internal,
             {recv, {data_row, Values}},
             State,
             #{parameters := Parameters,
               config := Config,
               columns := Columns} = Data) ->
    ?LOG_DEBUG(#{data_row => Values}),
    {keep_state,
     maps:put(State,
              lists:foldl(
                fun
                    ({#{field_name := FieldName}, Value}, A) ->
                        A#{FieldName => Value}
                end,
                maps:get(State, Data, #{}),
                lists:zip(Columns,
                          decode(
                            Parameters,
                            lists:zip(Columns, Values),
                            pgmp_types:cache(Config)))),
              Data)};

handle_event(internal, {query, SQL}, _, _) ->
    ?LOG_DEBUG(#{query => SQL}),
    {keep_state_and_data,
     nei({send, ["Q", size_inclusive([marshal(string, SQL)])]})};

handle_event(internal,
             {standby_status_update,
              #{received_wal := ReceivedWAL,
                flushed_wal := FlushedWAL,
                applied_wal := AppliedWAL,
                clock := Clock,
                reply := Reply}},
             _,
             _) ->
    {keep_state_and_data,
     nei({send,
          ["d",
           size_inclusive(
             ["r",
              <<ReceivedWAL:64,
                FlushedWAL:64,
                AppliedWAL:64,
                Clock:64,
                (b(Reply)):8>>])]})};

handle_event(internal, {rep_telemetry, EventName, Measurements}, _, _) ->
    {keep_state_and_data, nei({rep_telemetry, EventName, Measurements, #{}})};

handle_event(internal,
             {rep_telemetry, EventName, Measurements, Metadata},
             _,
             #{config := #{publication := Publication}} = Data) ->
    {keep_state_and_data,
     nei({telemetry,
          [rep, EventName],
          maps:merge(
            Measurements,
            maps:with([wal], Data)),
          maps:merge(
            Metadata#{publication => Publication},
            maps:with([identify_system, replication_slot], Data))})};

handle_event(info,
             {'DOWN' ,_ , process, Manager, noproc},
             _,
             #{manager := Manager}) ->
    stop;

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).


row_tuple(Config, Parameters, Columns, Values) ->
    list_to_tuple(
      pgmp_data_row:decode(
        Parameters,
        lists:map(
          fun
              ({#{type := Type}, null = Value}) ->
                  {#{format => text, type_oid => Type}, Value};

              ({#{type := Type}, #{format := Format, value := Value}}) ->
                  {#{format => Format, type_oid => Type}, Value}
          end,
          lists:zip(Columns, Values)),
        pgmp_types:cache(Config))).


b(false) -> 0;
b(true) -> 1;
b(0) -> false;
b(1) -> true.


relation(Detail) ->
    maps:with([name, namespace], Detail).

create_slot_options(ProtoVersion) when ProtoVersion >= 3 ->
    ["(",
     lists:foldl(
       fun
           (Option, A) ->
               case pgmp_config:replication(logical, Option) of
                   false ->
                       A;

                   true ->
                       [A,
                        [", " || A /= []],
                        slot_option_name(Option)];

                   Value ->
                       [A,
                        [", " || A /= []],
                        slot_option(Option, Value)]
               end
       end,
       [],
       [two_phase, reserve_wal, snapshot]),
     ")"];

create_slot_options(2) ->
    case pgmp_config:replication(logical, snapshot) of
        export ->
            "EXPORT_SNAPSHOT";
        no ->
            "NOEXPORT_SNAPSHOT";
        use ->
            "USE_SNAPSHOT"
    end.


slot_option(Name, Value) when is_boolean(Value) ->
    lists:join(
      " ",
      [slot_option_name(Name), slot_option_value(Value)]);

slot_option(Name, Value) ->
    lists:join(
      " ",
      [slot_option_name(Name), slot_option_value(Value)]).

slot_option_name(Name) ->
    string:uppercase(atom_to_list(Name)).

slot_option_value(Value) when is_boolean(Value) ->
    ["'", atom_to_list(Value), "'"];

slot_option_value(Value) ->
    ["'", atom_to_list(Value), "'"].


pgoutput_options(ProtoVersion, PublicationNames) ->
    lists:foldl(
      fun
          (K, A) ->
              A#{K => pgmp_config:pgoutput(K, ProtoVersion)}
      end,
      #{proto_version => ProtoVersion,
        publication_names => PublicationNames},
      pgoutput_options(ProtoVersion)).


protocol_version(#{major := Major}) when Major >= 16 ->
    4;

protocol_version(#{major := Major}) when Major >= 15 ->
    3;

protocol_version(#{major := Major}) when Major >= 14 ->
    2;

protocol_version(#{major := _}) ->
    1.


pgoutput_options(ProtocolVersion) when ProtocolVersion >= 4 ->
    [binary, messages, streaming, two_phase, origin];

pgoutput_options(ProtocolVersion) when ProtocolVersion >= 3 ->
    [binary, messages, streaming, two_phase];

pgoutput_options(ProtocolVersion) when ProtocolVersion >= 2 ->
    [binary, messages, streaming];

pgoutput_options(_) ->
    [].
