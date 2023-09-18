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


-module(pgmp_codec).


-export([demarshal/1]).
-export([demarshal/2]).
-export([marshal/2]).
-export([size_exclusive/1]).
-export([size_inclusive/1]).
-export_type([command_complete_response/0]).
-export_type([row_description/0]).
-include_lib("kernel/include/logger.hrl").


-type pg_integer() :: int8
                    | int16
                    | int32
                    | int64.

-type type() :: {byte, pos_integer()}
              | pg_integer()
              | {int, pgmp:int_bit_sizes()}
              | clock
              | string
              | empty_query_response
              | bind_complete
              | portal_suspended
              | no_data
              | parse_complete
              | notice_response
              | error_response
              | authentication
              | parameter_description
              | x_log_data
              | tuple_data
              | copy_data
              | copy_both_response
              | parameter_status
              | backend_key_data
              | command_complete
              | ready_for_query
              | row_description
              | data_row.

demarshal({Type, Encoded}) ->
    ?LOG_DEBUG(#{type => Type, encoded => Encoded}),
    ?FUNCTION_NAME(Type, Encoded).

-type x_log_begin_transaction() :: #{final_lsn := pgmp:int64(),
                                     commit_timestamp := pgmp:int64(),
                                     xid := pgmp:int32()}.

-type x_log_logical_decoding() :: #{xid := pgmp:int32(),
                                    flags := pgmp:int8(),
                                    lsn := pgmp:int64(),
                                    prefix := binary(),
                                    content := pgmp:int32()}.

-type x_log_commit() :: #{flags := pgmp:int8(),
                          commit_lsn := pgmp:int64(),
                          end_lsn := pgmp:int64(),
                          commit_timestamp := pgmp:int64()}.

-type x_log_origin() :: #{commit_lsn := pgmp:int64(),
                          name := binary()}.

-type x_log_relation() :: #{id := pgmp:int32(),
                            namespace := binary(),
                            name := binary(),
                            replica_identity := pgmp:int8(),
                            ncols := pgmp:int16()}.

-type x_log_type() :: #{xid := pgmp:int32(),
                        type := pgmp:int32(),
                        namespace := binary(),
                        name := binary()}.

-type x_log_insert() :: #{relation := pgmp:int32(),
                          tuple := [tuple_data()]}.

-type x_log_update() :: #{relation := pgmp:int32(),
                          key := [tuple_data()],
                          new := [tuple_data()]}.

-type x_log_delete() :: #{relation := pgmp:int32(),
                          key := [tuple_data()]}.

-type x_log_truncate() :: #{relations := [pgmp:int32()],
                            options := pgmp:int8()}.

-type x_log_data() :: {begin_transaction, x_log_begin_transaction()}
                    | {logical_decoding, x_log_logical_decoding()}
                    | {commit, x_log_commit()}
                    | {origin, x_log_origin()}
                    | {relation, x_log_relation()}
                    | {type, x_log_type()}
                    | {insert, x_log_insert()}
                    | {update, x_log_update()}
                    | {delete, x_log_delete()}
                    | {truncate, x_log_truncate()}.

-type startup() :: #{major := integer(),
                     minor := integer(),
                     parameters := #{binary() => binary()}}.

-type copy_both_response() :: #{format := text | binary,
                                n_of_cols := non_neg_integer()}.

-type copy_out_response() :: #{format := text | binary,
                               col_formats := [text | binary]}.

-spec demarshal(nonempty_list(type()), binary()) -> {[any()], binary()};
               ({byte, pos_integer()}, binary()) -> {binary(), binary()};
               ({int, 8}, binary()) -> {pgmp:int8(), binary()};
               (int8, binary()) -> {pgmp:int8(), binary()};
               ({int, 16}, binary()) -> {pgmp:int16(), binary()};
               (int16, binary()) -> {pgmp:int16(), binary()};
               ({int, 32}, binary()) -> {pgmp:int32(), binary()};
               (int32, binary()) -> {pgmp:int32(), binary()};
               ({int, 64}, binary()) -> {pgmp:int64(), binary()};
               (int64, binary()) -> {pgmp:int64(), binary()};
               (clock, binary()) -> {pgmp:int64(), binary()};
               (string, binary()) -> {binary(), binary()};
               (empty_query_response, binary()) -> {[], <<>>};
               (bind_complete, binary()) -> {[], <<>>};
               (portal_suspended, binary()) -> {[], <<>>};
               (no_data, binary()) -> {[], <<>>};
               (parse_complete, binary()) -> {[], <<>>};
               (notice_response, binary()) -> {any(), binary()};
               (error_response, binary()) -> {any(), binary()};
               (authentication, binary()) -> {any(), binary()};
               (parameter_description, binary()) -> {[pgmp:int32()], binary()};
               (password, binary()) -> {binary(), binary()};
               (startup, binary()) -> {startup(), binary()};
               (x_log_data, binary()) -> {x_log_data(), binary()};
               (tuple_data, binary()) -> {[tuple_data()], binary()};
               (pairs, binary()) -> {#{}, binary()};
               (copy_out_response, binary()) -> {copy_out_response(), binary()};
               (copy_both_response, binary()) -> {copy_both_response(), binary()};
               (copy_data, binary()) -> {{x_log_data, x_log_data()}, binary()};
               (ssl, binary()) -> {ssl, boolean()}.

demarshal(Types, Encoded) when is_list(Types) ->
    ?LOG_DEBUG(#{types => Types, encoded => Encoded}),
    lists:mapfoldl(fun demarshal/2, Encoded, Types);

demarshal({byte, N}, Encoded) ->
    <<Decoded:N/bytes, Remainder/bytes>> = Encoded,
    {Decoded, Remainder};

demarshal(int8, Encoded) ->
    ?FUNCTION_NAME({int, 8}, Encoded);

demarshal(int16, Encoded) ->
    ?FUNCTION_NAME({int, 16}, Encoded);

demarshal(int32, Encoded) ->
    ?FUNCTION_NAME({int, 32}, Encoded);

demarshal(int64, Encoded) ->
    ?FUNCTION_NAME({int, 64}, Encoded);

demarshal(clock, Encoded) ->
    {Clock, Remainder} = ?FUNCTION_NAME({int, 64}, Encoded),
    {pgmp_calendar:decode(Clock), Remainder};

demarshal({int, Bits}, Encoded)
  when Bits == 8; Bits == 16; Bits == 32; Bits == 64 ->
    <<Decoded:Bits/signed, Remainder/bytes>> = Encoded,
    {Decoded, Remainder};

demarshal(string, Encoded) ->
    list_to_tuple(binary:split(Encoded, <<0>>));

demarshal(Type, <<>>)
  when Type == empty_query_response;
       Type == bind_complete;
       Type == portal_suspended;
       Type == no_data;
       Type == parse_complete ->
    {[], <<>>};

demarshal(Type, Encoded) when Type == notice_response; Type == error_response ->
    ?FUNCTION_NAME(Type, Encoded, []);

demarshal(authentication, Encoded) ->
    {Code, R} = ?FUNCTION_NAME(int32, Encoded),
    ?FUNCTION_NAME(authentication, Code, R);

demarshal(parameter_description, Encoded) ->
    {N, R0} = ?FUNCTION_NAME(int16, Encoded),
    ?FUNCTION_NAME(lists:duplicate(N, int32), R0);

demarshal(x_log_data, <<"B", BeginMessage/bytes>>) ->
    ?LOG_DEBUG(#{start_transaction => BeginMessage}),
    {Decoded, Remainder} =  ?FUNCTION_NAME(
                               [final_lsn, commit_timestamp, xid],
                               [int64, int64, int32],
                               BeginMessage),
    {{begin_transaction, Decoded}, Remainder};

demarshal(x_log_data, <<"M", LogicalDecoding/bytes>>) ->
    ?LOG_DEBUG(#{logical => LogicalDecoding}),
    {Decoded, Remainder} =  ?FUNCTION_NAME(
                               [xid, flags, lsn, prefix, content],
                               [int32, int8, int64, string, int32],
                               LogicalDecoding),
    {{logical_decoding, Decoded}, Remainder};

demarshal(x_log_data, <<"C", CommitMessage/bytes>>) ->
    ?LOG_DEBUG(#{commit => CommitMessage}),
    {Decoded, Remainder} =  ?FUNCTION_NAME(
                               [flags, commit_lsn, end_lsn, commit_timestamp],
                               [int8, int64, int64, int64],
                               CommitMessage),
    {{commit, Decoded}, Remainder};

demarshal(x_log_data, <<"O", OriginMessage/bytes>>) ->
    ?LOG_DEBUG(#{origin => OriginMessage}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [commit_lsn, name],
                              [int64, string],
                              OriginMessage),
    {{origin, Decoded}, Remainder};

demarshal(x_log_data, <<"R", RelationMessage/bytes>>) ->
    ?LOG_DEBUG(#{relation => RelationMessage}),
    {Relation, R0} = ?FUNCTION_NAME(
                        [id, namespace, name, replica_identity, ncols],
                        [int32, string, string, int8, int16],
                        RelationMessage),
    Columns = lists:reverse(
                pgmp_binary:foldl(
                  fun
                      (EncodedColumn, A) ->
                          {Column, Remainder} = ?FUNCTION_NAME(
                                                   [flags, name, type, modifier],
                                                   [int8, string, int32, int32],
                                                   EncodedColumn),
                          {Remainder, [Column | A]}
                  end,
                  [],
                  R0)),
    {{relation, Relation#{columns => Columns}}, <<>>};

demarshal(x_log_data, <<"Y", TypeMessage/bytes>>) ->
    ?LOG_DEBUG(#{type => TypeMessage}),
    {Decoded, Remainder} = ?FUNCTION_NAME([xid, type, namespace, name],
                                          [in32, int32, string, string],
                                          TypeMessage),
    {{type, Decoded}, Remainder};

demarshal(x_log_data, <<"I", Id:32, "N", TupleData/bytes>>) ->
    ?LOG_DEBUG(#{id => Id, tuple => TupleData}),
    {Tuple, Remainder} = ?FUNCTION_NAME(tuple_data, TupleData),
    {{insert, #{relation => Id, tuple => Tuple}}, Remainder};

demarshal(x_log_data, <<"U", Relation:32, "K", KeyData/bytes>>) ->
    ?LOG_DEBUG(#{update => Relation, key => KeyData}),
    {Key, <<"N", NewData/bytes>>} = ?FUNCTION_NAME(tuple_data, KeyData),
    {New, Remainder} = ?FUNCTION_NAME(tuple_data, NewData),
    {{update, #{key => Key, relation => Relation, new => New}}, Remainder};

demarshal(x_log_data, <<"U", Relation:32, "N", NewData/bytes>>) ->
    ?LOG_DEBUG(#{update => Relation, new => NewData}),
    {New, Remainder} = ?FUNCTION_NAME(tuple_data, NewData),
    {{update, #{relation => Relation, new => New}}, Remainder};

demarshal(x_log_data, <<"D", Relation:32, "K", KeyData/bytes>>) ->
    ?LOG_DEBUG(#{update => Relation, delete => KeyData}),
    {Key, Remainder} = ?FUNCTION_NAME(tuple_data, KeyData),
    {{delete, #{key => Key, relation => Relation}}, Remainder};

demarshal(x_log_data, <<"T", N:32, Options:8, Data/bytes>>) ->
    ?LOG_DEBUG(#{truncate => Data}),
    {Relations, Remainder} = ?FUNCTION_NAME(
                                lists:duplicate(N, int32),
                                Data),
    {{truncate, #{options => Options, relations => Relations}}, Remainder};

demarshal(x_log_data, <<"b", BeginPrepare/bytes>>) ->
    ?LOG_DEBUG(#{begin_prepare => BeginPrepare}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [lsn, end_lsn, timestamp, xid, gid],
                              [int64, int64, int64, int32, string],
                              BeginPrepare),
    {{begin_prepare, Decoded}, Remainder};

demarshal(x_log_data, <<"P", Prepare/bytes>>) ->
    ?LOG_DEBUG(#{prepare => Prepare}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [flags, lsn, end_lsn, timestamp, xid, gid],
                              [int8, int64, int64, int64, int32, string],
                              Prepare),
    {{prepare, Decoded}, Remainder};

demarshal(x_log_data, <<"K", CommitPrepared/bytes>>) ->
    ?LOG_DEBUG(#{commit_prepared => CommitPrepared}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [flags, lsn, end_lsn, timestamp, xid, gid],
                              [int8, int64, int64, int64, int32, string],
                              CommitPrepared),
    {{commit_prepared, Decoded}, Remainder};

demarshal(x_log_data, <<"r", RollbackPrepared/bytes>>) ->
    ?LOG_DEBUG(#{rollback_prepared => RollbackPrepared}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [flags,
                               prepared_end_lsn,
                               rollback_end_lsn,
                               prepare_timestamp,
                               rollback_timestamp,
                               xid,
                               gid],
                              [int8, int64, int64, int64, int64, int32, string],
                              RollbackPrepared),
    {{rollback_prepared, Decoded}, Remainder};

demarshal(tuple_data, <<Columns:16, TupleData/bytes>>) ->
    ?LOG_DEBUG(#{columns => Columns, tuple => TupleData}),
    ?FUNCTION_NAME(tuple_data, Columns, TupleData, []);

demarshal(copy_data, <<"w", XLogData/bytes>>) ->
    ?LOG_DEBUG(#{x_log => XLogData}),
    {Decoded, Stream} = ?FUNCTION_NAME(
                           [start_wal, end_wal, clock],
                           [int64, int64, int64],
                           XLogData),
    {WAL, Remainder} = ?FUNCTION_NAME(x_log_data, Stream),
    {{x_log_data, Decoded#{stream => WAL}}, Remainder};

demarshal(copy_data, <<"k", PrimaryKeepalive/bytes>>) ->
    ?LOG_DEBUG(#{primary_keepalive => PrimaryKeepalive}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [end_wal, clock, reply],
                              [int64, int64, {byte, 1}],
                              PrimaryKeepalive),
    {{keepalive,
      maps:map(
        fun
            (reply, <<0:8>>) ->
                false;

            (reply, <<1:8>>) ->
                true;

            (_, V) ->
                V
        end,
        Decoded)},
     Remainder};

demarshal(copy_data, <<"r", StandbyStatusUpdate/bytes>>) ->
    ?LOG_DEBUG(#{standby_status_update => StandbyStatusUpdate}),
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [written,
                               flushed,
                               applied,
                               clock,
                               reply],
                              [int64,
                               int64,
                               int64,
                               int64,
                               {byte, 1}],
                           StandbyStatusUpdate),
    {{standby_status_update,
      maps:map(
        fun
            (reply, <<0:8>>) ->
                false;

            (reply, <<1:8>>) ->
                true;

            (_, V) ->
                V
        end,
        Decoded)},
     Remainder};

demarshal(copy_data = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    {Encoded, <<>>};

demarshal(copy_out_response = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    {[Format, NumOfCols], R0} = ?FUNCTION_NAME([int8, int16], Encoded),
    {ColFormats, R1} = ?FUNCTION_NAME(
                          lists:duplicate(NumOfCols, int16),
                          R0),

    {#{format => text_binary(Format),
       col_formats => lists:map(
                        fun text_binary/1,
                        ColFormats)},
     R1};

demarshal(copy_done = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    {Encoded, <<>>};

demarshal(copy_both_response = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    case ?FUNCTION_NAME([int8, int16], Encoded) of
        {[0, N], <<>>} ->
            {#{format => text, n_of_cols => N}, <<>>};

        {[1, N], <<>>} ->
            {#{format => binary, n_of_cols => N}, <<>>}
    end;

demarshal(parameter_status = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    {[K, V], R0} = ?FUNCTION_NAME([string, string], Encoded),
    {{K, V}, R0};

demarshal(backend_key_data = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    ?FUNCTION_NAME([int32, int32], Encoded);

demarshal(command_complete = Tag, Encoded) ->
    ?LOG_DEBUG(#{Tag => Encoded}),
    {Decoded, Remainder} = demarshal(string, Encoded),
    ?FUNCTION_NAME(Tag, Decoded, Remainder);


demarshal(ready_for_query, TxStatus) ->
    {tx_status(TxStatus), <<>>};

demarshal(row_description = Type, Encoded) ->
    ?LOG_DEBUG(#{Type => Encoded}),
    {Columns, Remainder} = demarshal(int16, Encoded),
    ?FUNCTION_NAME(Type, Columns, Remainder, []);

demarshal(data_row = Type, Encoded) ->
    ?LOG_DEBUG(#{Type => Encoded}),
    {Columns, Remainder} = demarshal(int16, Encoded),
    ?FUNCTION_NAME(Type, Columns, Remainder, []);

demarshal(startup, Encoded) ->
    ?LOG_DEBUG(#{startup => Encoded}),
    {[Major, Minor], R0} = ?FUNCTION_NAME([int16, int16], Encoded),
    {Parameters, R1} = ?FUNCTION_NAME(pairs, R0),
    {#{major => Major, minor => Minor, parameters => Parameters}, R1};

demarshal(pairs, Encoded) ->
    ?LOG_DEBUG(#{pairs => Encoded}),
    {pgmp_binary:foldl(
       fun
           (<<0>>, A) ->
               {<<>>, A};

           (EncodedPair, A) ->
               {[Key, Value], Remainder} = ?FUNCTION_NAME(
                                              [string, string],
                                              EncodedPair),
               {Remainder, A#{Key => Value}}
       end,
       #{},
       Encoded),
     <<>>};

demarshal(password, Encoded) ->
    ?FUNCTION_NAME(string, Encoded);

demarshal(query, Encoded) ->
    ?FUNCTION_NAME(string, Encoded);

demarshal(ssl, <<"N">>) ->
    {false, <<>>};
demarshal(ssl, <<"S">>) ->
    {true, <<>>};

demarshal(terminate, <<>>) ->
    {#{}, <<>>}.


-type command_complete_operation() :: insert
                                    | delete
                                    | select
                                    | update
                                    | move
                                    | fetch
                                    | copy.

-type command_complete_response() :: {{command_complete_operation(), non_neg_integer()}, binary()}
                                   | {atom(), binary()}.

-type authentication_response() :: {authenticated, binary()}
                                 | {kerberos, binary()}
                                 | {{md5_password, binary()}, binary()}
                                 | {scm_credentials, binary()}
                                 | {gss, binary()}
                                 | {sspi, binary()}
                                 | {{sasl, any()}, binary()}
                                 | {{sasl_continue, binary()}, <<>>}
                                 | {{sasl_final, binary()}, <<>>}.

-spec demarshal(command_complete, binary(), binary()) -> command_complete_response();
               (authentication, non_neg_integer(), binary()) -> authentication_response();
               (notice_response, binary(), [{<<_:8>>, binary()}]) -> {[{<<_:8>>, binary()}], binary()};
               (error_response, binary(), [{<<_:8>>, binary()}]) -> {[{<<_:8>>, binary()}], binary()};
               ([atom()], [type()], binary()) -> {#{atom() => any()}, binary()}.


demarshal(command_complete, <<"INSERT 0 ", Rows/bytes>>, Remainder) ->
    {{insert, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, <<"DELETE ", Rows/bytes>>, Remainder) ->
    {{delete, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, <<"SELECT ", Rows/bytes>>, Remainder) ->
    {{select, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, <<"UPDATE ", Rows/bytes>>, Remainder) ->
    {{update, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, <<"MOVE ", Rows/bytes>>, Remainder) ->
    {{move, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, <<"FETCH ", Rows/bytes>>, Remainder) ->
    {{fetch, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, <<"COPY ", Rows/bytes>>, Remainder) ->
    {{copy, binary_to_integer(Rows)}, Remainder};

demarshal(command_complete, Decoded, Remainder) ->
    {pgmp_util:snake_case(
       lists:map(
         fun
             (Name) ->
                 binary_to_list(string:lowercase(Name))
         end,
         binary:split(
           Decoded,
           <<" ">>,
           [global]))),
     Remainder};

demarshal(authentication, 0, R) ->
    {authenticated, R};

demarshal(authentication, 2, R) ->
    {kerberos, R};

demarshal(authentication, 3, R) ->
    {clear_text_password, R};

demarshal(authentication, 5, R0) ->
    {Salt, R1} = ?FUNCTION_NAME({byte, 4}, R0),
    {{md5_password, Salt}, R1};

demarshal(authentication, 6, R) ->
    {scm_credentials, R};

demarshal(authentication, 7, R) ->
    {gss, R};

demarshal(authentication, 9, R) ->
    {sspi, R};

demarshal(authentication, 10, R) ->
    {{sasl,
     lists:reverse(
       pgmp_binary:foldl(
         fun
             (<<0:8>>, A) ->
                 {<<>>, A};

             (Encoded, A) ->
                 {Mechanism, Remainder} = ?FUNCTION_NAME(string, Encoded),
                 {Remainder, [Mechanism | A]}
         end,
         [],
         R))},
     <<>>};

demarshal(authentication, 11, R) ->
    {{sasl_continue, R}, <<>>};

demarshal(authentication, 12, R) ->
    {{sasl_final, R}, <<>>};

demarshal(Type, <<0:8, Remainder/bytes>>, A)
  when Type == notice_response;
       Type == error_response ->
    {lists:reverse(A), Remainder};

demarshal(Type, <<Tag:1/bytes, R0/bytes>>, A)
  when Type == notice_response;
       Type == error_response ->
    {Value, R1} = ?FUNCTION_NAME(string, R0),
    ?FUNCTION_NAME(Type, R1, [{Tag, Value} | A]);

demarshal(Keys, Types, Encoded) when is_list(Keys), is_list(Types) ->
    {Values, Remainder} = ?FUNCTION_NAME(Types, Encoded),
    {maps:from_list(lists:zip(Keys, Values)), Remainder}.


-type row_description() :: #{field_name := binary(),
                             table_oid := pgmp:int32(),
                             column_number := pgmp:int16(),
                             type_oid := pgmp:int32(),
                             type_size := pgmp:int16(),
                             type_modifier := pgmp:int32(),
                             format := text | binary}.

-type tuple_data() :: #{format := text, value := any()}
                    | null.

-spec demarshal(row_description, non_neg_integer(), binary(), [row_description()]) -> {[row_description(), ...], binary()};
               (data_row, non_neg_integer(), binary(), [pgmp_data_row:decoded()]) -> {[pgmp_data_row:decoded()], binary()};
               (tuple_data, non_neg_integer(), binary(), [tuple_data()]) -> {[tuple_data()], binary()}.


demarshal(row_description, 0, Remainder, A) ->
    {lists:reverse(A), Remainder};

demarshal(row_description = Type, Columns, D0, A) ->
    Keys = [field_name,
            table_oid,
            column_number,
            type_oid,
            type_size,
            type_modifier,
            format],

    Types = [string,
             int32,
             int16,
             int32,
             int16,
             int32,
             int16],

    {#{format := Format} = KV, D1} = ?FUNCTION_NAME(Keys, Types, D0),
    ?FUNCTION_NAME(Type,
                   Columns - 1,
                   D1,
                   [KV#{format := text_binary(Format)} | A]);

demarshal(data_row, 0, Remainder, A) ->
    {lists:reverse(A), Remainder};

demarshal(data_row = Type, Column, D0, A) ->
    case demarshal(int32, D0) of
        {0, D1} ->
            ?FUNCTION_NAME(Type, Column - 1, D1, [<<>> | A]);

        {-1, D1} ->
            ?FUNCTION_NAME(Type, Column - 1, D1, [null | A]);

        {Size, D1} ->
            <<Data:Size/bytes, D2/bytes>> = D1,
            ?FUNCTION_NAME(Type, Column - 1, D2, [Data | A])
    end;

demarshal(tuple_data, 0, Remainder, A) ->
    {lists:reverse(A), Remainder};


demarshal(tuple_data, Columns, <<"n", Remainder/bytes>>, A) ->
    ?FUNCTION_NAME(tuple_data,
                   Columns - 1,
                   Remainder,
                   [null | A]);

demarshal(tuple_data,
          Columns,
          <<"b", Length:32, Value:Length/bytes, Remainder/bytes>>,
          A) ->
    ?FUNCTION_NAME(tuple_data,
                   Columns - 1,
                   Remainder,
                   [#{format => binary, value => Value} | A]);

demarshal(tuple_data,
          Columns,
          <<"t", Length:32, Value:Length/bytes, Remainder/bytes>>,
          A) ->
    ?FUNCTION_NAME(tuple_data,
                   Columns - 1,
                   Remainder,
                   [#{format => text, value => Value} | A]).


-spec size_inclusive(iodata()) -> iolist().

size_inclusive(Data) ->
    [marshal(int32, iolist_size(Data) + 4), Data].


-spec size_exclusive(iodata()) -> iolist().

size_exclusive(Data) ->
    [marshal(int32, iolist_size(Data)), Data].


marshal(Names, Types, Bindings) ->
    ?LOG_DEBUG(#{names => Names, types => Types, bindings => Bindings}),
    lists:map(
      fun
          ({Name, Type}) ->
              ?FUNCTION_NAME(Type, maps:get(Name, Bindings))
      end,
      lists:zip(Names, Types)).


-spec marshal(string, iodata()) -> iolist();
             (byte, <<_:8>>) -> iodata();
             (int8, pgmp:int8()) -> nonempty_binary();
             (int16, pgmp:int16()) -> nonempty_binary();
             (int32, pgmp:int32()) -> nonempty_binary();
             (int64, pgmp:int32()) -> nonempty_binary();
             ({int, 8}, pgmp:int8()) -> nonempty_binary();
             ({int, 16}, pgmp:int16()) -> nonempty_binary();
             ({int, 32}, pgmp:int32()) -> nonempty_binary();
             ({int, 64}, pgmp:int64()) -> nonempty_binary();
             (x_log_data, x_log_data()) -> iodata();
             (authentication, ok) -> iodata();
             (ready_for_query, tx_status()) -> iodata();
             (row_description, [row_description()]) -> iodata();
             (data_row, iodata()) -> iodata();
             (command_complete, {iodata() | atom(), integer()} | iodata() | atom()) -> iodata();
             (copy_data, iodata()) -> iodata();
             (copy_both_response, copy_both_response()) -> iodata();
             (copy_out_response, copy_out_response()) -> iodata();
             (tuple_data, tuple_data()) -> iodata().

marshal(string, Value) ->
    [Value, <<0:8>>];

marshal(byte, Value) ->
    Value;

marshal({byte, N}, Value) when byte_size(Value) == N ->
    Value;

marshal(int8, Value) ->
    ?FUNCTION_NAME({int, 8}, Value);

marshal(int16, Value) ->
    ?FUNCTION_NAME({int, 16}, Value);

marshal(int32, Value) ->
    ?FUNCTION_NAME({int, 32}, Value);

marshal(int64, Value) ->
    ?FUNCTION_NAME({int, 64}, Value);

marshal({int, Size}, Value) when is_integer(Value)->
    <<Value:Size>>;

marshal(authentication, ok) ->
    ["R", size_inclusive([<<0:32>>])];

marshal(ready_for_query, TxStatus) ->
    ["Z", size_inclusive(tx_status(TxStatus))];

marshal(command_complete = Tag, {Command, Rows}) when is_atom(Command),
                                                      is_integer(Rows) ->
    ?FUNCTION_NAME(
       Tag,
       [string:uppercase(atom_to_list(Command)),
        " ",
        integer_to_list(Rows)]);

marshal(command_complete = Tag, Command) when is_atom(Command) ->
    ?FUNCTION_NAME(Tag, string:uppercase(atom_to_list(Command)));

marshal(command_complete, Description) ->
    ["C", size_inclusive(marshal(string, Description))];

marshal(row_description, FieldDescriptions) ->
    ["T",
     size_inclusive(
       [marshal(int16, length(FieldDescriptions)),
        lists:map(
          fun
              (#{field_name := FieldName,
                 table_oid := TableOID,
                 column_number := ColumnNumber,
                 type_oid := TypeOID,
                 type_size := TypeSize,
                 type_modifier := TypeModifier,
                 format := Format}) ->
                  [marshal(string, FieldName),
                   marshal(int32, TableOID),
                   marshal(int16, ColumnNumber),
                   marshal(int32, TypeOID),
                   marshal(int16, TypeSize),
                   marshal(int32, TypeModifier),
                   marshal(int16, text_binary(Format))]
          end,
          FieldDescriptions)])];

marshal(data_row, Columns) ->
    ["D",
     size_inclusive(
       [marshal(int16, length(Columns)),
        lists:map(
          fun
              (<<-1:32/signed>> = Column) ->
                  Column;

              (Column) ->
                  size_exclusive(Column)
          end,
          Columns)])];

marshal(copy_data, {standby_status_update, StandbyStatusUpdate}) ->
    ["d",
     size_inclusive(
       ["r",
        ?FUNCTION_NAME(
           [written,
            flushed,
            applied,
            clock,
            reply],
           [int64,
            int64,
            int64,
            int64,
            {byte, 1}],
          maps:map(
            fun
                (reply, false) ->
                    <<0:8>>;

                (reply, true) ->

                    <<1:8>>;

                (_, V) ->
                    V
            end,
            StandbyStatusUpdate))])];

marshal(copy_data, {keepalive, PrimaryKeepalive}) ->
    ["d",
     size_inclusive(
       ["k",
        ?FUNCTION_NAME(
          [end_wal, clock, reply],
          [int64, int64, {byte, 1}],
          maps:map(
            fun
                (reply, false) ->
                    <<0:8>>;

                (reply, true) ->

                    <<1:8>>;

                (_, V) ->
                    V
            end,
            PrimaryKeepalive))])];

marshal(copy_data, {x_log_data, #{stream := Stream} = XLogData}) ->
    ["d",
     size_inclusive(
       ["w",
        ?FUNCTION_NAME(
           [start_wal, end_wal, clock],
           [int64, int64, int64],
           XLogData),
        ?FUNCTION_NAME(x_log_data, Stream)])];

marshal(x_log_data, {begin_transaction, BeginTransaction}) ->
    ["B",
     ?FUNCTION_NAME(
        [final_lsn, commit_timestamp, xid],
        [int64, int64, int32],
        BeginTransaction)];

marshal(x_log_data, {relation, #{columns := Columns} = Relation}) ->
    ["R",
     ?FUNCTION_NAME(
        [id, namespace, name, replica_identity, ncols],
        [int32, string, string, int8, int16],
        maps:merge(
          #{ncols => length(Columns)},
          Relation)),
     lists:map(
       fun
           (Column) ->
               ?FUNCTION_NAME(
                  [flags, name, type, modifier],
                  [int8, string, int32, int32],
                  Column)
       end,
       Columns)];

marshal(x_log_data,
        {insert,
         #{relation := Relation, tuple := Tuple}}) ->
    ["I",
     ?FUNCTION_NAME(int32, Relation),
     "N",
     ?FUNCTION_NAME(tuple_data, Tuple)];

marshal(x_log_data,
        {update, #{key := Key, relation := Relation, new := New}}) ->
    ["U",
     ?FUNCTION_NAME(int32, Relation),
     "K",
     ?FUNCTION_NAME(tuple_data, Key),
     "N",
     ?FUNCTION_NAME(tuple_data, New)];

marshal(x_log_data,
        {update, #{relation := Relation, new := New}}) ->
    ["U",
     ?FUNCTION_NAME(int32, Relation),
     "N",
     ?FUNCTION_NAME(tuple_data, New)];

marshal(x_log_data,
        {delete, #{key := Key, relation := Relation}}) ->
    ["D",
     ?FUNCTION_NAME(int32, Relation),
     "K",
     ?FUNCTION_NAME(tuple_data, Key)];

marshal(x_log_data,
        {truncate, #{options := Options, relations := Relations}}) ->
    ["T",
     ?FUNCTION_NAME(int32, length(Relations)),
     ?FUNCTION_NAME(int8, Options),
     [?FUNCTION_NAME(int32, Relation) || Relation <- Relations]];

marshal(tuple_data, Columns) ->
    [?FUNCTION_NAME(int16, length(Columns)),
     lists:map(
       fun
           (null) ->
               "n";

           (#{format := Format, value := Value}) ->
               [case Format of
                    binary ->
                        "b";

                    text ->
                        "t"
                end,
                ?FUNCTION_NAME(int32, byte_size(Value)), Value]
       end,
       Columns)];

marshal(x_log_data, {commit, Commit}) ->
    ["C",
     ?FUNCTION_NAME(
        [flags, commit_lsn, end_lsn, commit_timestamp],
        [int8, int64, int64, int64],
        Commit)];

marshal(copy_both_response, #{format := Format, n_of_cols := N}) ->
    ["W",
     size_inclusive(
       [?FUNCTION_NAME(int8, text_binary(Format)),
        ?FUNCTION_NAME(int16, N)])];

marshal(copy_out_response, #{format := Format, col_formats := ColFormats}) ->
    ["H",
     size_inclusive(
       [?FUNCTION_NAME(int8, text_binary(Format)),
        ?FUNCTION_NAME(int16, length(ColFormats)),
        lists:map(
          fun
              (ColFormat) ->
                  ?FUNCTION_NAME(int16, text_binary(ColFormat))
          end,
          ColFormats)])];

marshal(copy_data, Encoded) ->
    ["d", size_inclusive(Encoded)];

marshal(copy_done, <<>> = Encoded) ->
    ["c", size_inclusive(Encoded)];

marshal(parameter_status, {Key, Value}) ->
    ["S",
     size_inclusive(
       [?FUNCTION_NAME(string, Key),
        ?FUNCTION_NAME(string, Value)])];

marshal(Type, Value) ->
    error(badarg, [Type, Value]).


-type tx_status() :: idle
                   | in_tx_block
                   | in_failed_tx_block.

tx_status(idle) ->
    <<"I">>;
tx_status(in_tx_block) ->
    <<"T">>;
tx_status(in_failed_tx_block) ->
    <<"E">>;
tx_status(<<"I">>) ->
    idle;
tx_status(<<"T">>) ->
    in_tx_block;
tx_status(<<"E">>) ->
    in_failed_tx_block.


text_binary(text) -> 0;
text_binary(binary) -> 1;
text_binary(0) -> text;
text_binary(1) -> binary.
