% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
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


demarshal({Type, Encoded}) ->
    ?FUNCTION_NAME(Type, Encoded).


demarshal(Types, Encoded) when is_list(Types) ->
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
    {Decoded, Remainder} =  ?FUNCTION_NAME(
                               [final_lsn, commit_timestamp, xid],
                               [int64, int64, int32],
                               BeginMessage),
    {{begin_transaction, Decoded}, Remainder};

demarshal(x_log_data, <<"M", LogicalDecoding/bytes>>) ->
    {Decoded, Remainder} =  ?FUNCTION_NAME(
                               [xid, flags, lsn, prefix, content],
                               [int32, int8, int64, string, int32],
                               LogicalDecoding),
    {{logical_decoding, Decoded}, Remainder};

demarshal(x_log_data, <<"C", CommitMessage/bytes>>) ->
    {Decoded, Remainder} =  ?FUNCTION_NAME(
                               [flags, commit_lsn, end_lsn, commit_timestamp],
                               [int8, int64, int64, int64],
                               CommitMessage),
    {{commit, Decoded}, Remainder};

demarshal(x_log_data, <<"O", OriginMessage/bytes>>) ->
    {Decoded, Remainder} = ?FUNCTION_NAME(
                              [commit_lsn, name],
                              [int64, string],
                              OriginMessage),
    {{origin, Decoded}, Remainder};

demarshal(x_log_data, <<"R", RelationMessage/bytes>>) ->
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
    {Decoded, Remainder} = ?FUNCTION_NAME([xid, type, namespace, name],
                                          [in32, int32, string, string],
                                          TypeMessage),
    {{type, Decoded}, Remainder};

demarshal(x_log_data, <<"I", Id:32, "N", TupleData/bytes>>) ->
    {Tuple, Remainder} = ?FUNCTION_NAME(tuple_data, TupleData),
    {{insert, #{relation => Id, tuple => Tuple}}, Remainder};

demarshal(x_log_data, <<"U", Relation:32, "K", KeyData/bytes>>) ->
    {Key, <<"N", NewData/bytes>>} = ?FUNCTION_NAME(tuple_data, KeyData),
    {New, Remainder} = ?FUNCTION_NAME(tuple_data, NewData),
    {{update, #{key => Key, relation => Relation, new => New}}, Remainder};

demarshal(x_log_data, <<"U", Relation:32, "N", NewData/bytes>>) ->
    {New, Remainder} = ?FUNCTION_NAME(tuple_data, NewData),
    {{update, #{relation => Relation, new => New}}, Remainder};

demarshal(x_log_data, <<"D", Relation:32, "K", KeyData/bytes>>) ->
    {Key, Remainder} = ?FUNCTION_NAME(tuple_data, KeyData),
    {{delete, #{key => Key, relation => Relation}}, Remainder};

demarshal(x_log_data, <<"T", N:32, Options:8, Data/bytes>>) ->
    {Relations, Remainder} = ?FUNCTION_NAME(
                                lists:duplicate(N, int32),
                                Data),
    {{truncate, #{options => Options, relations => Relations}}, Remainder};

demarshal(tuple_data, <<Columns:16, TupleData/bytes>>) ->
    ?FUNCTION_NAME(tuple_data, Columns, TupleData, []);

demarshal(copy_data, <<"w", XLogData/bytes>>) ->
    {Decoded, Stream} = ?FUNCTION_NAME(
                           [start_wal, end_wal, clock],
                           [int64, int64, int64],
                           XLogData),
    {WAL, Remainder} = ?FUNCTION_NAME(x_log_data, Stream),
    {{x_log_data, Decoded#{stream => WAL}}, Remainder};

demarshal(copy_data, <<"k", PrimaryKeepalive/bytes>>) ->
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

demarshal(copy_data, Encoded) ->
    {Encoded, <<>>};

demarshal(copy_both_response, Encoded) ->
    case ?FUNCTION_NAME([int8, int16], Encoded) of
        {[0, N], <<>>} ->
            {#{format => text, n_of_cols => N}, <<>>};

        {[1, N], <<>>} ->
            {#{format => binary, n_of_cols => N}, <<>>}
    end;

demarshal(parameter_status, Encoded) ->
    {[K, V], R0} = ?FUNCTION_NAME([string, string], Encoded),
    {{K, V}, R0};

demarshal(backend_key_data, Encoded) ->
    ?FUNCTION_NAME([int32, int32], Encoded);

demarshal(command_complete = Tag, Encoded) ->
    {Decoded, Remainder} = demarshal(string, Encoded),
    ?FUNCTION_NAME(Tag, Decoded, Remainder);


demarshal(ready_for_query, <<"I">>) ->
    {idle, <<>>};

demarshal(ready_for_query, <<"T">>) ->
    {in_tx_block, <<>>};

demarshal(ready_for_query, <<"E">>) ->
    {in_failed_tx_block, <<>>};

demarshal(row_description = Type, Encoded) ->
    {Columns, Remainder} = demarshal(int16, Encoded),
    ?FUNCTION_NAME(Type, Columns, Remainder, []);

demarshal(data_row = Type, Encoded) ->
    {Columns, Remainder} = demarshal(int16, Encoded),
    ?FUNCTION_NAME(Type, Columns, Remainder, []).


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


demarshal(row_description, 0, Remainder, A) ->
    {lists:reverse(A), Remainder};

demarshal(row_description = Type, Columns, D0, A) ->
    Keys = [field_name, table_oid, column_number, type_oid, type_size, type_modifier, format],
    Types = [string, int32, int16, int32, int16, int32, int16],

    {KV, D1} = ?FUNCTION_NAME(Keys, Types, D0),
    ?FUNCTION_NAME(Type,
                   Columns - 1,
                   D1,
                   [case KV of
                        #{format := 0} ->
                            KV#{format := text};

                        #{format := 1} ->
                            KV#{format := binary}
                    end | A]);

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
          <<"t", Length:32, Value:Length/bytes, Remainder/bytes>>,
          A) ->
    ?FUNCTION_NAME(tuple_data,
                   Columns - 1,
                   Remainder,
                   [#{format => text, value => Value} | A]).


size_inclusive(Data) ->
    [marshal(int32, iolist_size(Data) + 4), Data].

size_exclusive(Data) ->
    [marshal(int32, iolist_size(Data)), Data].

marshal(string, Value) ->
    [Value, <<0:8>>];

marshal(byte, Value) ->
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
    <<Value:Size>>.
