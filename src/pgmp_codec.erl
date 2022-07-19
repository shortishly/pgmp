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
-export([prefix_with_size/1]).


demarshal({Type, Encoded}) ->
    ?FUNCTION_NAME(Type, Encoded).


demarshal(Types, Encoded) when is_list(Types) ->
    lists:mapfoldl(fun demarshal/2, Encoded, Types);

demarshal({byte, N}, Encoded) ->
    <<Decoded:N/bytes, Remainder/bytes>> = Encoded,
    {Decoded, Remainder};

demarshal({int, Bits}, Encoded) ->
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
    {Code, R} = ?FUNCTION_NAME({int, 32}, Encoded),
    ?FUNCTION_NAME(authentication, Code, R);

demarshal(parameter_description, Encoded) ->
    {N, R0} = ?FUNCTION_NAME({int, 16}, Encoded),
    ?FUNCTION_NAME(lists:duplicate(N, {int, 32}), R0);

demarshal(parameter_status, Encoded) ->
    {[K, V], R0} = ?FUNCTION_NAME([string, string], Encoded),
    {{K, V}, R0};

demarshal(backend_key_data, Encoded) ->
    ?FUNCTION_NAME([{int, 32}, {int, 32}], Encoded);

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
    {Columns, Remainder} = demarshal({int, 16}, Encoded),
    ?FUNCTION_NAME(Type, Columns, Remainder, []);

demarshal(data_row = Type, Encoded) ->
    {Columns, Remainder} = demarshal({int, 16}, Encoded),
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
    Types = [string, {int, 32}, {int, 16}, {int, 32}, {int, 16}, {int, 32}, {int, 16}],

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
    case demarshal({int, 32}, D0) of
        {0, D1} ->
            ?FUNCTION_NAME(Type, Column - 1, D1, [<<>> | A]);

        {-1, D1} ->
            ?FUNCTION_NAME(Type, Column - 1, D1, [null | A]);

        {Size, D1} ->
            <<Data:Size/bytes, D2/bytes>> = D1,
            ?FUNCTION_NAME(Type, Column - 1, D2, [Data | A])
    end.


prefix_with_size(Data) ->
    [marshal({int, 32}, iolist_size(Data) + 4), Data].

marshal(string, Value) ->
    [Value, <<0:8>>];

marshal(byte, Value) ->
    Value;

marshal({int, Size}, Value) when is_integer(Value)->
    <<Value:Size>>.
