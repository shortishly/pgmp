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


-module(pgmp_data_row).


-export([decode/2]).
-export([decode/3]).
-export([decode/5]).


decode(Parameters, TypeValue) ->
    ?FUNCTION_NAME(Parameters, TypeValue, pgmp_types:cache()).

decode(Parameters, TypeValue, Types) ->
    ?FUNCTION_NAME(Parameters, TypeValue, Types, []).

decode(Parameters,
       [{#{format := Format, type_oid := OID}, Value} | T],
       Types,
       A) ->
    case maps:find(OID, Types) of
        {ok, Type} ->
            ?FUNCTION_NAME(
               Parameters,
               T,
               Types,
               [?FUNCTION_NAME(Parameters,
                               Format,
                               Types,
                               Type,
                               Value) | A]);

        error ->
            ?FUNCTION_NAME(Parameters,
                           T,
                           Types,
                           [Value | A])
    end;

decode(_, [], _, A) ->
    lists:reverse(A).


decode(_, _, _, _, null) ->
    null;

decode(_, binary, _, #{<<"typname">> := <<"bool">>}, <<1>>) ->
    true;

decode(_, text, _, #{<<"typname">> := <<"bool">>}, <<"t">>) ->
    true;

decode(_, binary, _, #{<<"typname">> := <<"bool">>}, <<0>>) ->
    false;

decode(_, text, _, #{<<"typname">> := <<"bool">>}, <<"f">>) ->
    true;

decode(_, text, _, #{<<"typname">> := <<"oid">>}, Value) ->
    binary_to_integer(Value);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"oid">>},
       <<Value:32>>) ->
    Value;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"bit">>},
       <<Length:32, Data:Length/bits, 0:(8 - Length)>>) ->
    Data;

decode(_,
       text,
       _,
       #{<<"typname">> := <<"bit">>},
       Encoded) ->
    erlang:list_to_bitstring(
      [<<(binary_to_integer(I)):1>> || <<I:1/bytes>> <= Encoded]);

decode(_,
       text,
       _,
       #{<<"typname">> := <<"int", _/bytes>>},
       Encoded) ->
    binary_to_integer(Encoded);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"uuid">>},
       <<TimeLow:32, TimeMid:16, TimeHi:16, Clock:16, Node:48>>) ->
    iolist_to_binary(
      io_lib:format(
        "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
        [TimeLow, TimeMid, TimeHi, Clock, Node]));

decode(_,
       text,
       _,
       #{<<"typname">> := <<"uuid">>},
       <<_:8/bytes,
         "-",
         _:4/bytes,
         "-",
         _:4/bytes,
         "-",
         _:4/bytes,
         "-",
         _:12/bytes>> = UUID) ->
    UUID;

decode(_,
       _,
       _,
       #{<<"typname">> := Type}, Value)
  when Type == <<"xml">>;
       Type == <<"json">> ->
    (pgmp_config:decode(binary_to_list(Type))):decode(Value);


%% src/backend/utils/adt/arrayfuncs.c#array_recv
decode(Parameters,
       binary = Format,
       Types,
       #{<<"typreceive">> := TypeReceive} = Type,
       Data) when TypeReceive == <<"array_recv">>;
                  TypeReceive == <<"oidvectorrecv">> ->
    array_recv(Parameters, Format, Types, Type, Data);

decode(_,
       text,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       Encoded)
  when Encoded == <<"NaN">>;
       Encoded == <<"Infinity">>;
       Encoded == <<"-Infinity">> ->
    binary_to_atom(Encoded);

decode(_, text, _, #{<<"typname">> := <<"float", _/bytes>>}, Encoded) ->
    numeric(Encoded);

decode(_, text, _, #{<<"typname">> := <<"numeric">>}, Encoded) ->
    numeric(Encoded);

decode(_, text, _, #{<<"typname">> := <<"date">>}, <<Ye:4/bytes, "-", Mo:2/bytes, "-", Da:2/bytes>>) ->
    triple(Ye, Mo, Da);

decode(_, binary, _, #{<<"typname">> := <<"date">>}, <<Days:32>>) ->
    calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(epoch_date(pg)) + Days);

decode(_, text, _, #{<<"typname">> := <<"time">>}, <<Ho:2/bytes, ":", Mi:2/bytes, ":", Se:2/bytes>>) ->
    triple(Ho, Mi, Se);

decode(_, binary, _, #{<<"typname">> := <<"time">>}, <<Time:64>>) ->
    calendar:seconds_to_time(erlang:convert_time_unit(Time, microsecond, second));

decode(#{<<"integer_datetimes">> := <<"on">>},
       text,
       _,
       #{<<"typname">> := <<"timestamp">>},
       <<Ye:4/bytes,
         "-",
         Mo:2/bytes,
         "-",
         Da:2/bytes,
         " ",
         Ho:2/bytes,
         ":",
         Mi:2/bytes,
         ":",
         Se:2/bytes,
         _/bytes>>) ->
    {triple(Ye, Mo, Da), triple(Ho, Mi, Se)};

decode(_, binary, _, #{<<"typname">> := <<"timestamp">>}, <<Encoded:64>>) ->
    calendar:system_time_to_universal_time(
      epoch(pg) + Encoded - epoch(posix),
      microsecond);

decode(_, binary, _, #{<<"typname">> := <<"int", R/bytes>>}, Encoded) ->
    Size = binary_to_integer(R),
    <<Decoded:Size/signed-unit:8>> = Encoded,
    Decoded;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<1:1, ((1 bsl 8) - 1):8, 0:23>>) ->
    '-Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<0:1, ((1 bsl 8) - 1):8, 0:23>>) ->
    'Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<_:1, ((1 bsl 8) - 1):8, (1 bsl 22):23>>) ->
    'NaN';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<1:1, ((1 bsl 11) - 1):11, 0:52>>) ->
    '-Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<0:1, ((1 bsl 11) - 1):11, 0:52>>) ->
    'Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<_:1, ((1 bsl 11) - 1):11, (1 bsl 51):52>>) ->
    'NaN';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", R/bytes>>},
       Encoded) ->
    Size = binary_to_integer(R),
    <<Decoded:Size/signed-float-unit:8>> = Encoded,
    Decoded;

decode(_,
       _,
       _,
       #{<<"typname">> := <<"bpchar">>},
       <<Encoded/bytes>>) ->
    Encoded;

decode(#{<<"client_encoding">> := <<"UTF8">>},
       _,
       _,
       #{<<"typname">> := Type},
       <<Encoded/bytes>>) when Type == <<"varchar">>;
                               Type == <<"name">>;
                               Type == <<"text">> ->
    unicode:characters_to_binary(Encoded);

decode(#{<<"client_encoding">> := <<"SQL_ASCII">>},
       _,
       _,
       #{<<"typname">> := Type},
       <<Encoded/bytes>>) when Type == <<"varchar">>;
                               Type == <<"name">>;
                               Type == <<"text">> ->
    unicode:characters_to_binary(Encoded, latin1);

decode(_,
       text,
       _,
       #{<<"typname">> := <<"bytea">>},
       <<"\\x", Encoded/bytes>>) ->
    list_to_binary([binary_to_integer(X, 16) || <<X:2/bytes>> <= Encoded]);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"bytea">>},
       <<Encoded/bytes>>) ->
    Encoded;

decode(Parameters, Format, _TypeCache,Type, Value) ->
    #{parameters => Parameters, format => Format, type => Type, value => Value}.


numeric(Value) ->
    ?FUNCTION_NAME(
       [fun erlang:binary_to_float/1, fun erlang:binary_to_integer/1], Value).


numeric([Conversion | Conversions], Value) ->
    try
        Conversion(Value)
    catch
        error:badarg ->
            ?FUNCTION_NAME(Conversions, Value)
    end;

numeric([], Value) ->
    Value.


triple(X, Y, Z) ->
    list_to_tuple([binary_to_integer(I) || I <- [X, Y, Z]]).


epoch(System) ->
    erlang:convert_time_unit(
      calendar:datetime_to_gregorian_seconds({epoch_date(System), {0, 0, 0}}),
      second,
      microsecond).

epoch_date(pg) ->
    {2000, 1, 1};

epoch_date(posix) ->
    {1970, 1, 1}.


array_recv(Parameters, binary = Format, Types, _Type, <<_NDim:32, 0:32, OID:32, _Dimensions:32, _LowerBnds:32, Data/bytes>>) ->
    {ok, Type} =  maps:find(OID, Types),
    [decode(Parameters, Format, Types, Type, Value) || <<Length:32, Value:Length/bytes>> <= Data];

array_recv(Parameters, binary = Format, Types, _Type, <<_NDim:32, 1:32, OID:32, _Dimensions:32, _LowerBnds:32, Data/bytes>>) ->
    {ok, Type} =  maps:find(OID, Types),
    lists:reverse(
      pgmp_binary:foldl(
        fun
            (<<Length:32, Value:Length/bytes, R/bytes>>, A) ->
                {R, [decode(Parameters, Format, Types, Type, Value) | A]};

            (<<16#ffffffff:32, R/bytes>>, A) ->
                {R, [null | A]}
        end,
        [],
        Data)).
