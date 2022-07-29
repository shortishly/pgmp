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


-module(pgmp_data_row_tests).


-include_lib("eunit/include/eunit.hrl").


decode_test_() ->
    lists:map(
      test_decode(decode(types())),
      phrase_file:consult("test/data-row.terms")).

decode(Types) ->
    fun
        (Parameters, TypeValue) ->
            pgmp_data_row:decode(Parameters, TypeValue, Types)
    end.


test_decode(F) ->
    fun
        ({Expected, Parameters, Format, Type, Value} = Test) ->
            {nm(Test),
             ?_assertEqual(
                [Expected],
                apply(F,
                      [Parameters,
                       [{#{format => Format, type_oid => Type}, Value}]]))}
    end.


encode_test_() ->
    lists:map(
      test_encode(encode(types())),
      phrase_file:consult("test/data-row.terms")).

encode(Types) ->
    fun
        (Parameters, TypeValue) ->
            pgmp_data_row:encode(Parameters, TypeValue, Types)
    end.


test_encode(F) ->
    fun
        ({Value, Parameters, Format, Type, Expected} = Test) ->
            {nm(Test),
             ?_assertEqual(
                Expected,
                iolist_to_binary(
                  apply(F,
                        [Parameters,
                         [{#{format => Format, type_oid => Type}, Value}]])))}
    end.


types() ->
    maps:from_list(phrase_file:consult("test/type.terms")).


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).
