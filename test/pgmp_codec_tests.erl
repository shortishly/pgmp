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


-module(pgmp_codec_tests).


-include_lib("eunit/include/eunit.hrl").


demarshal_test_() ->
    lists:map(
      demarshal_t(fun pgmp_codec:demarshal/1),
      phrase_file:consult("test/codec-demarshal.terms")).

demarshal_t(F) ->
    fun
        ({Expected, Input} = Test) ->
            {nm(Test), ?_assertEqual(Expected, F(Input))}
    end.


marshal_test_() ->
    lists:map(
      marshal_t(fun pgmp_codec:marshal/2),
      phrase_file:consult("test/codec-marshal.terms")).


marshal_t(F) ->
    fun
        ({{Value, <<>>}, {Type, Expected}} = Test) ->
            <<_:8, _:32, Actual/bytes>> = iolist_to_binary(F(Type, Value)),
            {nm(Test), ?_assertEqual(Expected, Actual)}
    end.


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).
