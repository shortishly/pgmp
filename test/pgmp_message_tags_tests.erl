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


-module(pgmp_message_tags_tests).


-include_lib("eunit/include/eunit.hrl").


name_test_() ->
    {setup,
     fun pgmp_message_tags:start/0,
     fun
         ({ok, Pid}) when is_pid(Pid) ->
             gen_statem:stop(Pid)
     end,
    lists:map(
      t(fun pgmp_message_tags:name/2),
      [{authentication, {backend, <<$R>>}},
       {close, {frontend, <<$C>>}},
       {data_row, {backend, <<$D>>}},
       {execute, {frontend, <<$E>>}},
       {row_description, {backend, <<$T>>}},
       {sasl_initial_response, {frontend, <<$p>>}},
       {copy_data, {backend, <<$d>>}}])}.

t(F) ->
    fun
        ({Expected, {Role, Tag}} = Test) ->
            {nm(Test), ?_assertEqual(Expected, F(Role, Tag))}
    end.


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).
