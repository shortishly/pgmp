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


-module(pgmp_statem).


-export([nei/1]).
-export([send_request/1]).


send_request(#{server_ref := ServerRef,
               request := Request,
               label := Label,
               requests := Requests}) ->
    gen_statem:send_request(ServerRef, Request, Label, Requests);

send_request(#{requests := _} = Arg) ->
    error(badarg, [Arg]);

send_request(#{server_ref := ServerRef, request := Request}) ->
    gen_statem:send_request(ServerRef, Request).


nei(Event) ->
    {next_event, internal, Event}.
