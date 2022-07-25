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


-module(pgmp_pg).


-export([get_members/1]).
-export([join/1]).
-export([leave/1]).
-export([which_groups/0]).


leave(Group) ->
    pg:leave(pgmp_config:pg(scope), Group, self()).


join(Group) ->
    pg:join(pgmp_config:pg(scope), Group, self()).


get_members(Group) ->
    pg:get_members(pgmp_config:pg(scope), Group).


which_groups() ->
    pg:which_groups(pgmp_config:pg(scope)).
