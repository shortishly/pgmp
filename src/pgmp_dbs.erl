%% Copyright (c) 2023 Peter Morgan <peter.james.morgan@gmail.com>
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


-module(pgmp_dbs).


-export([all/0]).


-spec all() -> #{binary() => pgmp_dbs_sup:db()}.

all() ->
    lists:foldl(
      fun
          ({DB, Supervisor, supervisor, [pgmp_db_sup]}, A) ->
              A#{DB => pgmp_db:config(pgmp_sup:get_child_pid(Supervisor, db))}
      end,
      #{},
      supervisor:which_children(
        pgmp_sup:get_child_pid(pgmp_sup, dbs_sup))).
