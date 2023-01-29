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


-module(pgmp_rep_log).


-optional_callbacks([begin_transaction/1]).
-optional_callbacks([commit/1]).


-type relation() :: binary().
-type request_id_collection() :: gen_statem:request_id_collection().
-type server_ref() :: gen_statem:server_ref().
-type snapshot_id() :: binary().
-type x_log() :: #{clock := integer(),
                   start_wal := integer(),
                   end_wal := integer()}.

-type crud_collection_req() :: #{server_ref := server_ref(),
                                 label := any(),
                                 relation := relation(),
                                 tuple := tuple(),
                                 x_log := x_log(),
                                 requests := request_id_collection()}.

-type snapshot_collection_req() :: #{server_ref := server_ref(),
                                     label := any(),
                                     id := snapshot_id(),
                                     requests := request_id_collection()}.

-type truncate_collection_req() :: #{server_ref := server_ref(),
                                     label := any(),
                                     relations := [relation()],
                                     x_log := x_log(),
                                     requests := request_id_collection()}.

-type begin_transaction_req() :: #{server_ref := server_ref(),
                                   label := any(),
                                   commit_timestamp := integer(),
                                   final_lsn := integer(),
                                   xid := integer(),
                                   requests := request_id_collection()}.

-type commit_req() :: #{server_ref := server_ref(),
                        label := any(),
                        commit_lsn := integer(),
                        commit_timestamp := integer(),
                        end_lsn := integer(),
                        requests := request_id_collection()}.


-callback delete(crud_collection_req()) -> request_id_collection().
-callback insert(crud_collection_req()) -> request_id_collection().
-callback snapshot(snapshot_collection_req()) -> request_id_collection().
-callback truncate(truncate_collection_req()) -> request_id_collection().
-callback update(crud_collection_req()) -> request_id_collection().
-callback begin_transaction(begin_transaction_req()) -> request_id_collection().
-callback commit(commit_req()) -> request_id_collection().
