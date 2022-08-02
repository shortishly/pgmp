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


-type relation() :: binary().
-type request_id_collection() :: gen_statem:request_id_collection().
-type server_ref() :: gen_statem:server_ref().
-type snapshot_id() :: binary().

-type crud_collection_req() :: #{server_ref := server_ref(),
                                 label := any(),
                                 relation := relation(),
                                 tuple := tuple(),
                                 requests := request_id_collection()}.

-type snapshot_collection_req() :: #{server_ref := server_ref(),
                                     label := any(),
                                     id := snapshot_id(),
                                     requests := request_id_collection()}.

-type truncate_collection_req() :: #{server_ref := server_ref(),
                                     label := any(),
                                     relations := [relation()],
                                     requests := request_id_collection()}.


-callback delete(crud_collection_req()) -> request_id_collection().
-callback insert(crud_collection_req()) -> request_id_collection().
-callback snapshot(snapshot_collection_req()) -> request_id_collection().
-callback truncate(truncate_collection_req()) -> request_id_collection().
-callback update(crud_collection_req()) -> request_id_collection().
