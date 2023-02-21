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


-module(pgmp_rep_log_ets_common).


-export([insert_new/5]).
-export([insert_or_update_tuple/2]).
-export([metadata/4]).
-export([table_name/3]).
-export([update/5]).


metadata({_Namespace, _Name} = Relation, Key,  Value, Metadata) ->
    case Metadata of
        #{Relation := TMD} ->
            Metadata#{Relation := TMD#{Key => Value}};

        #{} ->
            Metadata#{Relation => #{Key => Value}}
    end.


insert_new(Publication, Schema, Table, Tuple, Keys) ->
    ets:insert_new(
      table_name(Publication, Schema, Table),
      insert_or_update_tuple(Tuple, Keys)).


update(Publication, Schema, Table, Tuple, Keys) ->
    ets:insert(
      table_name(Publication, Schema, Table),
      insert_or_update_tuple(Tuple, Keys)).


insert_or_update_tuple(Tuple, [_]) ->
    Tuple;

insert_or_update_tuple(Tuple, Composite) ->
    list_to_tuple(
      [list_to_tuple([element(Pos, Tuple) || Pos <- Composite]) |
       lists:filtermap(
         fun
             ({Position, Value}) ->
                 case lists:member(Position, Composite) of
                     true ->
                         false;

                     false ->
                         {true, Value}
                 end
         end,
         lists:zip(
           lists:seq(1, tuple_size(Tuple)),
           tuple_to_list(Tuple)))]).



table_name(Publication, Schema, Table) ->
    pgmp_util:snake_case(
      lists:filtermap(
        fun
            ([]) ->
                false;

            ([Value]) ->
                {true, Value};

            (_) ->
                true
        end,
        [pgmp_config:rep_log_ets(prefix_table_name),
         [binary_to_list(Publication) || pgmp_config:enabled(
                                           rep_log_ets_pub_in_table_name)],
         [binary_to_list(Schema) || pgmp_config:enabled(
                                      rep_log_ets_schema_in_table_name)],
         binary_to_list(Table)])).
