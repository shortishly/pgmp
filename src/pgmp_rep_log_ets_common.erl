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


-export([delete/6]).
-export([insert_new/6]).
-export([insert_or_update_tuple/2]).
-export([metadata/4]).
-export([new_table/4]).
-export([table_name/3]).
-export([truncate/3]).
-export([update/6]).


new_table(Publication, Namespace, Name, Keys) ->
    ets:new(
      table_name(Publication, Namespace, Name),
      [{keypos, keypos(Keys)}, protected, named_table]).


keypos([Primary]) ->
    Primary;

keypos(L) when length(L) > 1 ->
    1.


metadata({_Namespace, _Name} = Relation, Key,  Value, Metadata) ->
    case Metadata of
        #{Relation := TMD} ->
            Metadata#{Relation := TMD#{Key => Value}};

        #{} ->
            Metadata#{Relation => #{Key => Value}}
    end.


insert_new(Scope,
           Publication,
           Schema,
           Table,
           Tuples,
           KeyPositions) when is_list(Tuples) ->
    true = ets:insert_new(
             table_name(Publication, Schema, Table),
             [insert_or_update_tuple(Tuple, KeyPositions) || Tuple <- Tuples]),
    notify(Scope,
           Publication,
           Schema,
           Table,
           ?FUNCTION_NAME,
           Tuples,
           KeyPositions),
    ok;

insert_new(Scope, Publication, Schema, Table, Tuple, Keys) when is_tuple(Tuple) ->
    ?FUNCTION_NAME(Scope, Publication, Schema, Table, [Tuple], Keys).


update(Scope, Publication, Schema, Table, Tuples, KeyPositions) when is_list(Tuples) ->
    ets:insert(
      table_name(Publication, Schema, Table),
      [insert_or_update_tuple(Tuple, KeyPositions) || Tuple <- Tuples]),
    notify(Scope, Publication, Schema, Table, ?FUNCTION_NAME, Tuples, KeyPositions),
    ok;

update(Scope, Publication, Schema, Table, Tuple, Keys) when is_tuple(Tuple) ->
    ?FUNCTION_NAME(Scope, Publication, Schema, Table, [Tuple], Keys).


delete(Scope, Publication, Schema, Table, Tuple, KeyPositions) ->
    ets:delete(
      table_name(Publication, Schema, Table),
      key(Tuple, KeyPositions)),
    notify(Scope,
           Publication,
           Schema,
           Table,
           ?FUNCTION_NAME,
           [Tuple],
           KeyPositions),
    ok.


notify(Scope, Publication, Namespace, Name, Action, Tuples, KeyPositions) ->
    notify_members(
       pg:get_members(
         Scope,
         #{m => pgmp_rep_log_ets,
           publication => Publication,
           name => Name}),
       Publication,
       Namespace,
       Name,
       Action,
       Tuples,
       KeyPositions).


notify_members([],
               _Publication,
               _Namespace,
               _Name,
               _Action,
               _Tuples,
               _KeyPositions) ->
    ok;

notify_members(Members,
               Publication,
               Namespace,
               Name,
               Action,
               Tuples,
               KeyPositions) ->
    ChangedKeys = key(Tuples, KeyPositions),
    Relation = table_name(Publication, Namespace, Name),

    lists:foreach(
      fun
          (Member) ->
              Member ! {notify,
                        #{publication => Publication,
                          namespace => Namespace,
                          name => Name,
                          relation => Relation,
                          keys => ChangedKeys,
                          action => Action}}
      end,
      Members).


key(Tuples, KeyPositions) when is_list(Tuples) ->
    [?FUNCTION_NAME(Tuple, KeyPositions) || Tuple <- Tuples];

key(Tuple, [Primary]) ->
    element(Primary, Tuple);

key(Tuple, Composite) when length(Composite) > 1 ->
    list_to_tuple([element(Position, Tuple) || Position <- Composite]).


truncate(Publication, Schema, Table) ->
    ets:delete_all_objects(
      table_name(Publication, Schema, Table)).


insert_or_update_tuple(Tuple, [_]) ->
    Tuple;

insert_or_update_tuple(Tuple, Composite) ->
    list_to_tuple(
      [key(Tuple, Composite) |
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
