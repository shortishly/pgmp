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


-module(pgmp_rep_log_ets).


-behaviour(pgmp_rep_log).
-export([begin_transaction/1]).
-export([callback_mode/0]).
-export([commit/1]).
-export([delete/1]).
-export([handle_event/4]).
-export([init/1]).
-export([insert/1]).
-export([metadata/1]).
-export([snapshot/1]).
-export([start_link/1]).
-export([terminate/3]).
-export([truncate/1]).
-export([update/1]).
-export([when_ready/1]).
-import(pgmp_rep_log_ets_common, [delete/6]).
-import(pgmp_rep_log_ets_common, [insert_new/6]).
-import(pgmp_rep_log_ets_common, [metadata/4]).
-import(pgmp_rep_log_ets_common, [truncate/3]).
-import(pgmp_rep_log_ets_common, [update/6]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], envy_gen:options(?MODULE)).


snapshot(Arg) ->
    send_request(?FUNCTION_NAME, [id], Arg).


begin_transaction(Arg) ->
    send_request(?FUNCTION_NAME,
                 [commit_timestamp, final_lsn, xid, x_log],
                 Arg).


commit(Arg) ->
    send_request(?FUNCTION_NAME,
                 [commit_lsn, commit_timestamp, end_lsn, x_log],
                 Arg).


insert(Arg) ->
    send_request(?FUNCTION_NAME, [relation, tuple, x_log], Arg).


update(Arg) ->
    send_request(?FUNCTION_NAME, [relation, tuple, x_log], Arg).


delete(Arg) ->
    send_request(?FUNCTION_NAME, [relation, tuple, x_log], Arg).


truncate(Arg) ->
    send_request(?FUNCTION_NAME, [relations, x_log], Arg).


metadata(Arg) ->
    send_request(?FUNCTION_NAME, [], Arg).


send_request(Action, Keys, Arg) ->
    send_request(
      maps:without(
        Keys,
        Arg#{request => {Action, maps:with(Keys, Arg)}})).


when_ready(Arg) ->
    send_request(
      maps:merge(
        Arg,
        #{request => ?FUNCTION_NAME})).


send_request(#{label := _} = Arg) ->
    pgmp_statem:send_request(Arg);

send_request(Arg) ->
    pgmp_statem:send_request(Arg#{label => ?MODULE}).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok,
     unready,
     #{requests => gen_statem:reqids_new(),
       config => Arg,
       metadata => #{}},
     nei(join)}.


callback_mode() ->
    handle_event_function.


handle_event(internal,
             join,
             _,
             #{config := #{scope := Scope, publication := Publication}}) ->
    pg:join(Scope, [?MODULE, Publication], self()),
    keep_state_and_data;

handle_event({call, From}, {metadata, #{}}, ready, #{metadata := Metadata}) ->
    {keep_state_and_data, {reply, From, Metadata}};

handle_event({call, From}, when_ready, ready, _) ->
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, _}, when_ready, _, _) ->
    {keep_state_and_data, postpone};

handle_event({call, From},
             {begin_transaction, _},
             _,
             _) ->
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From},
             {commit, _},
             _,
             _) ->
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From},
             {insert,
              #{relation := #{namespace := Namespace, name := Name} = Relation,
                x_log := XLog,
                tuple := Tuple}},
             _,
             #{metadata := Metadata,
               config := #{scope := Scope,
                           publication := Publication}} = Data) ->
    case Metadata of
        #{{Namespace, Name} := #{keys := Positions}} ->
            insert_new(Scope, Publication, Namespace, Name, Tuple, Positions),
            {keep_state,
             Data#{metadata := metadata({Namespace, Name},
                                        x_log,
                                        XLog,
                                        Metadata)},
             {reply, From, ok}};

        _Otherwise ->
            {next_state,
             unready,
             Data,
             [{push_callback_module, pgmp_rep_log_ets_backfill},
              nei({relation, Relation}),
              postpone]}
    end;

handle_event({call, From},
             {update,
              #{relation := #{namespace := Namespace, name := Name} = Relation,
                x_log := XLog,
                tuple := Tuple}},
             _,
             #{metadata := Metadata,
               config := #{scope := Scope,
                           publication := Publication}} = Data) ->
    case Metadata of
        #{{Namespace, Name} := #{keys := Positions}} ->
            update(Scope, Publication, Namespace, Name, Tuple, Positions),
            {keep_state,
             Data#{metadata := metadata({Namespace, Name}, x_log, XLog, Metadata)},
             {reply, From, ok}};

        _Otherwise ->
            {keep_state_and_data,
             [{push_callback_module, pgmp_rep_log_ets_backfill},
              nei({relation, Relation}),
              postpone]}
    end;

handle_event({call, From},
             {delete,
              #{relation := #{namespace := Namespace, name := Name} = Relation,
                x_log := XLog,
                tuple := Tuple}},
              _,
             #{metadata := Metadata,
               config := #{scope := Scope,
                           publication := Publication}} = Data) ->
    case Metadata of
        #{{Namespace, Name} := #{keys := Keys}} ->
            delete(Scope, Publication, Namespace, Name, Tuple, Keys),
            {keep_state,
             Data#{metadata := metadata({Namespace, Name},
                                        x_log,
                                        XLog,
                                        Metadata)},
             {reply, From, ok}};

        _Otherwise ->
            {keep_state_and_data,
             [{push_callback_module, pgmp_rep_log_ets_backfill},
              nei({relation, Relation}),
              postpone]}
    end;

handle_event({call, From},
             {truncate, #{relations := Relations}},
             _,
             #{metadata := Metadata,
               config := #{publication := Publication}}) ->
    case lists:filter(
           fun
               (#{namespace := Namespace, name := Name}) ->
                   case maps:find({Namespace, Name}, Metadata) of
                       {ok, _} ->
                           false;

                       error ->
                           true
                   end
           end,
           Relations) of

        [] ->
            lists:foreach(
              fun
                  (#{namespace := Namespace, name := Name}) ->
                      truncate(Publication, Namespace, Name)
              end,
              Relations),
            {keep_state_and_data, {reply, From, ok}};


        [Relation | _] ->
            {keep_state_and_data,
             [{push_callback_module, pgmp_rep_log_ets_backfill},
              nei({relation, Relation}),
              postpone]}
    end;

handle_event({call, Stream}, {snapshot, #{id := Id}}, _, Data) ->
    {keep_state,
     Data#{stream => Stream},
     [{push_callback_module, pgmp_rep_log_ets_snapshot},
      nei(begin_transaction),
      nei({set_transaction_snapshot, Id}),
      nei(sync_publication_tables)]};

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, ServerRef}}, Label, UpdatedRequests} ->
                {stop,
                 #{reason => Reason,
                   server_ref => ServerRef,
                   label => Label},
                 Data#{requests := UpdatedRequests}}
    end.


terminate(_Reason,
          _State,
          #{config := #{scope := Scope, publication := Publication}}) ->
    pg:leave(Scope, [?MODULE, Publication], self()).
