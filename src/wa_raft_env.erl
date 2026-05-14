%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module contains utility functions for consulting application
%%% configuration in the OTP application environment according to the search
%%% order configured for each RAFT partition.

-module(wa_raft_env).
-compile(warn_missing_spec_all).

%% Config API
-export([
    database_path/1,
    election_weight/1,
    set_election_weight_override/2
]).

%% Internal API
-export([
    get_env/2,
    get_env/3,
    get_table_env/3,
    get_table_env/4
]).

-type scope() :: Application :: atom() | {Table :: wa_raft:table(), Partition :: wa_raft:partition()} | SearchApps :: [atom()].
-type key() :: Key :: atom() | {Primary :: atom(), Fallback :: atom()}.

-define(ELECTION_WEIGHT_OVERRIDE(App), {?MODULE, election_weight, App}).

-include_lib("wa_raft/include/wa_raft.hrl").

%%-------------------------------------------------------------------
%% Config API
%%-------------------------------------------------------------------

-spec database_path(Scope :: scope()) -> Root :: file:filename().
database_path(Scope) ->
    case get_env(Scope, ?RAFT_DATABASE) of
        {ok, Root} -> Root;
        undefined  -> error({no_configured_database_path, Scope})
    end.

-spec election_weight(Scope :: scope()) -> Weight :: non_neg_integer().
election_weight(Scope) ->
    Apps = search_apps(Scope),
    case persistent_term:get(?ELECTION_WEIGHT_OVERRIDE(hd(Apps)), undefined) of
        undefined ->
            case get_env_impl(Apps, key(?RAFT_ELECTION_WEIGHT), fallback(?RAFT_ELECTION_WEIGHT)) of
                undefined -> ?RAFT_ELECTION_DEFAULT_WEIGHT;
                {ok, Weight} -> Weight
            end;
        Weight    -> Weight
    end.

-spec set_election_weight_override(App :: atom(), Weight :: non_neg_integer()) -> ok.
set_election_weight_override(App, Weight) ->
    persistent_term:put(?ELECTION_WEIGHT_OVERRIDE(App), Weight).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

-spec get_env(Scope :: scope(), Key :: key()) -> {ok, Value :: dynamic()} | undefined.
get_env(Scope, Key) ->
    get_env_impl(search_apps(Scope), key(Key), fallback(Key)).

-spec get_env(Scope :: scope(), Key :: key(), Default :: Value) -> Value.
get_env(Scope, Key, Default) ->
    case get_env(Scope, Key) of
        {ok, Value} -> Value;
        undefined   -> Default
    end.

-spec get_env_impl(SearchApps :: [atom()], Key :: atom(), FallbackKey :: atom()) -> {ok, Value :: dynamic()} | undefined.
get_env_impl([], _Key, FallbackKey) ->
    ?RAFT_CONFIG(FallbackKey);
get_env_impl([App | SearchApps], Key, FallbackKey) ->
    case application:get_env(App, Key) of
        {ok, Value} -> {ok, Value};
        undefined   -> get_env_impl(SearchApps, Key, FallbackKey)
    end.

-doc """
Get a configuration value with table-level overrides.

Table-level configuration is supported by storing the value as a tagged tuple:
  `{table_overrides, #{Table => Value}, AppDefault}`
When this format is found, the table is looked up in the map first.
If the table is not in the map, AppDefault is used. If the value is a plain
(untagged) term, it is used directly as the app-level value for all tables.
""".
-spec get_table_env(Scope :: scope(), Table :: wa_raft:table(), Key :: key()) -> {ok, Value :: dynamic()} | undefined.
get_table_env(Scope, Table, Key) ->
    get_table_env_impl(search_apps(Scope), Table, key(Key), fallback(Key)).

-spec get_table_env(Scope :: scope(), Table :: wa_raft:table(), Key :: key(), Default :: Value) -> Value.
get_table_env(Scope, Table, Key, Default) ->
    case get_table_env(Scope, Table, Key) of
        {ok, Value} -> Value;
        undefined   -> Default
    end.

-spec get_table_env_impl(SearchApps :: [atom()], Table :: wa_raft:table(), Key :: atom(), FallbackKey :: atom()) ->
    {ok, Value :: dynamic()} | undefined.
get_table_env_impl([], _Table, _Key, FallbackKey) ->
    ?RAFT_CONFIG(FallbackKey);
get_table_env_impl([App | SearchApps], Table, Key, FallbackKey) ->
    case application:get_env(App, Key) of
        {ok, {table_overrides, TableOverrides, AppDefault}} ->
            {ok, maps:get(Table, TableOverrides, AppDefault)};
        {ok, Value} ->
            {ok, Value};
        undefined ->
            get_table_env_impl(SearchApps, Table, Key, FallbackKey)
    end.

-spec search_apps(Scope :: scope()) -> SearchApps :: [atom()].
search_apps(Application) when is_atom(Application) ->
    case wa_raft_sup:options(Application) of
        undefined       -> [];
        RaftApplication -> RaftApplication#raft_application.config_search_apps
    end;
search_apps({Table, Partition}) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> [];
        Options   -> search_apps(Options#raft_options.application)
    end;
search_apps(SearchApps) ->
    SearchApps.

-spec key(key()) -> atom().
key({Key, _}) -> Key;
key(Key) -> Key.

-spec fallback(key()) -> atom().
fallback({_, Fallback}) -> Fallback;
fallback(Key) -> Key.
