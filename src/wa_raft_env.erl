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
    database_path/1
]).

%% Internal API
-export([
    get_env/2,
    get_env/3
]).

-type scope() :: Application :: atom() | {Table :: wa_raft:table(), Partition :: wa_raft:partition()} | SearchApps :: [atom()].
-type key() :: Key :: atom() | {Primary :: atom(), Fallback :: atom()}.

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
