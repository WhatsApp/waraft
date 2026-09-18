% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_test_helper).
-oncall("whatsapp_msgd").
-compile(warn_missing_spec_all).

-export([
    suite/0
]).

% Normal tests
-export([
    setup_environment/1,
    teardown_environment/1,
    setup_group/1,
    teardown_group/1
]).

-export([
    start_sentinel/2
]).

-export([
    unload_mocks/1
]).

-export([
    set_database_path/2,
    set_app_option/3,
    unset_app_option/2
]).

-export([
    get_server_status/1,
    get_server_status/2,
    get_storage_status/1,
    start_server/1,
    stop_server/1,
    wipe_server/1,
    call_server/2,
    write/3,
    read/2
]).

-include_lib("wa_raft/include/wa_raft.hrl").

-define(TABLE, test).
-define(PARTITION, 1).

-define(SENTINELS_KEY, wa_raft_test_helper_sentinels).
-define(APPLICATION_ENV_KEY, wa_raft_test_helper_application_env).
-define(GROUP_APPLICATION_ENV_KEY, wa_raft_test_helper_group_application_env).

-spec suite() -> [term()].
suite() ->
    [].

-spec setup_environment(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
setup_environment(Config) ->
    case application:load(?RAFT_APPLICATION) of
        ok -> ok;
        {error, {already_loaded, ?RAFT_APPLICATION}} -> ok
    end,
    ApplicationEnv = application:get_all_env(?RAFT_APPLICATION),
    ok = application:set_env(?RAFT_APPLICATION, raft_database, proplists:get_value(priv_dir, Config)),
    ok = wa_raft_sup:prepare_application(?RAFT_APPLICATION),
    [{?APPLICATION_ENV_KEY, ApplicationEnv} | Config].

-spec teardown_environment(Config :: ct_suite:ct_config()) -> ok.
teardown_environment(Config) ->
    stop_sentinels(Config),
    restore_application_env(proplists:get_value(?APPLICATION_ENV_KEY, Config, [])),
    ok.

-spec setup_group(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
setup_group(Config) ->
    [{?GROUP_APPLICATION_ENV_KEY, application:get_all_env(?RAFT_APPLICATION)} | Config].

-spec teardown_group(Config :: ct_suite:ct_config()) -> ok.
teardown_group(Config) ->
    stop_sentinels(Config),
    restore_application_env(proplists:get_value(?GROUP_APPLICATION_ENV_KEY, Config, [])),
    ok.

-spec stop_sentinels(Config :: ct_suite:ct_config()) -> ok.
stop_sentinels(Config) ->
    [stop_sentinel(Sentinel, Ref) || {?SENTINELS_KEY, {Sentinel, Ref}} <- Config],
    ok.

-spec restore_application_env([{atom(), term()}]) -> ok.
restore_application_env(ApplicationEnv) ->
    [application:unset_env(?RAFT_APPLICATION, Key) || {Key, _} <- application:get_all_env(?RAFT_APPLICATION)],
    [application:set_env(?RAFT_APPLICATION, Key, Value) || {Key, Value} <- ApplicationEnv],
    ok.

-spec unload_mocks([module()]) -> ok.
unload_mocks(Modules) ->
    Mocked = meck:mocked(),
    [meck:unload(Module) || Module <- Modules, lists:member(Module, Mocked)],
    ok.

-spec start_sentinel(Fun :: fun(() -> ok), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
start_sentinel(Fun, Config) ->
    Self = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> sentinel(Self, Ref, Fun) end),
    receive
        {Ref, ready} -> [{?SENTINELS_KEY, {Pid, Ref}} | Config];
        {Ref, {error, Class, Reason, Stack}} -> erlang:raise(Class, Reason, Stack)
    end.

-spec stop_sentinel(Sentinel :: pid(), Ref :: reference()) -> ok.
stop_sentinel(Sentinel, Ref) ->
    monitor(process, Sentinel),
    Sentinel ! {exit, self()},
    receive
        {Ref, exited} -> ok;
        {'DOWN', process, Sentinel, _} -> ok
    after 1000 -> ok
    end.

-spec sentinel(Parent :: pid(), Ref :: reference(), Fun :: fun(() -> ok)) -> ok.
sentinel(Parent, Ref, Fun) ->
    try
        Fun(),
        Parent ! {Ref, ready},
        receive
            {exit, From} ->
                From ! {Ref, exited},
                ok
        end
    catch
        C:R:S ->
            Parent ! {Ref, {error, C, R, S}},
            ok
    end.

-spec set_database_path(Node :: node(), Config :: ct_suite:ct_config()) -> ok.
set_database_path(Node, Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    Group = proplists:get_value(group, Config, nogroup),
    Testcase = proplists:get_value(testcase, Config, notestcase),
    Path = filename:join([PrivDir, Group, Testcase, atom_to_list(Node)]),
    set_app_option(Node, raft_database, Path).

-spec set_app_option(Node :: node(), Option :: atom(), Value :: term()) -> ok.
set_app_option(Node, Option, Value) ->
    erpc:call(Node, application, set_env, [?RAFT_APPLICATION, Option, Value]).

-spec unset_app_option(Node :: node(), Option :: atom()) -> ok.
unset_app_option(Node, Option) ->
    erpc:call(Node, application, unset_env, [?RAFT_APPLICATION, Option]).

-spec get_server_status(Node :: node()) -> wa_raft_server:status().
get_server_status(Node) ->
    Server = wa_raft_server:default_name(?TABLE, ?PARTITION),
    wa_raft_server:status({Server, Node}).

-spec get_server_status(Node :: node(), Key :: atom() | [atom()]) -> dynamic().
get_server_status(Node, Key) ->
    Server = wa_raft_server:default_name(?TABLE, ?PARTITION),
    wa_raft_server:status({Server, Node}, Key).

-spec get_storage_status(Node :: node()) -> wa_raft_storage:status().
get_storage_status(Node) ->
    Storage = wa_raft_storage:default_name(?TABLE, ?PARTITION),
    case wa_raft_storage:status({Storage, Node}) of
        Result when is_list(Result) -> Result
    end.

-spec start_server(Node :: node()) -> ok.
start_server(Node) ->
    {ok, _} = rpc:call(Node, supervisor, restart_child, [wa_raft_app_sup, wa_raft_sup]),
    ok.

-spec stop_server(Node :: node()) -> ok.
stop_server(Node) ->
    ok = rpc:call(Node, supervisor, terminate_child, [wa_raft_app_sup, wa_raft_sup]),
    ok.

-spec wipe_server(Node :: node()) -> ok.
wipe_server(Node) ->
    Path = erpc:call(Node, wa_raft_part_sup, registered_partition_path, [?TABLE, ?PARTITION]),
    case file:del_dir_r(Path) of
        ok -> ok;
        {error, enoent} -> ok
    end.

-spec call_server(Node :: node(), Request :: term()) -> dynamic().
call_server(Node, Request) ->
    Server = wa_raft_server:default_name(?TABLE, ?PARTITION),
    gen_statem:call({Server, Node}, Request).

-spec write(Node :: node(), Key :: non_neg_integer(), Value :: term()) -> wa_raft_acceptor:commit_result().
write(Node, Key, Value) ->
    Acceptor = wa_raft_acceptor:default_name(?TABLE, ?PARTITION),
    wa_raft_acceptor:commit({Acceptor, Node}, {make_ref(), {write, ?TABLE, Key, Value}}).

-spec read(Node :: node(), Key :: non_neg_integer()) -> ok.
read(Node, Key) ->
    Storage = wa_raft_storage:default_name(?TABLE, ?PARTITION),
    wa_raft_storage:read({Storage, Node}, {read, ?TABLE, Key}).
