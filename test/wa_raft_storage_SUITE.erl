% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_storage_SUITE).
-oncall("whatsapp_msgd").
-compile(warn_missing_spec_all).

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    basic_snapshot/1,
    config_snapshot/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 300}}] ++ wa_raft_test_helper:suite().

-spec init_per_suite(ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_suite(Config) ->
    wa_raft_test_helper:setup_environment(Config).

-spec end_per_suite(ct_suite:ct_config()) -> ok.
end_per_suite(Config) ->
    wa_raft_test_helper:teardown_environment(Config).

-spec init_per_group(atom(), ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_group(Group, Config) ->
    [{group, Group} | Config].

-spec end_per_group(atom(), ct_suite:ct_config()) -> ok.
end_per_group(_, _) ->
    ok.

-spec init_per_testcase(atom(), ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_testcase(Testcase, Config) ->
    Group = proplists:get_value(group, Config),
    PrivDir = proplists:get_value(priv_dir, Config),
    RootDir = filename:join([PrivDir, Group, Testcase]),
    ok = application:set_env(?RAFT_APPLICATION, raft_database, RootDir),
    [{testcase, Testcase} | Config].

-spec end_per_testcase(atom(), ct_suite:ct_config()) -> ok.
end_per_testcase(_, _) ->
    ok.

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, api}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {api, [
            basic_snapshot,
            config_snapshot
        ]}
    ].

%%--------------------------------------------------------------------
%% TEST CASES
%%

-spec get_raft_options(wa_raft:table(), wa_raft:partition()) -> #raft_options{}.
get_raft_options(Table, Partition) ->
    wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, #{
        table => Table,
        partition => Partition
    }).

-spec start(wa_raft:table(), wa_raft:partition()) -> ok.
start(Table, Partition) ->
    Options = get_raft_options(Table, Partition),
    {ok, _QueuePid} = wa_raft_queue:start_link(Options),
    {ok, _StoragePid} = wa_raft_storage:start_link(Options),
    ok.

%% Create and open snapshot
-spec basic_snapshot(ct_suite:ct_config()) -> ok.
basic_snapshot(Config) ->
    Table = proplists:get_value(testcase, Config),

    % Start storage for partition 1
    ok = start(Table, 1),
    Storage1 = wa_raft_storage:registered_name(Table, 1),

    % Advance storage by applying 100 log entries
    [
        ok = wa_raft_storage:apply(Storage1, undefined, {I, {2, {I, undefined, noop}}}, 1, high)
     || I <- lists:seq(1, 100)
    ],
    Status1 = wa_raft_storage:status(Storage1),
    ?assertEqual(100, proplists:get_value(last_applied, Status1)),

    % Create a snapshot
    Path1 = ?RAFT_SNAPSHOT_PATH(Table, 1, 100, 2),
    ?assertEqual({ok, #raft_log_pos{index = 100, term = 2}}, wa_raft_storage:create_snapshot(Storage1)),
    ?assert(filelib:is_dir(Path1)),

    % Start storage for partition 2
    ok = start(Table, 2),
    Storage2 = wa_raft_storage:registered_name(Table, 2),

    % Move the snapshot of partition 1 to the location expected by partition 2
    Path2 = ?RAFT_SNAPSHOT_PATH(Table, 2, 100, 2),
    ok = filelib:ensure_dir(Path2),
    ok = file:rename(Path1, Path2),

    % Attempt to open the snapshot
    ?assertEqual(ok, wa_raft_storage:open_snapshot(Storage2, Path2, #raft_log_pos{index = 100, term = 2})),
    Status2 = wa_raft_storage:status(Storage2),
    ?assertEqual(100, proplists:get_value(last_applied, Status2)),

    % Start storage for partition 3
    ok = start(Table, 3),
    Storage3 = wa_raft_storage:registered_name(Table, 3),

    % Make a new snapshot of partition 1 and move it to the location expected by partition 3
    % but with an incorrect position
    Path3 = ?RAFT_SNAPSHOT_PATH(Table, 3, 200, 2),
    ?assertEqual({ok, #raft_log_pos{index = 100, term = 2}}, wa_raft_storage:create_snapshot(Storage1)),
    ok = filelib:ensure_dir(Path3),
    ok = file:rename(Path1, Path3),

    % Attempt to open the snapshot at a wrong position should fail
    ?assertNotEqual(ok, wa_raft_storage:open_snapshot(Storage3, Path3, #raft_log_pos{index = 200, term = 2})),
    Status3 = wa_raft_storage:status(Storage3),
    ?assertNotEqual(100, proplists:get_value(last_applied, Status3)),

    ok.

%% Read and write config metadata and validate config metadata is preserved in snapshot
-spec config_snapshot(ct_suite:ct_config()) -> ok.
config_snapshot(Config) ->
    Table = proplists:get_value(testcase, Config),
    Node1 = #raft_identity{name = test, node = node1},
    Node2 = #raft_identity{name = test, node = node2},
    Node3 = #raft_identity{name = test, node = node3},

    % Start storage for partition 1
    ok = start(Table, 1),
    Storage1 = wa_raft_storage:registered_name(Table, 1),

    % Config should start undefined on an empty storage state
    ?assertEqual(undefined, wa_raft_storage:config(Storage1)),

    % Apply a log entry that changes the configuration
    Config0 = wa_raft_server:make_config([Node1]),
    ?assertEqual(
        ok, wa_raft_storage:apply(Storage1, undefined, {1, {1, {make_ref(), undefined, {config, Config0}}}}, 1, high)
    ),
    ?assertEqual({ok, #raft_log_pos{index = 1, term = 1}, Config0}, wa_raft_storage:config(Storage1)),

    % Apply another log entry that changes the configuration yet again
    Config1 = wa_raft_server:make_config([Node1, Node2]),
    ok = wa_raft_storage:apply(Storage1, undefined, {2, {1, {make_ref(), undefined, {config, Config1}}}}, 1, high),
    ?assertEqual({ok, #raft_log_pos{index = 2, term = 1}, Config1}, wa_raft_storage:config(Storage1)),

    % Applying other operations should not change the config
    ok = wa_raft_storage:apply(Storage1, undefined, {3, {1, {3, undefined, noop}}}, 1, high),
    ok = wa_raft_storage:apply(Storage1, undefined, {4, {1, {4, undefined, noop}}}, 1, high),
    ok = wa_raft_storage:apply(Storage1, undefined, {5, {2, {5, undefined, noop}}}, 1, high),
    ?assertEqual({ok, #raft_log_pos{index = 2, term = 1}, Config1}, wa_raft_storage:config(Storage1)),

    % Applying after other operations should still change the config normally
    Config2 = wa_raft_server:make_config([Node1, Node2, Node3]),
    ok = wa_raft_storage:apply(Storage1, undefined, {6, {2, {make_ref(), undefined, {config, Config2}}}}, 1, high),
    ?assertEqual({ok, #raft_log_pos{index = 6, term = 2}, Config2}, wa_raft_storage:config(Storage1)),

    % Create a snapshot with the latest config
    Path1 = ?RAFT_SNAPSHOT_PATH(Table, 1, 6, 2),
    ?assertEqual({ok, #raft_log_pos{index = 6, term = 2}}, wa_raft_storage:create_snapshot(Storage1)),
    ?assert(filelib:is_dir(Path1)),

    % Start storage for partition 2
    ok = start(Table, 2),
    Storage2 = wa_raft_storage:registered_name(Table, 2),

    % Move the snapshot of partition 1 to the location expected by partition 2
    Path2 = ?RAFT_SNAPSHOT_PATH(Table, 2, 6, 2),
    ok = filelib:ensure_dir(Path2),
    ok = file:rename(Path1, Path2),

    % Attempt to open the snapshot
    ?assertEqual(ok, wa_raft_storage:open_snapshot(Storage2, Path2, #raft_log_pos{index = 6, term = 2})),
    Status2 = wa_raft_storage:status(Storage2),
    ?assertEqual(6, proplists:get_value(last_applied, Status2)),

    % Check that the previous config is restored with the snapshot
    ?assertEqual({ok, #raft_log_pos{index = 6, term = 2}, Config2}, wa_raft_storage:config(Storage2)),

    ok.
