%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_storage_unit_SUITE).
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

%% Unit tests
-export([
    list_snapshots/1,
    durable_position/1,
    durable_position_fails_closed_without_partition/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 30}}] ++ wa_raft_test_helper:suite().

-spec init_per_suite(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_suite(Config) ->
    wa_raft_test_helper:setup_environment(Config).

-spec end_per_suite(Config :: ct_suite:ct_config()) -> ok.
end_per_suite(Config) ->
    wa_raft_test_helper:teardown_environment(Config).

-spec init_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_group(Group, Config) ->
    [{group, Group} | Config].

-spec end_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_group(_, _) ->
    ok.

-spec init_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_testcase(Testcase, Config) ->
    [{testcase, Testcase} | Config].

-spec end_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_, _) ->
    ok.

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, unit}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {unit, [
            list_snapshots,
            durable_position,
            durable_position_fails_closed_without_partition
        ]}
    ].

%%--------------------------------------------------------------------
%% UNIT TESTS
%%
%% List snapshot
-spec list_snapshots(Config :: ct_suite:ct_config()) -> ok.
list_snapshots(Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    Snapshot1 = ?SNAPSHOT_NAME(100, 1),
    Snapshot2 = ?SNAPSHOT_NAME(101, 2),
    Snapshot3 = ?SNAPSHOT_NAME(102, 3),
    [file:make_dir([PrivDir, Name]) || Name <- [Snapshot1, Snapshot2, Snapshot3]],

    Snapshots = wa_raft_storage:list_snapshots(PrivDir),
    Expected = [
        {#raft_log_pos{index = 100, term = 1}, Snapshot1},
        {#raft_log_pos{index = 101, term = 2}, Snapshot2},
        {#raft_log_pos{index = 102, term = 3}, Snapshot3}
    ],
    ?assertEqual(Expected, Snapshots),
    ok.

-spec durable_position(ct_suite:ct_config()) -> ok.
durable_position(Config) ->
    Table = proplists:get_value(testcase, Config),
    Partition = 1,
    Options = wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, #{
        table => Table,
        partition => Partition
    }),
    {ok, _QueuePid} = wa_raft_queue:start_link(Options),
    {ok, _StoragePid} = wa_raft_storage:start_link(Options),
    Storage = wa_raft_storage:registered_name(Table, Partition),
    Position = #raft_log_pos{index = 1, term = 1},

    ok = wa_raft_storage:apply(Storage, undefined, {1, {1, {make_ref(), undefined, noop}}}, 1, high),
    ?assertEqual(Position, wa_raft_storage:position(Storage)),
    ?assertEqual(unbounded, wa_raft_storage:durable_position(Table, Partition)).


-spec durable_position_fails_closed_without_partition(ct_suite:ct_config()) -> ok.
durable_position_fails_closed_without_partition(Config) ->
    Table = proplists:get_value(testcase, Config),
    Partition = 1,
    ?assertEqual(#raft_log_pos{}, wa_raft_storage:durable_position(Table, Partition)).
