% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_acceptor_SUITE).
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
    commit/1,
    commit_error_apply_queue_full/1,
    commit_error_commit_queue_full/1,
    strong_read/1,
    strong_read_apply_queue_full/1,
    strong_read_read_queue_full/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-define(TABLE, test).
-define(PARTITION_1, 1).

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 300}}] ++ wa_raft_test_helper:suite().

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
    Group = proplists:get_value(group, Config),
    PrivDir = proplists:get_value(priv_dir, Config),
    RootDir = filename:join([PrivDir, Group, Testcase]),
    ok = application:set_env(?RAFT_APPLICATION, raft_database, RootDir),
    Config.

-spec end_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_, _) ->
    wa_raft_test_helper:unload_mocks([wa_raft_server]).

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, api}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {api, [
            commit,
            commit_error_apply_queue_full,
            commit_error_commit_queue_full,
            strong_read,
            strong_read_apply_queue_full,
            strong_read_read_queue_full
        ]}
    ].

%%--------------------------------------------------------------------
%% TEST CASES

-spec get_raft_options() -> #raft_options{}.
get_raft_options() ->
    wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, #{
        table => ?TABLE,
        partition => ?PARTITION_1
    }).

-spec reset(Storage :: gen_server:server_ref(), Position :: #raft_log_pos{}, Config :: ct_suite:ct_config()) -> ok.
reset(Storage, Position, Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    Path = filename:join(PrivDir, "snapshot.tmp"),
    ok = wa_raft_storage:make_empty_snapshot(Storage, Path, Position, wa_raft_server:make_config(), #{}),
    ok = wa_raft_storage:open_snapshot(Storage, Path, Position).

%% Commit an op
-spec commit(Config :: ct_suite:ct_config()) -> ok.
commit(Config) ->
    meck:new(wa_raft_server, [passthrough]),

    Options = get_raft_options(),
    {ok, QueuePid} = wa_raft_queue:start_link(Options),
    {ok, StoragePid} = wa_raft_storage:start_link(Options),
    {ok, AcceptorPid} = wa_raft_acceptor:start_link(Options),
    ok = reset(StoragePid, #raft_log_pos{index = 100, term = 2}, Config),

    % Commit a successful write op at pos 101
    WriteOp = {write, ?TABLE, 1, 1},
    ok = meck:expect(
        wa_raft_server,
        commit,
        fun(_, From, {Ref, Command}, _) ->
            % apply after 10ms
            timer:apply_after(10, wa_raft_storage, apply, [
                StoragePid, From, {101, {2, {Ref, undefined, Command}}}, 1, high
            ]),
            ok
        end
    ),
    ?assertEqual(ok, wa_raft_acceptor:commit(AcceptorPid, {erlang:make_ref(), WriteOp})),
    ?assertEqual({ok, 1}, wa_raft_storage:read(?RAFT_STORAGE_NAME(?TABLE, ?PARTITION_1), {read, ?TABLE, 1})),

    ok = gen_server:stop(AcceptorPid, normal, 1000),
    ok = gen_server:stop(StoragePid, normal, 1000),
    ok = gen_server:stop(QueuePid, normal, 1000),
    ok.

-spec commit_error_apply_queue_full(Config :: ct_suite:ct_config()) -> ok.
commit_error_apply_queue_full(Config) ->
    meck:new(wa_raft_server, [passthrough]),

    Options = get_raft_options(),
    {ok, QueuePid} = wa_raft_queue:start_link(Options),
    {ok, StoragePid} = wa_raft_storage:start_link(Options),
    {ok, AcceptorPid} = wa_raft_acceptor:start_link(Options),
    ok = reset(StoragePid, #raft_log_pos{index = 100, term = 2}, Config),

    % Commit and get throttled.
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_applies, 0),
    WriteOp = {write, ?TABLE, 1, 1},
    Ref = erlang:make_ref(),
    ?assertEqual({error, apply_queue_full}, wa_raft_acceptor:commit(AcceptorPid, {Ref, WriteOp})),
    ok = application:unset_env(?RAFT_APPLICATION, raft_max_pending_applies),

    ok = gen_server:stop(AcceptorPid, normal, 1000),
    ok = gen_server:stop(StoragePid, normal, 1000),
    ok = gen_server:stop(QueuePid, normal, 1000),
    ok.

-spec commit_error_commit_queue_full(Config :: ct_suite:ct_config()) -> ok.
commit_error_commit_queue_full(Config) ->
    meck:new(wa_raft_server, [passthrough]),

    Options = get_raft_options(),
    {ok, QueuePid} = wa_raft_queue:start_link(Options),
    {ok, StoragePid} = wa_raft_storage:start_link(Options),
    {ok, AcceptorPid} = wa_raft_acceptor:start_link(Options),
    ok = reset(StoragePid, #raft_log_pos{index = 100, term = 2}, Config),

    % Commit and get throttled.
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_high_priority_commits, 0),
    WriteOp = {write, ?TABLE, 1, 1},
    Ref = erlang:make_ref(),
    ?assertEqual({error, commit_queue_full}, wa_raft_acceptor:commit(AcceptorPid, {Ref, WriteOp})),
    ok = application:unset_env(?RAFT_APPLICATION, raft_max_pending_high_priority_commits),

    ok = gen_server:stop(AcceptorPid, normal, 1000),
    ok = gen_server:stop(StoragePid, normal, 1000),
    ok = gen_server:stop(QueuePid, normal, 1000),
    ok.

%% Read an op
-spec strong_read(Config :: ct_suite:ct_config()) -> ok.
strong_read(Config) ->
    meck:new(wa_raft_server, [passthrough]),

    Options = get_raft_options(),
    {ok, QueuePid} = wa_raft_queue:start_link(Options),
    {ok, StoragePid} = wa_raft_storage:start_link(Options),
    {ok, AcceptorPid} = wa_raft_acceptor:start_link(Options),
    ok = reset(StoragePid, #raft_log_pos{index = 100, term = 2}, Config),

    % Start a write operation at pos 101
    WriteFrom = {self(), make_ref()},
    WriteRef = make_ref(),
    WriteCommand = {write, ?TABLE, 1, 42},
    ok = meck:expect(wa_raft_server, commit, fun(_, _, _, _) -> ok end),
    ok = wa_raft_acceptor:commit_async(AcceptorPid, WriteFrom, {WriteRef, WriteCommand}),

    % Trigger a fake commit to occur when the read request is accepted
    % so that the read is completed
    ok = meck:expect(
        wa_raft_server,
        read,
        fun(_, From, Command, _) ->
            % Add read to queue at 101
            Queues = wa_raft_queue:queues(?TABLE, ?PARTITION_1),
            ?assert(Queues =/= undefined),
            wa_raft_queue:submit_read(Queues, 101, From, Command),
            % Commit the write op at 101, which should execute the read pending at 101
            wa_raft_storage:apply(StoragePid, WriteFrom, {101, {2, {WriteRef, undefined, WriteCommand}}}, 1, high),
            ok
        end
    ),

    % Submit strong-read pending at 101
    ?assertEqual({ok, 42}, wa_raft_acceptor:read(AcceptorPid, {read, ?TABLE, 1})),

    ok = gen_server:stop(AcceptorPid, normal, 1000),
    ok = gen_server:stop(StoragePid, normal, 1000),
    ok = gen_server:stop(QueuePid, normal, 1000),
    ok.

-spec strong_read_apply_queue_full(Config :: ct_suite:ct_config()) -> ok.
strong_read_apply_queue_full(Config) ->
    meck:new(wa_raft_server, [passthrough]),

    Options = get_raft_options(),
    {ok, QueuePid} = wa_raft_queue:start_link(Options),
    {ok, StoragePid} = wa_raft_storage:start_link(Options),
    {ok, AcceptorPid} = wa_raft_acceptor:start_link(Options),
    ok = reset(StoragePid, #raft_log_pos{index = 100, term = 2}, Config),

    % Strong-read
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_applies, 0),
    ?assertEqual({error, apply_queue_full}, wa_raft_acceptor:read(AcceptorPid, {read, ?TABLE, 1})),
    ok = application:unset_env(?RAFT_APPLICATION, raft_max_pending_applies),

    ok = gen_server:stop(AcceptorPid, normal, 1000),
    ok = gen_server:stop(StoragePid, normal, 1000),
    ok = gen_server:stop(QueuePid, normal, 1000),
    ok.

-spec strong_read_read_queue_full(Config :: ct_suite:ct_config()) -> ok.
strong_read_read_queue_full(Config) ->
    meck:new(wa_raft_server, [passthrough]),

    Options = get_raft_options(),
    {ok, QueuePid} = wa_raft_queue:start_link(Options),
    {ok, StoragePid} = wa_raft_storage:start_link(Options),
    {ok, AcceptorPid} = wa_raft_acceptor:start_link(Options),
    ok = reset(StoragePid, #raft_log_pos{index = 100, term = 2}, Config),

    % Strong-read
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_reads, 0),
    ?assertEqual({error, read_queue_full}, wa_raft_acceptor:read(AcceptorPid, {read, ?TABLE, 1})),
    ok = application:unset_env(?RAFT_APPLICATION, raft_max_pending_reads),

    ok = gen_server:stop(AcceptorPid, normal, 1000),
    ok = gen_server:stop(StoragePid, normal, 1000),
    ok = gen_server:stop(QueuePid, normal, 1000),
    ok.
