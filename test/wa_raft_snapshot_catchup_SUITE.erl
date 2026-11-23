% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_snapshot_catchup_SUITE).
-compile(warn_missing_spec_all).

%% Test server callbacks
-export([
    suite/0,
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    precheck_overloaded_skips_snapshot/1,
    precheck_disk_full_skips_snapshot/1,
    precheck_sends_snapshot_size_estimate/1,
    precheck_unavailable_creates_snapshot/1,
    precheck_disabled_creates_snapshot/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-define(APP, test_app).
-define(TABLE, test_table).
-define(PARTITION, 1).
-define(PEER, 'follower@nohost').
-define(WITNESS_PEER, 'witness@nohost').
-define(PRECHECK_META(Witness, IncomingBytes), #{
    type => snapshot, table => ?TABLE, partition => ?PARTITION, witness => Witness, incoming_bytes => IncomingBytes
}).

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 30}}].

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [
        precheck_overloaded_skips_snapshot,
        precheck_disk_full_skips_snapshot,
        precheck_sends_snapshot_size_estimate,
        precheck_unavailable_creates_snapshot,
        precheck_disabled_creates_snapshot
    ].

-spec init_per_testcase(TestCase :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_testcase(_TestCase, Config) ->
    ok = meck:new(wa_raft_part_sup, [passthrough]),
    ok = meck:expect(wa_raft_part_sup, registered_partition_path, fun(_Table, _Partition) -> "/tmp/raft" end),
    ok = meck:new(wa_raft_storage, [passthrough]),
    ok = meck:expect(wa_raft_storage, create_snapshot, fun(_Storage) -> {ok, #raft_log_pos{index = 1, term = 1}} end),
    ok = meck:new(wa_raft_transport, [passthrough]),
    ok = meck:expect(wa_raft_transport, start_snapshot_transfer, fun(_, _, _, _, _, _, _) ->
        {error, receiver_overloaded}
    end),
    ok = meck:new(wa_raft_metrics, [passthrough]),
    {ok, _Pid} = wa_raft_snapshot_catchup:start_link(),
    Config.

-spec end_per_testcase(TestCase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_TestCase, _Config) ->
    application:unset_env(wa_raft, raft_snapshot_catchup_precheck),
    try
        ok = gen_server:stop(wa_raft_snapshot_catchup)
    after
        % Unloaded explicitly because the open-source build has no automatic mock cleanup.
        wa_raft_test_helper:unload_mocks([wa_raft_part_sup, wa_raft_storage, wa_raft_transport, wa_raft_metrics])
    end.

-spec precheck_overloaded_skips_snapshot(Config :: ct_suite:ct_config()) -> ok.
precheck_overloaded_skips_snapshot(_Config) ->
    ok = meck:expect(wa_raft_transport, may_accept, fun(_Peer, _Request, _Timeout) -> {error, receiver_overloaded} end),
    catchup(),
    ?assert(meck:called(wa_raft_metrics, count, [{raft, ?TABLE, 'snapshot_catchup.precheck.rejected'}])),
    ?assertNot(meck:called(wa_raft_storage, create_snapshot, '_')).

-spec precheck_disk_full_skips_snapshot(Config :: ct_suite:ct_config()) -> ok.
precheck_disk_full_skips_snapshot(_Config) ->
    ok = meck:expect(wa_raft_transport, may_accept, fun(_Peer, _Request, _Timeout) -> {error, receiver_disk_full} end),
    catchup(),
    ?assert(meck:called(wa_raft_metrics, count, [{raft, ?TABLE, 'snapshot_catchup.precheck.disk_full'}])),
    ?assertNot(meck:called(wa_raft_storage, create_snapshot, '_')).

-spec precheck_sends_snapshot_size_estimate(Config :: ct_suite:ct_config()) -> ok.
precheck_sends_snapshot_size_estimate(_Config) ->
    ok = meck:expect(wa_raft_storage, snapshot_size_estimate, fun(?TABLE, ?PARTITION) -> 4096 end),
    ok = meck:expect(wa_raft_transport, may_accept, fun(_Peer, _Request, _Timeout) -> {error, receiver_disk_full} end),
    catchup(),
    ?assert(meck:called(wa_raft_transport, may_accept, [?PEER, ?PRECHECK_META(false, 4096), '_'])),
    % Witness snapshots carry no storage data, so they are not estimated.
    catchup(witness_name, ?WITNESS_PEER, true),
    ?assert(meck:called(wa_raft_transport, may_accept, [?WITNESS_PEER, ?PRECHECK_META(true, 0), '_'])),
    ?assertEqual(1, meck:num_calls(wa_raft_storage, snapshot_size_estimate, '_')).

-spec precheck_unavailable_creates_snapshot(Config :: ct_suite:ct_config()) -> ok.
precheck_unavailable_creates_snapshot(_Config) ->
    ok = meck:expect(wa_raft_transport, may_accept, fun(_Peer, _Request, _Timeout) -> {error, unavailable} end),
    catchup(),
    ?assert(meck:called(wa_raft_metrics, count, [{raft, ?TABLE, 'snapshot_catchup.precheck.unavailable'}])),
    ?assert(meck:called(wa_raft_storage, create_snapshot, '_')).

-spec precheck_disabled_creates_snapshot(Config :: ct_suite:ct_config()) -> ok.
precheck_disabled_creates_snapshot(_Config) ->
    ok = application:set_env(wa_raft, raft_snapshot_catchup_precheck, false),
    ok = meck:expect(wa_raft_transport, may_accept, fun(_Peer, _Request, _Timeout) -> {error, receiver_overloaded} end),
    catchup(),
    ?assertNot(meck:called(wa_raft_transport, may_accept, '_')),
    ?assert(meck:called(wa_raft_storage, create_snapshot, '_')).

-spec catchup() -> ok.
catchup() ->
    catchup(test_name, ?PEER, false).

-spec catchup(Name :: atom(), Peer :: node(), Witness :: boolean()) -> ok.
catchup(Name, Peer, Witness) ->
    ok = wa_raft_snapshot_catchup:catchup(?APP, Name, Peer, ?TABLE, ?PARTITION, Witness),
    % A call is handled only after the catchup cast queued before it.
    _ = wa_raft_snapshot_catchup:which_transports(),
    ok.
