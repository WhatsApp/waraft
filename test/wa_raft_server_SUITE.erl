%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_server_SUITE).
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

%% Internal API
-export([
    server_all/0,
    server_groups/0
]).

%% Unit test cases
-export([
    compute_quorum/1,
    max_index_to_apply/1,
    persist_state/1,
    config/1,
    adjust_config/1
]).

%% Server unit test cases
-export([
    init/1,
    init_witness/1,
    init_empty/1,
    advance_term/1,
    advance_term_vote/1,
    advance_term_witness/1,
    stale_rpc/1,
    stale_rpc_witness/1,
    election/1,
    election_three_members/1,
    election_four_members/1,
    request_vote/1,
    request_vote_drop/1,
    append_entries/1,
    append_entries_witness/1,
    append_entries_witness_match_cap/1,
    append_entries_two_witness_full_member_cap/1,
    heartbeat/1,
    heartbeat_three_members/1,
    heartbeat_four_members/1,
    replication_index/1,
    commit/1,
    commit_label/1,
    commit_witness/1,
    commit_follower/1,
    commit_cancelled_truncate/1,
    snapshot_leader_witness/1,
    commit_two_witness/1,
    commit_one_witness/1,
    commit_three_members/1,
    commit_four_members/1,
    commit_candidate/1,
    commit_cancelled_candidate/1,
    read/1,
    read_after/1,
    read_candidate/1,
    read_after_candidate/1,
    read_lease_hit/1,
    read_lease_miss_disabled/1,
    read_lease_miss_stale/1,
    read_lease_miss_not_current_term/1,
    read_lease_miss_no_quorum_ts/1,
    read_lease_miss_not_applied/1,
    read_lease_miss_handover_in_progress/1,
    read_lease_handover_failed_rearms/1,
    read_lease_handover_timeout_rearms/1,
    truncate/1,
    update_info/1,
    update_info_witness/1,
    promote/1,
    promote_witness/1,
    resign/1,
    witness/1,
    disable/1,
    disable_leader/1,
    disable_follower/1,
    disable_candidate/1,
    disable_witness/1,
    handover/1,
    handover_follower/1,
    handover_witness/1,
    add_member/1,
    add_member_follower/1,
    remove_member/1,
    remove_member_follower/1,
    get_current_config/1,
    get_current_config_different_states/1,
    pre_vote_election/1,
    pre_vote_rejected/1,
    pre_vote_request/1
]).

%% Server sequential unit test cases
-export([
    commit_batch/1,
    node_weight/1,
    check_quorum/1,
    check_quorum_force_promote_multi_node/1,
    enter_follower_does_not_flap_liveness/1,
    promote_gate_considers_both_signals/1,
    stepped_down_leader_is_not_stale/1,
    leader_commit_advance_bumps_ts/1,
    participant_trim_index/1,
    trim_index/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_rpc.hrl").

-define(FROM(), {self(), make_ref()}).
-define(FROM(Ref), {self(), Ref}).

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 30}}] ++ wa_raft_test_helper:suite().

-spec init_per_suite(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_suite(Config) ->
    wa_raft_test_helper:setup_environment(Config).

-spec end_per_suite(Config :: ct_suite:ct_config()) -> ok.
end_per_suite(Config) ->
    wa_raft_test_helper:teardown_environment(Config),
    ok.

-spec init_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_group(Group, Config0) ->
    ok = application:set_env(?RAFT_APPLICATION, use_trim_index, true),
    Config1 = [{group, Group} | Config0],
    case Group of
        unit -> unit_init_per_group(Config1);
        server -> server_init_per_group(Config1);
        server_sequential -> server_init_per_group(Config1)
    end.

-spec end_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_group(_, _) ->
    ok.

-spec init_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_testcase(Testcase, Config0) ->
    Config1 = [{testcase, Testcase} | Config0],
    case proplists:get_value(group, Config1) of
        unit -> unit_init_per_testcase(Config1);
        server -> server_init_per_testcase(Config1);
        server_sequential -> server_init_per_testcase(Config1)
    end.

-spec end_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_, _) ->
    ok.

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    unit_all() ++ server_all().

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    unit_groups() ++ server_groups().

-spec unit_all() -> [{group, unit}].
unit_all() ->
    [{group, unit}].

-spec unit_groups() -> [{unit, [atom()]}].
unit_groups() ->
    [
        % Unit tests test specific functions with the RAFT server
        {unit, [
            compute_quorum, max_index_to_apply
        ]}
    ].

-spec server_all() -> [{group, server | server_sequential}].
server_all() ->
    [{group, server}, {group, server_sequential}].

-spec server_groups() -> [{server | server_sequential, [atom()]}].
server_groups() ->
    [
        % Server unit tests test RAFT server behavior when handling events
        {server, [
            init, init_witness, init_empty,
            advance_term, advance_term_vote, advance_term_witness,
            stale_rpc, stale_rpc_witness,
            election, election_three_members, election_four_members,
            request_vote, request_vote_drop,
            append_entries, append_entries_witness, append_entries_witness_match_cap,
            append_entries_two_witness_full_member_cap,
            heartbeat, heartbeat_three_members, heartbeat_four_members,
            replication_index,
            commit, commit_label, commit_witness, commit_follower,
            commit_cancelled_truncate,
            snapshot_leader_witness,
            commit_two_witness, commit_one_witness,
            commit_three_members, commit_four_members,
            commit_candidate, commit_cancelled_candidate,
            read, read_after, read_candidate, read_after_candidate,
            read_lease_hit, read_lease_miss_disabled, read_lease_miss_stale,
            read_lease_miss_not_current_term, read_lease_miss_no_quorum_ts,
            read_lease_miss_not_applied, read_lease_miss_handover_in_progress,
            read_lease_handover_failed_rearms, read_lease_handover_timeout_rearms,
            truncate,
            update_info, update_info_witness,
            promote, promote_witness,
            resign,
            witness,
            disable, disable_leader, disable_follower, disable_candidate, disable_witness,
            handover, handover_follower, handover_witness,
            add_member, add_member_follower,
            remove_member, remove_member_follower,
            persist_state, config, adjust_config,
            get_current_config, get_current_config_different_states,
            pre_vote_election, pre_vote_rejected, pre_vote_request
        ]},
        % Server unit tests that affect global state and should be run sequentially
        {server_sequential, [
            commit_batch,
            node_weight,
            check_quorum,
            check_quorum_force_promote_multi_node,
            enter_follower_does_not_flap_liveness,
            promote_gate_considers_both_signals,
            stepped_down_leader_is_not_stale,
            leader_commit_advance_bumps_ts,
            participant_trim_index,
            trim_index
        ]}
    ].

%%--------------------------------------------------------------------
%% UNIT TESTS
%%--------------------------------------------------------------------
%% Unit tests test the behavior of specific functions with the RAFT
%% server implementation.
%%--------------------------------------------------------------------

-spec unit_init_per_group(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
unit_init_per_group(Config) ->
    Config.

-spec unit_init_per_testcase(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
unit_init_per_testcase(Config) ->
    Config.

-spec compute_quorum(_Config :: ct_suite:ct_config()) -> term().
compute_quorum(_Config) ->
    % single node cluster
    C1 = wa_raft_server:make_config([
        #raft_identity{name = a, node = f1}
    ]),
    % When all members are present, quorum is the member value
    ?assertEqual({quorum, 100}, wa_raft_server:compute_member_quorum(#{f1 => 100}, C1)),
    ?assertEqual({quorum, 200}, wa_raft_server:compute_member_quorum(#{f1 => 200}, C1)),
    % When no members are present, quorum is none
    ?assertEqual(none, wa_raft_server:compute_member_quorum(#{}, C1)),
    % Non-members are ignored
    ?assertEqual(none, wa_raft_server:compute_member_quorum(#{f2 => 100}, C1)),
    ?assertEqual({quorum, 100}, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 200}, C1)),

    % three node cluster
    C3 = wa_raft_server:make_config([
        #raft_identity{name = a, node = f1},
        #raft_identity{name = a, node = f2},
        #raft_identity{name = a, node = f3}
    ]),
    % With all members present, quorum is the largest value a majority (2) have reached
    ?assertEqual({quorum, 150}, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 150, f3 => 200}, C3)),
    ?assertEqual({quorum, 100}, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 100, f3 => 200}, C3)),
    % With majority present (2 of 3), quorum is the largest value the majority have reached
    ?assertEqual({quorum, 100}, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 200}, C3)),
    ?assertEqual({quorum, 150}, wa_raft_server:compute_member_quorum(#{f2 => 150, f3 => 200}, C3)),
    % With minority present (1 of 3), quorum is none
    ?assertEqual(none, wa_raft_server:compute_member_quorum(#{f1 => 100}, C3)),
    ?assertEqual(none, wa_raft_server:compute_member_quorum(#{}, C3)),

    % five node cluster
    C5 = wa_raft_server:make_config([
        #raft_identity{name = a, node = f1},
        #raft_identity{name = a, node = f2},
        #raft_identity{name = a, node = f3},
        #raft_identity{name = a, node = f4},
        #raft_identity{name = a, node = f5}
    ]),
    % With all members, quorum is the largest value that a majority (3) have reached
    ?assertEqual({quorum, 200}, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 150, f3 => 200, f4 => 250, f5 => 300}, C5)),
    % With 3 of 5 present (majority), quorum is computed from those values
    ?assertEqual({quorum, 100}, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 150, f3 => 200}, C5)),
    % With 2 of 5 present (minority), quorum is none
    ?assertEqual(none, wa_raft_server:compute_member_quorum(#{f1 => 100, f2 => 200}, C5)).

-spec max_index_to_apply(_Config) -> ok.
max_index_to_apply(_Config) ->
    F =
        fun (MI, LI) ->
            % Create a config with the members: leader + each node in the map
            Ns = [node() | maps:keys(MI)],
            Ms = [#raft_identity{name = server, node = N} || N <- Ns],
            C = wa_raft_server:make_config(Ms),
            wa_raft_server:compute_member_quorum(MI#{node() => LI}, C)
        end,

    % single node cluster
    ?assertEqual({quorum, 100}, F(#{}, 100)),
    ?assertEqual({quorum, 150}, F(#{}, 150)),
    ?assertEqual({quorum, 200}, F(#{}, 200)),

    % all nodes caught up
    ?assertEqual({quorum, 50}, F(#{f1 => 50}, 50)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50}, 50)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50, f3 => 50}, 50)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50, f3 => 50, f4 => 50}, 50)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50, f3 => 50, f4 => 50, f5 => 50}, 50)),

    % quorum of nodes is caught up
    ?assertEqual({quorum, 50}, F(#{f1 => 50}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 41}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 41, f3 => 51}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 41, f3 => 51, f4 => 40}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 41, f3 => 51, f4 => 40, f5 => 50}, 60)),

    % quorum of nodes is not caught up
    ?assertEqual({quorum, 41}, F(#{f1 => 41}, 60)),
    ?assertEqual({quorum, 41}, F(#{f1 => 41, f2 => 40}, 60)),
    ?assertEqual({quorum, 41}, F(#{f1 => 41, f2 => 40, f3 => 50}, 60)),
    ?assertEqual({quorum, 43}, F(#{f1 => 41, f2 => 40, f3 => 50, f4 => 43}, 60)),
    ?assertEqual({quorum, 43}, F(#{f1 => 41, f2 => 40, f3 => 50, f4 => 43, f5 => 50}, 60)),

    % leader is ahead
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50, f3 => 50}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50, f3 => 50, f4 => 50}, 60)),
    ?assertEqual({quorum, 50}, F(#{f1 => 50, f2 => 50, f3 => 50, f4 => 50, f5 => 50}, 60)).

-spec persist_state(Config :: ct_suite:ct_config()) -> ok.
persist_state(Config) ->
    {_, RaftState} = server_start(Config),
    PrivDir = filename:join(proplists:get_value(priv_dir, Config), ?FUNCTION_NAME),
    FPath = fun (Table, Partition) ->
        wa_raft_part_sup:default_partition_path(PrivDir, Table, Partition)
    end,
    FState = fun (Table, Partition) ->
        RaftState#raft_state{partition_path = FPath(Table, Partition)}
    end,
    FCycle = fun (Table, Partition, CurrentTerm, VotedFor) ->
        State = FState(Table, Partition),
        StateFile = filename:join(FPath(Table, Partition), ?STATE_FILE_NAME),
        SaveState = State#raft_state{
            current_term = CurrentTerm,
            voted_for = VotedFor
        },
        wa_raft_durable_state:store(SaveState),
        ?assert(filelib:is_file(StateFile)),
        ?assertEqual({ok, SaveState}, wa_raft_durable_state:load(State)),
        ok
    end,

    ?assertEqual(no_state, wa_raft_durable_state:load(FState(test, 1))),
    ?assertEqual(no_state, wa_raft_durable_state:load(FState(test, 2))),

    StateFile = filename:join(FPath(fail, 1), ?STATE_FILE_NAME),
    ok = filelib:ensure_dir(StateFile),

    ok = prim_file:write_file(StateFile, "{no_crc, false}.\n"),
    ?assertEqual({error, no_crc}, wa_raft_durable_state:load(FState(fail, 1))),

    ok = prim_file:write_file(StateFile, "{crc, 0}.\n{invalid_crc, false}.\n"),
    ?assertEqual({error, invalid_crc}, wa_raft_durable_state:load(FState(fail, 1))),

    ok = prim_file:write_file(StateFile, "{crc, 2954778004}.\n{missing_current_term, false}.\n"),
    ?assertEqual({error, {missing, current_term}}, wa_raft_durable_state:load(FState(fail, 1))),

    ok = prim_file:write_file(StateFile, "{crc, 1303241358}.\n{current_term, invalid_term}.\n"),
    ?assertEqual({error, {invalid, current_term}}, wa_raft_durable_state:load(FState(fail, 1))),

    ok = prim_file:write_file(StateFile, "{crc, 1900801428}.\n{current_term, 1}.\n{missing_voted_for, false}.\n"),
    ?assertEqual({error, {missing, voted_for}}, wa_raft_durable_state:load(FState(fail, 1))),

    ok = prim_file:write_file(StateFile, "{crc, 718323598}.\n{current_term, 1}.\n{voted_for, \"invalid\"}.\n"),
    ?assertEqual({error, {invalid, voted_for}}, wa_raft_durable_state:load(FState(fail, 1))),

    ok = prim_file:write_file(StateFile, "{crc, 1422789693}.\n{current_term, 1}.\n{voted_for, undefined}.\n"),
    ?assertMatch({ok, #raft_state{current_term = 1, voted_for = undefined, disable_reason = undefined}}, wa_raft_durable_state:load(FState(fail, 1))),

    ?assertEqual(ok, FCycle(test, 1, 10, peer1)),
    ?assertEqual(ok, FCycle(test, 1, 12, peer1)),
    ?assertEqual(ok, FCycle(test, 1, 15, peer1)),
    ?assertEqual(ok, FCycle(test, 1, 20, peer1)),
    ?assertEqual(ok, FCycle(test, 1, 10, peer2)),
    ?assertEqual(ok, FCycle(test, 1, 10, peer3)),
    ?assertEqual(ok, FCycle(test, 1, 10, peer4)),

    ?assertEqual(ok, FCycle(test, 2, 10, peer1)),
    ?assertEqual(ok, FCycle(test, 3, 10, peer1)),
    ?assertEqual(ok, FCycle(test, 4, 10, peer1)).

-spec config(Config :: ct_suite:ct_config()) -> ok.
config(Config) ->
    Node = node(),
    WitnessNode = node2,

    ok = meck:new(wa_raft_log, [passthrough]),

    EmptyConfig0 = wa_raft_server:make_config(),
    CachedConfig0 = wa_raft_server:make_config([#raft_identity{name = config, node = Node}]),
    LogConfig0 = wa_raft_server:make_config([#raft_identity{name = log, node = Node}]),

    % No cached config + no config in log -> fallback config
    ok = meck:expect(wa_raft_log, config, fun(_) -> not_found end),
    {_, RaftState} = server_start(Config),
    ?assertEqual(EmptyConfig0, wa_raft_server:config(RaftState)),

    % Cached config + no config in log -> cached config
    ok = meck:expect(wa_raft_log, config, fun(_) -> not_found end),
    ?assertEqual(CachedConfig0, wa_raft_server:config(RaftState#raft_state{cached_config = {1, CachedConfig0}})),

    % Cached config + older config in log (should not actually ever occur) -> cached config
    ok = meck:expect(wa_raft_log, config, fun(_) -> {ok, 1, LogConfig0} end),
    ?assertEqual(CachedConfig0, wa_raft_server:config(RaftState#raft_state{cached_config = {2, CachedConfig0}})),

    % Cached config + newer config in log -> log config
    ok = meck:expect(wa_raft_log, config, fun(_) -> {ok, 2, LogConfig0} end),
    ?assertEqual(LogConfig0, wa_raft_server:config(RaftState#raft_state{cached_config = {1, CachedConfig0}})),

    CachedConfig1 = wa_raft_server:make_config([#raft_identity{name = config, node = Node}], [#raft_identity{name = config, node = WitnessNode}]),
    LogConfig1 = wa_raft_server:make_config([#raft_identity{name = log, node = Node}], [#raft_identity{name = config, node = WitnessNode}]),

    % No cached config + no config in log -> fallback config
    ok = meck:expect(wa_raft_log, config, fun(_) -> not_found end),
    ?assertEqual(EmptyConfig0, wa_raft_server:config(RaftState#raft_state{})),

    % Cached config + no config in log -> cached config
    ok = meck:expect(wa_raft_log, config, fun(_) -> not_found end),
    ?assertEqual(CachedConfig1, wa_raft_server:config(RaftState#raft_state{cached_config = {1, CachedConfig1}})),

    % Cached config + older config in log (should not actually ever occur) -> cached config
    ok = meck:expect(wa_raft_log, config, fun(_) -> {ok, 1, LogConfig1} end),
    ?assertEqual(CachedConfig1, wa_raft_server:config(RaftState#raft_state{cached_config = {2, CachedConfig1}})),

    % Cached config + newer config in log -> log config
    ok = meck:expect(wa_raft_log, config, fun(_) -> {ok, 2, LogConfig1} end),
    ?assertEqual(LogConfig1, wa_raft_server:config(RaftState#raft_state{cached_config = {1, CachedConfig1}})),

    ok.

-spec adjust_config(Config :: ct_suite:ct_config()) -> ok.
adjust_config(_) ->
    Node1 = #raft_identity{name = name, node = node1},
    Node2 = #raft_identity{name = name, node = node2},
    Node3 = #raft_identity{name = name, node = node3},
    Node4 = #raft_identity{name = name, node = node4},
    Node5 = #raft_identity{name = name, node = node5},

    Peer1 = {name, node1},
    Peer2 = {name, node2},
    Peer3 = {name, node3},
    Peer4 = {name, node4},
    Peer5 = {name, node5},

    ok = meck:new(wa_raft_log, [passthrough]),
    ok = meck:expect(wa_raft_log, config, fun(_) -> not_found end),
    ok = meck:expect(wa_raft_log, last_index, fun(_) -> 0 end),

    Config0 = wa_raft_server:make_config([Node4], [Node1, Node2, Node3], [Node3]),
    % eqwalizer:ignore - partially defined state for unit test
    State = #raft_state{
        name = name,
        self = Node1,
        cached_config = {0, Config0}
    },

    % Add a new member
    Config1 = wa_raft_server:make_config([Node4], [Node1, Node2, Node3, Node5], [Node3]),
    ?assertEqual({ok, Config1}, wa_raft_server:leader_adjust_config({add, Peer5}, State)),

    % Promote a participant to a member
    Config2 = wa_raft_server:make_config([Node1, Node2, Node3, Node4], [Node3]),
    ?assertEqual({ok, Config2}, wa_raft_server:leader_adjust_config({add, Peer4}, State)),

    % Try to add an existing member
    ?assertEqual({error, already_member}, wa_raft_server:leader_adjust_config({add, Peer2}, State)),

    % Try to add an existing witness member
    ?assertEqual({error, already_member}, wa_raft_server:leader_adjust_config({add, Peer3}, State)),

    % Try to add an existing non-member witness
    Config3 = wa_raft_server:make_config([Node1, Node2], [Node3]),
    ?assertEqual({error, already_witness}, wa_raft_server:leader_adjust_config({add, Peer3}, State#raft_state{cached_config = {0, Config3}})),

    % Add a new witness member
    Config4 = wa_raft_server:make_config([Node4], [Node1, Node2, Node3, Node5], [Node3, Node5]),
    ?assertEqual({ok, Config4}, wa_raft_server:leader_adjust_config({add_witness, Peer5}, State)),

    % Promote a witness participant to a witness member
    Config5 = wa_raft_server:make_config([Node3], [Node1, Node2], [Node3]),
    Config6 = wa_raft_server:make_config([Node1, Node2, Node3], [Node3]),
    ?assertEqual({ok, Config6}, wa_raft_server:leader_adjust_config({add_witness, Peer3}, State#raft_state{cached_config = {0, Config5}})),

    % Try to add an existing member as a witness
    ?assertEqual({error, already_member}, wa_raft_server:leader_adjust_config({add_witness, Peer2}, State)),

    % Try to add an existing witness
    ?assertEqual({error, already_witness}, wa_raft_server:leader_adjust_config({add_witness, Peer3}, State)),

    % Add a new participant
    Config7 = wa_raft_server:make_config([Node4, Node5], [Node1, Node2, Node3], [Node3]),
    ?assertEqual({ok, Config7}, wa_raft_server:leader_adjust_config({add_participant, Peer5}, State)),

    % Try to add an existing member
    ?assertEqual({error, already_member}, wa_raft_server:leader_adjust_config({add_participant, Peer3}, State)),

    % Try to add an existing non-member witness
    ?assertEqual({error, already_witness}, wa_raft_server:leader_adjust_config({add_participant, Peer3}, State#raft_state{cached_config = {0, Config3}})),

    % Try to add an existing participant
    ?assertEqual({error, already_participating}, wa_raft_server:leader_adjust_config({add_participant, Peer4}, State)),

    % Remove a participant
    Config8 = wa_raft_server:make_config([Node1, Node2, Node3], [Node3]),
    ?assertEqual({ok, Config8}, wa_raft_server:leader_adjust_config({remove, Peer4}, State)),

    % Remove a member
    Config9 = wa_raft_server:make_config([Node4], [Node1, Node3], [Node3]),
    ?assertEqual({ok, Config9}, wa_raft_server:leader_adjust_config({remove, Peer2}, State)),

    % Remove a witness member
    Config10 = wa_raft_server:make_config([Node4], [Node1, Node2], []),
    ?assertEqual({ok, Config10}, wa_raft_server:leader_adjust_config({remove, Peer3}, State)),

    % Try to remove self
    ?assertEqual({error, cannot_remove_self}, wa_raft_server:leader_adjust_config({remove, Peer1}, State)),

    % Try to remove a nonexistent participant
    ?assertEqual({error, not_a_participant}, wa_raft_server:leader_adjust_config({remove, Peer5}, State)),

    % Remove a witness
    ?assertEqual({ok, Config10}, wa_raft_server:leader_adjust_config({remove_witness, Peer3}, State)),

    % Try to remove self as witness
    ?assertEqual({error, cannot_remove_self}, wa_raft_server:leader_adjust_config({remove_witness, Peer1}, State)),

    % Try to remove a non-witness member
    ?assertEqual({error, not_a_witness}, wa_raft_server:leader_adjust_config({remove_witness, Peer2}, State)),

    % Try to remove a nonexistent peer
    ?assertEqual({error, not_a_witness}, wa_raft_server:leader_adjust_config({remove_witness, Peer5}, State)),

    % Remove a participant's membership
    Config11 = wa_raft_server:make_config([Node2, Node4], [Node1, Node3], [Node3]),
    ?assertEqual({ok, Config11}, wa_raft_server:leader_adjust_config({remove_membership, Peer2}, State)),

    % Remove a witness participant's membership
    Config12 = wa_raft_server:make_config([Node3, Node4], [Node1, Node2], [Node3]),
    ?assertEqual({ok, Config12}, wa_raft_server:leader_adjust_config({remove_membership, Peer3}, State)),

    % Try to remove one's own membership
    ?assertEqual({error, cannot_remove_self}, wa_raft_server:leader_adjust_config({remove_membership, Peer1}, State)),

    % Try to remove the membership of a participant with no membership
    ?assertEqual({error, not_a_member}, wa_raft_server:leader_adjust_config({remove_membership, Peer4}, State)),

    % Try to remove a nonexistent peer's membership
    ?assertEqual({error, not_a_member}, wa_raft_server:leader_adjust_config({remove_membership, Peer5}, State)),

    % Demote a member to a witness
    Config13 = wa_raft_server:make_config([Node4], [Node1, Node2, Node3], [Node2, Node3]),
    ?assertEqual({ok, Config13}, wa_raft_server:leader_adjust_config({demote_to_witness, Peer2}, State)),

    % Demote a participant to a witness
    Config14 = wa_raft_server:make_config([Node4], [Node1, Node2, Node3], [Node3, Node4]),
    ?assertEqual({ok, Config14}, wa_raft_server:leader_adjust_config({demote_to_witness, Peer4}, State)),

    % Try to demote self
    ?assertEqual({error, cannot_demote_self}, wa_raft_server:leader_adjust_config({demote_to_witness, Peer1}, State)),

    % Try to demote an existing witness
    ?assertEqual({error, already_witness}, wa_raft_server:leader_adjust_config({demote_to_witness, Peer3}, State)),

    % Try to demote a nonexistent peer
    ?assertEqual({error, not_a_participant}, wa_raft_server:leader_adjust_config({demote_to_witness, Peer5}, State)),

    % add_witness_participant: add a new peer as a non-voting witness
    % participant (in participants and witness, not in membership).
    AddWitnessParticipantConfig = wa_raft_server:make_config([Node4, Node5], [Node1, Node2, Node3], [Node3, Node5]),
    ?assertEqual({ok, AddWitnessParticipantConfig}, wa_raft_server:leader_adjust_config({add_witness_participant, Peer5}, State)),

    % add_witness_participant: adding an existing member fails.
    ?assertEqual({error, already_member}, wa_raft_server:leader_adjust_config({add_witness_participant, Peer2}, State)),

    % add_witness_participant: adding an existing witness member fails
    % with `already_member` (member check comes first).
    ?assertEqual({error, already_member}, wa_raft_server:leader_adjust_config({add_witness_participant, Peer3}, State)),

    % add_witness_participant: adding an existing non-voting participant
    % fails.
    ?assertEqual({error, already_participating}, wa_raft_server:leader_adjust_config({add_witness_participant, Peer4}, State)),

    % promote_participant_if_ready: promoting an existing non-voting
    % witness participant produces a voting witness member (peer in
    % participants + witness + membership). config_add_member preserves
    % the peer's presence in the witness list, so no separate primitive
    % is needed.
    WitnessParticipantConfig = wa_raft_server:make_config([Node4, Node5], [Node1, Node2, Node3], [Node3, Node5]),
    PromotedWitnessConfig = wa_raft_server:make_config([Node4, Node5], [Node1, Node2, Node3, Node5], [Node3, Node5]),
    ReadyWitnessState = State#raft_state{
        cached_config = {0, WitnessParticipantConfig},
        match_indices = #{node5 => 0},
        last_applied_indices = #{node5 => 0}
    },
    ?assertEqual(
        {ok, PromotedWitnessConfig},
        wa_raft_server:leader_adjust_config({promote_participant_if_ready, Peer5}, ReadyWitnessState)
    ),

    % promote_participant_if_ready: promoting a plain non-voting
    % participant produces a voting full member.
    PlainParticipantConfig = wa_raft_server:make_config([Node4, Node5], [Node1, Node2, Node3], [Node3]),
    PromotedFullConfig = wa_raft_server:make_config([Node4, Node5], [Node1, Node2, Node3, Node5], [Node3]),
    ReadyParticipantState = State#raft_state{
        cached_config = {0, PlainParticipantConfig},
        match_indices = #{node5 => 0},
        last_applied_indices = #{node5 => 0}
    },
    ?assertEqual(
        {ok, PromotedFullConfig},
        wa_raft_server:leader_adjust_config({promote_participant_if_ready, Peer5}, ReadyParticipantState)
    ),

    % promote_participant_if_ready: peer with unknown indices is not ready.
    NotReadyState = State#raft_state{cached_config = {0, WitnessParticipantConfig}},
    ?assertEqual(
        {error, not_ready},
        wa_raft_server:leader_adjust_config({promote_participant_if_ready, Peer5}, NotReadyState)
    ),

    % promote_participant_if_ready: peer that is already a voting member fails.
    ?assertEqual(
        {error, already_member},
        wa_raft_server:leader_adjust_config({promote_participant_if_ready, Peer2}, State)
    ),

    % promote_participant_if_ready: peer that is already a voting witness
    % member fails with `already_member` (member check comes first).
    ?assertEqual(
        {error, already_member},
        wa_raft_server:leader_adjust_config({promote_participant_if_ready, Peer3}, State)
    ),

    % promote_participant_if_ready: peer that is not in the config fails
    % with `not_a_participant`.
    ?assertEqual(
        {error, not_a_participant},
        wa_raft_server:leader_adjust_config({promote_participant_if_ready, Peer5}, State)
    ),

    ok.

%%--------------------------------------------------------------------
%% SERVER TESTS
%%--------------------------------------------------------------------
%% In order to make it as easy as possible to control the order in
%% which events are raised in the RAFT server, server tests involve
%% creating a mock for the RAFT server that allows arbitrary events
%% to be raised and prevents timeouts from being set by RAFT server
%% event callbacks. Additionally, certain actions performed by the
%% RAFT server are mocked to notify the test host so that test cases
%% can assert RPC sends and other events without relying on specific
%% side effects or testcase-specific mocks. This also enables us to
%% run all the server tests in parallel.
%%--------------------------------------------------------------------

%% Lookup the testcase host that is responsible for a particular
%% RAFT table and partition or storage process. The RAFT table and
%% partition can uniquely identify a particular test case.
-spec host() -> pid().
host() ->
    {Table, Partition} = get(?MODULE),
    host(Table, Partition).

-spec host(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> pid().
host(Table, Partition) ->
    ets:lookup_element(?MODULE, {Table, Partition}, 2).

%% Send a notify event to the specified host. Notify events are used
%% to asynchronously notify the testcase host of actions performed by
%% the RAFT server as part of handling an event that cannot be inferred
%% from the RAFT server state.
-spec server_notify(Host :: pid(), Type :: atom(), Event :: term()) -> term().
server_notify(Host, Type, Event) ->
    Host ! {'$notify', Type, Event}.

%% A mock for the RAFT server gen_statem state callback handlers that:
%%  1. allows for arbitrary events to be created through a special
%%     {'$override', Type, Event} event handler, and
%%  2. prevents any timeout actions from reaching the gen_statem so
%%     that in conjunction with 1 allows tests to precisely control
%%     event ordering
-spec server_callback_mock(RawType :: atom(), Event :: term(), Data :: term()) -> term().
server_callback_mock(_RawType, {'$override', Type, Event}, Data) ->
    server_filter_return(meck:passthrough([Type, Event, Data]));
server_callback_mock(Type, Event, Data) ->
    server_filter_return(meck:passthrough([Type, Event, Data])).

%% Detect if the return from the state callback handler has an action
%% list and filter out any timeout actions if found.
-spec server_filter_return(Return :: term()) -> term().
server_filter_return({next_state, State, Data, Actions}) ->
    {next_state, State, Data, server_filter_actions(Actions)};
server_filter_return({keep_state, Data, Actions}) ->
    {keep_state, Data, server_filter_actions(Actions)};
server_filter_return({keep_state_and_data, Actions}) ->
    {keep_state_and_data, server_filter_actions(Actions)};
server_filter_return({repeat_state, Data, Actions}) ->
    {repeat_state, Data, server_filter_actions(Actions)};
server_filter_return({repeat_state_and_data, Actions}) ->
    {repeat_state_and_data, server_filter_actions(Actions)};
server_filter_return(Other) ->
    Other.

%% Filter out any timeout actions from the provided timeout list
%% and notify the testcase host about any filtered timeouts.
-spec server_filter_actions(Actions :: term()) -> [term()].
server_filter_actions(Actions) when is_list(Actions) ->
    {FilteredActions, TimeoutActions} =
        lists:partition(
            fun (infinity) -> false;
                (Timeout) when is_integer(Timeout) -> false;
                ({timeout, _})                     -> false;
                ({timeout, _, _})                  -> false;
                ({timeout, _, _, _})               -> false;
                ({{timeout, _}, _, _})             -> false;
                ({{timeout, _}, _, _, _})          -> false;
                ({state_timeout, _})               -> false;
                ({state_timeout, _, _})            -> false;
                ({state_timeout, _, _, _})         -> false;
                (_Other)                           -> true
            end, Actions),
    Host = host(),
    [server_notify(Host, set_timeout, Timeout) || Timeout <- TimeoutActions],
    FilteredActions;
server_filter_actions(Action) ->
    server_filter_actions([Action]).

-spec server_init_per_group(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
server_init_per_group(Config) ->
    StorageModule = proplists:get_value(storage_module, Config, wa_raft_storage_ets),
    ok = application:set_env(?RAFT_APPLICATION, raft_database, proplists:get_value(priv_dir, Config)),
    ok = application:set_env(?RAFT_APPLICATION, raft_distribution_module, wa_raft_test_distribution),
    ok = application:set_env(?RAFT_APPLICATION, raft_storage_module, StorageModule),

    ok = meck:new(wa_raft_server, [passthrough, no_link]),
    ok = meck:expect(wa_raft_server, init,
        fun (#raft_options{table = Table, partition = Partition} = Options) ->
            put(?MODULE, {Table, Partition}),
            meck:passthrough([Options])
        end),
    ok = meck:expect(wa_raft_server, stalled, fun server_callback_mock/3),
    ok = meck:expect(wa_raft_server, leader, fun server_callback_mock/3),
    ok = meck:expect(wa_raft_server, follower, fun server_callback_mock/3),
    ok = meck:expect(wa_raft_server, candidate, fun server_callback_mock/3),
    ok = meck:expect(wa_raft_server, disabled, fun server_callback_mock/3),

    ok = meck:new(wa_raft_test_distribution, [non_strict, no_link]),
    ok = meck:expect(wa_raft_test_distribution, cast,
        fun ({Name, Node}, _Identifier, Message) ->
            server_notify(host(), cast, {Name, Node, Message}),
            ok
        end),
    ok = meck:new(wa_raft_test_label, [non_strict, no_link]),
    ok = meck:expect(wa_raft_test_label, new_label,
        fun (undefined, _Command)    -> 1;
            (LastLogLabel, _Command) -> LastLogLabel + 1
        end),

    ok = meck:new(wa_raft_storage, [passthrough, no_link]),
    ok = meck:expect(wa_raft_storage, init,
        fun (#raft_options{table = Table, partition = Partition} = Options) ->
            put(?MODULE, {Table, Partition}),
            meck:passthrough([Options])
        end),
    ok = meck:expect(wa_raft_storage, apply,
        fun (Storage, From, Record, Size, Priority) ->
            server_notify(host(), applied, {From, Record}),
            Result = meck:passthrough([Storage, From, Record, Size, Priority]),
            % Force synchronization against the storage server to avoid timing races.
            sys:get_state(Storage),
            Result
        end),

    wa_raft_test_helper:start_sentinel(
        fun () ->
            wa_raft_info:init_tables(),
            wa_raft_snapshot_catchup:init_tables(),

            % Create an ETS table that is used by RAFT server mocks to look up
            % which testcase host process to send notify information to.
            ?MODULE = ets:new(?MODULE, [set, public, named_table]),
            ok
        end, Config
    ).

-spec server_init_per_testcase(Config :: ct_suite:ct_config()) -> Config :: ct_suite:ct_config().
server_init_per_testcase(Config) ->
    % Collect info
    Group = proplists:get_value(group, Config),
    Testcase = proplists:get_value(testcase, Config),
    Table = list_to_atom(atom_to_list(Group) ++ "+" ++ atom_to_list(Testcase)),
    Partition = 1,
    LogModule = proplists:get_value(log_module, Config, wa_raft_log_ets),
    StorageModule = proplists:get_value(storage_module, Config, wa_raft_storage_ets),

    % Register this test host in the ETS
    true = ets:insert(?MODULE, {{Table, Partition}, self()}),

    % Generate options to pass to RAFT processes
    Options = #{
        table => Table,
        partition => Partition,
        log_module => LogModule,
        storage_module => StorageModule
    },

    % Preload RAFT options into cache, actual spec used may be different
    wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, Options),

    [{table, Table}, {partition, Partition}, {options, Options} | Config].


-define(SERVER_PID, server_pid).
-define(SERVER_NAME, server_name).

-define(SERVER_OPTIONS(Config), proplists:get_value(options, Config)).
-define(SERVER_TABLE(Config), proplists:get_value(table, Config)).
-define(SERVER_PARTITION(Config), proplists:get_value(partition, Config)).
-define(SERVER_NAME(Config), wa_raft_server:registered_name(?SERVER_TABLE(Config), ?SERVER_PARTITION(Config))).
-define(SERVER_QUEUES(Config), wa_raft_queue:queues(?SERVER_TABLE(Config), ?SERVER_PARTITION(Config))).

-define(SERVER_SINGLE_CONFIG(Config),
    wa_raft_server:make_config([#raft_identity{name = ?SERVER_NAME(Config), node = node()}])).
-define(SERVER_CLUSTER_CONFIG(Config),
    wa_raft_server:make_config([
        #raft_identity{name = ?SERVER_NAME(Config), node = node()},
        #raft_identity{name = ?SERVER_NAME(Config), node = node2},
        #raft_identity{name = ?SERVER_NAME(Config), node = node3},
        #raft_identity{name = ?SERVER_NAME(Config), node = node4},
        #raft_identity{name = ?SERVER_NAME(Config), node = node5}
    ])).

-define(SERVER_CLUSTER_CONFIG_WITH_WITNESS(Config),
    wa_raft_server:make_config([
        #raft_identity{name = ?SERVER_NAME(Config), node = node()},
        #raft_identity{name = ?SERVER_NAME(Config), node = node2},
        #raft_identity{name = ?SERVER_NAME(Config), node = node3},
        #raft_identity{name = ?SERVER_NAME(Config), node = node4},
        #raft_identity{name = ?SERVER_NAME(Config), node = node5}
    ],
    [
        #raft_identity{name = ?SERVER_NAME(Config), node = node4},
        #raft_identity{name = ?SERVER_NAME(Config), node = node5}
    ])).

-define(SERVER_CLUSTER_CONFIG_WITH_SELF_WITNESS(Config),
    wa_raft_server:make_config([
        #raft_identity{name = ?SERVER_NAME(Config), node = node()},
        #raft_identity{name = ?SERVER_NAME(Config), node = node2},
        #raft_identity{name = ?SERVER_NAME(Config), node = node3},
        #raft_identity{name = ?SERVER_NAME(Config), node = node4},
        #raft_identity{name = ?SERVER_NAME(Config), node = node5}
    ],
    [
        #raft_identity{name = ?SERVER_NAME(Config), node = node()},
        #raft_identity{name = ?SERVER_NAME(Config), node = node5}
      ])).


%% Trigger an arbitrary event on the RAFT server and return the
%% server state afterwards.
-spec server_invoke(Type :: atom(), Event :: term()) -> {atom(), #raft_state{}}.
server_invoke(Type, Event) ->
    server_cast({'$override', Type, Event}).

%% Send a cast to the RAFT server and return the server state afterwards.
-spec server_cast(Event :: term()) -> {atom(), #raft_state{}}.
server_cast(Event) ->
    Pid = get(?SERVER_PID),
    gen_statem:cast(Pid, Event),
    sys:get_state(Pid).

%% Send a call to the RAFT server and return the reply and the server
%% state afterwards.
-spec server_call(Event :: term()) -> {atom(), #raft_state{}, term()}.
server_call(Event) ->
    server_call(Event, 5000).
-spec server_call(Event :: term(), Timeout :: timeout()) -> {atom(), #raft_state{}, term()}.
server_call(Event, Timeout) ->
    Pid = get(?SERVER_PID),
    Reply = gen_statem:call(Pid, Event, Timeout),
    {State, Data} = sys:get_state(Pid),
    {State, Data, Reply}.

-spec server_start(Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start(Config) ->
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),

    % Ensure that no other RAFT server is already started.
    get(?SERVER_PID) =/= undefined andalso error(already_started),

    % Insert the requested node configuration into the RAFT args
    Options0 = ?SERVER_OPTIONS(Config),
    Options = wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, Options0),
    % Start all dependent components in the correct order.
    {ok, _Queue} = wa_raft_queue:start_link(Options),
    {ok, _Storage} = wa_raft_storage:start_link(Options),
    {ok, _Log} = wa_raft_log:start_link(Options),

    % Now start the RAFT server
    {ok, Server} = wa_raft_server:start_link(Options),

    % Store the current RAFT server info
    put(?SERVER_PID, Server),
    put(?SERVER_NAME, ?SERVER_NAME(Config)),

    % Store registered names
    true = ets:insert(?MODULE, {wa_raft_server:registered_name(Table, Partition), self()}),
    true = ets:insert(?MODULE, {wa_raft_storage:registered_name(Table, Partition), self()}),

    % Return the current state.
    sys:get_state(Server).

-spec server_start_and_bootstrap(Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start_and_bootstrap(Config) ->
    server_start_and_bootstrap(?SERVER_CLUSTER_CONFIG(Config), Config).
-spec server_start_and_bootstrap(ClusterConfig :: wa_raft_server:config(), Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start_and_bootstrap(ClusterConfig, Config) ->
    server_start_and_bootstrap(0, 0, ClusterConfig, Config).
-spec server_start_and_bootstrap(Index :: wa_raft_log:log_index(), Term :: wa_raft_log:log_term(), Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start_and_bootstrap(Index, Term, Config) ->
    server_start_and_bootstrap(Index, Term, ?SERVER_CLUSTER_CONFIG(Config), Config).
-spec server_start_and_bootstrap(Index :: wa_raft_log:log_index(), Term :: wa_raft_log:log_term(), ClusterConfig :: wa_raft_server:config(), Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start_and_bootstrap(Index, Term, ClusterConfig, Config) ->
    {stalled, _} = server_start(Config),
    {State, Data, ok} = server_call(?BOOTSTRAP_COMMAND(#raft_log_pos{index = Index, term = Term}, ClusterConfig, #{})),
    clear_message_queue(),
    {State, Data}.

-spec server_start_witness_and_bootstrap(Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start_witness_and_bootstrap(Config) ->
    server_start_witness_and_bootstrap(0, 0, Config).
-spec server_start_witness_and_bootstrap(Index :: wa_raft_log:log_index(), Term :: wa_raft_log:log_term(), Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_start_witness_and_bootstrap(Index, Term, Config) ->
    {stalled, _} = server_start(Config),
    {witness, Data, ok} = server_call(?BOOTSTRAP_COMMAND(#raft_log_pos{index = Index, term = Term}, ?SERVER_CLUSTER_CONFIG_WITH_SELF_WITNESS(Config), #{})),
    clear_message_queue(),
    {witness, Data}.

%% Start just the RAFT server
-spec server_restart(Config :: ct_suite:ct_config()) -> {atom(), #raft_state{}}.
server_restart(Config) ->
    % Ensure that no other RAFT server is already started.
    get(?SERVER_PID) =/= undefined andalso error(already_started),

    Options0 = ?SERVER_OPTIONS(Config),
    Options = wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, Options0),
    {ok, Server} = wa_raft_server:start_link(Options),

    % Store the current RAFT server info
    put(?SERVER_PID, Server),

    % Return the current state.
    sys:get_state(Server).

%% Stop just the RAFT server
-spec server_stop_only_server() -> ok.
server_stop_only_server() ->
    server_stop_only_server(normal).
-spec server_stop_only_server(Reason :: term()) -> ok.
server_stop_only_server(Reason) ->
    Pid = get(?SERVER_PID),
    gen_statem:stop(Pid, Reason, 5000),
    erase(?SERVER_PID),
    ok.

%% Stop the RAFT server by stopping it and then all its related processes.
-spec server_stop() -> ok.
server_stop() ->
    server_stop(normal).
-spec server_stop(Reason :: term()) -> ok.
server_stop(Reason) ->
    Pid = get(?SERVER_PID),

    % First get table and partition info from the RAFT server
    {_, #raft_state{table = Table, partition = Partition}} = sys:get_state(Pid),

    % Then stop all processes
    gen_statem:stop(Pid, Reason, 5000),
    gen_server:stop(wa_raft_log:registered_name(Table, Partition), Reason, 5000),
    gen_server:stop(wa_raft_storage:registered_name(Table, Partition), Reason, 5000),
    gen_server:stop(wa_raft_queue:registered_name(Table, Partition), Reason, 5000),

    % Then clear the server pid
    erase(?SERVER_PID),
    ok.

%% Execute a sys:replace_state against the RAFT server and return the
%% state afterwards.
-spec server_replace_state(Func :: fun(({atom(), #raft_state{}}) -> {atom(), #raft_state{}})) -> {atom(), #raft_state{}}.
server_replace_state(Func) ->
    Pid = get(?SERVER_PID),
    sys:replace_state(Pid, Func).


-define(assertReceive(MessagePattern), ?assertReceive(MessagePattern, 2000)).

-define(assertReceive(Pattern, Timeout),
    (fun () ->
        __Ret__ =
            receive
                Pattern = __Message__ -> __Message__
            after
                Timeout ->
                    error({assertReceive, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {reason, "expected message was not received within timeout"},
                        {pattern, ??Pattern},
                        {timeout, Timeout},
                        {messages_in_queue, process_info(self(), messages)}
                    ]})
            end,
        __Ret__
    end)()).

%% Assert that a matching notify event message was received by the
%% current testcase process within the provided timeout. Notify events
%% are sent by the RAFT server when it performs particular events like
%% setting timeouts, casting to peers, or making certain requests to
%% storage.
-define(assertNotify(TypePattern, EventPattern, Timeout),
    (fun () ->
        __Ret__ =
            receive
                {'$notify', TypePattern = __Type__, EventPattern = __Event__} -> {__Type__, __Event__}
            after
                Timeout ->
                    error({assertReceive, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {reason, "expected " ??TypePattern " was not received within timeout"},
                        {pattern, ??EventPattern},
                        {timeout, Timeout},
                        {messages_in_queue, process_info(self(), messages)}
                    ]})
            end,
        __Ret__
    end)()).

%% Assert that a cast with the specified message to the specified destination peer was made.
-define(assertCast(NamePattern, NodePattern, MessagePattern), ?assertCast(NamePattern, NodePattern, MessagePattern, 250)).
-define(assertCast(NamePattern, NodePattern, MessagePattern, Timeout), ?assertNotify(cast, {NamePattern, NodePattern, MessagePattern}, Timeout)).

%% Assert that a cast with the specified message to the specified destination peer was not made.
-define(assertNotCast(NamePattern, NodePattern, MessagePattern), ?assertNotCast(NamePattern, NodePattern, MessagePattern, 250)).
-define(assertNotCast(NamePattern, NodePattern, MessagePattern, Timeout),
    (fun () ->
        try (?assertCast(NamePattern, NodePattern, MessagePattern, Timeout)) of
            {_, {__Name__, __Node__, __Message__}} ->
                error({assertNotCast, [
                    {module, ?MODULE},
                    {line, ?LINE},
                    {reason, "unexpected cast was sent"},
                    {name, __Name__},
                    {node, __Node__},
                    {message, __Message__},
                    {name_pattern, ??NamePattern},
                    {node_pattern, ??NodePattern},
                    {message_pattern, ??MessagePattern},
                    {timeout, Timeout},
                    {messages_in_queue, process_info(self(), messages)}
                ]})
        catch
            error:{assertReceive, _} ->
                ok
        end
    end)()).

%% Assert that a matching timeout was set by some RAFT server event callback.
-define(assertSetTimeout(TimeoutPattern), ?assertSetTimeout(TimeoutPattern, 250)).
-define(assertSetTimeout(TimeoutPattern, Timeout), ?assertNotify(set_timeout, TimeoutPattern, Timeout)).

%% Assert that the RAFT server requested storage to apply the specified op.
-define(assertApplyOp(FromPattern, RecordPattern), ?assertApplyOp(FromPattern, RecordPattern, 250)).
-define(assertApplyOp(FromPattern, RecordPattern, Timeout), ?assertNotify(applied, {FromPattern, RecordPattern}, Timeout)).

%% Clear any messages in the queue. This is usually used to
%% clear any notify events from the RAFT server produced by prior
%% requests.
-spec clear_message_queue() -> ok.
clear_message_queue() ->
    receive
        _ -> clear_message_queue()
    after
        0 -> ok
    end.

-define(APPEND_ENTRIES_RPC(Term, SenderName, SenderNode, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex),
    ?RAFT_NAMED_RPC(append_entries, Term, SenderName, SenderNode, {PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex})).
-define(APPEND_ENTRIES_RESPONSE_RPC(Term, SenderName, SenderNode, PrevLogIndex, Success, MatchIndex, LastAppliedIndex),
    ?RAFT_NAMED_RPC(append_entries_response, Term, SenderName, SenderNode, {PrevLogIndex, Success, MatchIndex, LastAppliedIndex})).

-define(REQUEST_VOTE_RPC(Term, SenderName, SenderNode, ElectionType, LastLogIndex, LastLogTerm),
    ?RAFT_NAMED_RPC(request_vote, Term, SenderName, SenderNode, {ElectionType, LastLogIndex, LastLogTerm})).
-define(VOTE_RPC(Term, SenderName, SenderNode, Vote),
    ?RAFT_NAMED_RPC(vote, Term, SenderName, SenderNode, {Vote})).

-define(REQUEST_PRE_VOTE_RPC(Term, SenderName, SenderNode, Ref),
    ?RAFT_NAMED_RPC(request_pre_vote, Term, SenderName, SenderNode, {Ref})).
-define(PRE_VOTE_RPC(Term, SenderName, SenderNode, Ref, Vote, LastLogIndex, LastLogTerm),
    ?RAFT_NAMED_RPC(pre_vote, Term, SenderName, SenderNode, {Ref, Vote, LastLogIndex, LastLogTerm})).

-define(HANDOVER_RPC(Term, SenderName, SenderNode, Ref, PrevLogIndex, PrevLogTerm, Entries),
    ?RAFT_NAMED_RPC(handover, Term, SenderName, SenderNode, {Ref, PrevLogIndex, PrevLogTerm, Entries})).
-define(HANDOVER_FAILED_RPC(Term, SenderName, SenderNode, Ref),
    ?RAFT_NAMED_RPC(handover_failed, Term, SenderName, SenderNode, {Ref})).

-define(NOTIFY_TERM_RPC(Term, SenderName, SenderNode),
    ?RAFT_NAMED_RPC(notify_term, Term, SenderName, SenderNode, undefined)).

-spec init(Config :: ct_suite:ct_config()) -> ok.
init(Config) ->
    % Some data about how the server should be configured
    Name = ?SERVER_NAME(Config),
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),
    PartitionPath = ?RAFT_PARTITION_PATH(Table, Partition),

    % Start a server with data (starts at non-zero storage)
    {follower, State0} = server_start_and_bootstrap(100, 1, Config),
    ?assertEqual(Name, State0#raft_state.name),
    ?assertEqual(Table, State0#raft_state.table),
    ?assertEqual(Partition, State0#raft_state.partition),
    ?assertEqual(PartitionPath, State0#raft_state.partition_path),
    ?assertEqual(1, State0#raft_state.current_term),
    ?assertEqual(100, State0#raft_state.commit_index),
    ?assertEqual(100, State0#raft_state.last_applied),

    % Stop server
    ok = server_stop().

-spec init_witness(Config :: ct_suite:ct_config()) -> ok.
init_witness(Config) ->
    % Some data about how the server should be configured
    Name = ?SERVER_NAME(Config),
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),
    PartitionPath = ?RAFT_PARTITION_PATH(Table, Partition),

    % Start a server with data (starts at non-zero storage)
    {witness, State0} = server_start_witness_and_bootstrap(100, 1, Config),
    ?assertEqual(Name, State0#raft_state.name),
    ?assertEqual(Table, State0#raft_state.table),
    ?assertEqual(Partition, State0#raft_state.partition),
    ?assertEqual(PartitionPath, State0#raft_state.partition_path),
    ?assertEqual(1, State0#raft_state.current_term),
    ?assertEqual(100, State0#raft_state.commit_index),
    ?assertEqual(100, State0#raft_state.last_applied),

    ok = server_stop().

-spec init_empty(Config :: ct_suite:ct_config()) -> ok.
init_empty(Config) ->
    % Start and stop empty server (starting at storage index 0:0 with no durable state)
    {stalled, _} = server_start(Config),
    ok = server_stop_only_server(),

    % Start server with no data but with durable state
    {stalled, _} = server_restart(Config),

    % Bootstrap server so it has data
    {leader, _, ok} = server_call(?BOOTSTRAP_COMMAND(#raft_log_pos{index = 1, term = 1}, ?SERVER_SINGLE_CONFIG(Config), #{})),
    ok = server_stop_only_server(),

    % Start and stop server with data and durable state
    {candidate, _} = server_restart(Config),
    ok = server_stop().

-spec advance_term(Config :: ct_suite:ct_config()) -> ok.
advance_term(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Setup server
    {follower, State0} = server_start_and_bootstrap(Config),

    % Follower stays follower when handling RPCs with newer terms
    %  * Advancing term should also update the stored durable state
    {follower, StateA0} = server_cast(?NOTIFY_TERM_RPC(2, Name, Node)),
    ?assertEqual(2, StateA0#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 2}}, wa_raft_durable_state:load(State0)),

    % Candidate demotes to follower when advancing term
    {candidate, StateB0} = server_invoke(state_timeout, election),
    ?assertEqual(3, StateB0#raft_state.current_term),
    {follower, StateB1} = server_cast(?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual(4, StateB1#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 4}}, wa_raft_durable_state:load(State0)),

    % Leader demotes to follower when advancing term
    {leader, StateC0, ok} = server_call(?PROMOTE_COMMAND(5, true)),
    ?assertEqual(5, StateC0#raft_state.current_term),
    {follower, StateC1} = server_cast(?NOTIFY_TERM_RPC(6, Name, Node)),
    ?assertEqual(6, StateC1#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 6}}, wa_raft_durable_state:load(State0)),

    % Disabled stays disabled when advancing term
    {disabled, StateD0, ok} = server_call(?DISABLE_COMMAND("Test advance_term.")),
    ?assertEqual(6, StateD0#raft_state.current_term),
    {disabled, StateD1} = server_cast(?NOTIFY_TERM_RPC(7, Name, Node)),
    ?assertEqual(7, StateD1#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 7}}, wa_raft_durable_state:load(State0)),

    % Stalled stays stalled when advancing term
    {stalled, StateE0, ok} = server_call(?ENABLE_COMMAND),
    ?assertEqual(7, StateE0#raft_state.current_term),
    {stalled, StateE1} = server_cast(?NOTIFY_TERM_RPC(8, Name, Node)),
    ?assertEqual(8, StateE1#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 8}}, wa_raft_durable_state:load(State0)),

    % Stop server
    ok = server_stop().

-spec advance_term_vote(Config :: ct_suite:ct_config()) -> ok.
advance_term_vote(Config) ->
    Name = ?SERVER_NAME(Config),

    % Setup server
    {follower, _State0} = server_start_and_bootstrap(Config),

    % Follower should advance upon receiving normal vote request in new term.
    %  * No prior leader heartbeat means that this vote request should be accepted.
    {follower, StateA0} = server_cast(?REQUEST_VOTE_RPC(2, Name, node2, normal, 1, 1)),
    ?assertEqual(2, StateA0#raft_state.current_term),

    % Stop server
    ok = server_stop().

-spec advance_term_witness(Config :: ct_suite:ct_config()) -> ok.
advance_term_witness(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Setup server
    {witness, State0} = server_start_witness_and_bootstrap(Config),
    {witness, StateA0} = server_cast(?NOTIFY_TERM_RPC(2, Name, Node)),
    ?assertEqual(2, StateA0#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 2}}, wa_raft_durable_state:load(State0)),

    % Stop server
    ok = server_stop().

-spec stale_rpc(Config :: ct_suite:ct_config()) -> ok.
stale_rpc(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Setup server
    {follower, State0} = server_start_and_bootstrap(50, 4, Config),

    % Follower should ignore stale RPCs and respond with NotifyTerm
    ok = clear_message_queue(),
    ?assertEqual({follower, State0}, server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 20, false, 10, 5))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, normal, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, force, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?VOTE_RPC(3, Name, node2, true))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?HANDOVER_FAILED_RPC(3, Name, node2, make_ref()))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),
    ?assertEqual({follower, State0}, server_cast(?NOTIFY_TERM_RPC(3, Name, node2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(4, Name, Node)),

    % Candidate should ignore stale RPCs and respond with NotifyTerm
    {candidate, State1} = server_invoke(state_timeout, election),
    ok = clear_message_queue(),
    ?assertEqual({candidate, State1}, server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 20, false, 10, 5))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, normal, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, force, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?VOTE_RPC(3, Name, node2, true))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?HANDOVER_FAILED_RPC(3, Name, node2, make_ref()))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({candidate, State1}, server_cast(?NOTIFY_TERM_RPC(3, Name, node2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),

    % Leader should ignore stale RPCs and respond with NotifyTerm
    %  * Remove leader heartbeat since leader will always set it initially
    {candidate, _} = server_cast(?VOTE_RPC(5, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(5, Name, node2, true)),
    {leader, _} = server_cast(?VOTE_RPC(5, Name, node3, true)),
    {leader, State2} = server_replace_state(fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = undefined}} end),
    ok = clear_message_queue(),
    ?assertEqual({leader, State2}, server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 20, false, 10, 5))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, normal, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, force, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?VOTE_RPC(3, Name, node2, true))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?HANDOVER_FAILED_RPC(3, Name, node2, make_ref()))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({leader, State2}, server_cast(?NOTIFY_TERM_RPC(3, Name, node2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),

    % Disabled should ignore stale RPCs however it should not respond with NotifyTerm
    {disabled, State3, ok} = server_call(?DISABLE_COMMAND("Test")),
    ok = clear_message_queue(),
    ?assertEqual({disabled, State3}, server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0))),
    ?assertEqual({disabled, State3}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 20, false, 10, 5))),
    ?assertEqual({disabled, State3}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, normal, 20, 2))),
    ?assertEqual({disabled, State3}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, force, 20, 2))),
    ?assertEqual({disabled, State3}, server_cast(?VOTE_RPC(3, Name, node2, true))),
    ?assertEqual({disabled, State3}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertEqual({disabled, State3}, server_cast(?HANDOVER_FAILED_RPC(3, Name, node2, make_ref()))),
    ?assertEqual({disabled, State3}, server_cast(?NOTIFY_TERM_RPC(3, Name, node2))),
    ?assertNotCast(_, ?NOTIFY_TERM_RPC(_, Name, _), 100),

    % Stalled should ignore stale RPCs and respond with NotifyTerm
    {stalled, State4, ok} = server_call(?ENABLE_COMMAND),
    ok = clear_message_queue(),
    ?assertEqual({stalled, State4}, server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 20, false, 10, 5))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, normal, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?REQUEST_VOTE_RPC(3, Name, node2, force, 20, 2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?VOTE_RPC(3, Name, node2, true))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?HANDOVER_FAILED_RPC(3, Name, node2, make_ref()))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({stalled, State4}, server_cast(?NOTIFY_TERM_RPC(3, Name, node2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),

    % Stop server
    ok = server_stop().

-spec stale_rpc_witness(Config :: ct_suite:ct_config()) -> ok.
stale_rpc_witness(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Setup server
    {witness, State5} = server_start_witness_and_bootstrap(50, 5, Config),

    ok = clear_message_queue(),
    ?assertEqual({witness, State5}, server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({witness, State5}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 20, false, 10, 5))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({witness, State5}, server_cast(?VOTE_RPC(3, Name, node2, true))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({witness, State5}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({witness, State5}, server_cast(?HANDOVER_FAILED_RPC(3, Name, node2, make_ref()))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),
    ?assertEqual({witness, State5}, server_cast(?NOTIFY_TERM_RPC(3, Name, node2))),
    ?assertCast(Name, node2, ?NOTIFY_TERM_RPC(5, Name, Node)),

    % Stop server
    ok = server_stop().

-spec election(Config :: ct_suite:ct_config()) -> ok.
election(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Setup server
    {follower, State0} = server_start_and_bootstrap(1, 1, Config),
    ?assertEqual(1, State0#raft_state.current_term),

    % Trigger election timeout on follower
    %  * Follower should move to candidate because election starts
    %  * Candidate should advance term for election
    %  * Advancing term should update durable state with new term
    %  * Candidate should send a vote to self and request votes from all peers
    {candidate, State1} = server_invoke(state_timeout, election),
    ?assertEqual(2, State1#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 2}}, wa_raft_durable_state:load(State0)),
    ?assertCast(Name, Node, ?VOTE_RPC(2, Name, Node, true)),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node5, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),

    % Trigger election timeout on candidate
    %  * Candidate should advance another term for election restart
    %  * Advancing term should update durable state with new term
    %  * Candidate should send a vote to self and request votes from all peers
    {candidate, State2} = server_invoke(state_timeout, election),
    ?assertEqual(3, State2#raft_state.current_term),
    ?assertMatch({ok, #raft_state{current_term = 3}}, wa_raft_durable_state:load(State0)),
    ?assertCast(Name, Node, ?VOTE_RPC(3, Name, Node, true)),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(3, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(3, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(3, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node5, ?REQUEST_VOTE_RPC(3, Name, Node, normal, 1, 1)),

    % Votes come in for election
    %  * Candidate should wait until it gets a quorum of votes
    %  * Once it gets a quorum, it should make itself leader
    {candidate, State3} = server_cast(?VOTE_RPC(3, Name, Node, true)), % vote yes to self
    ?assertEqual(#{Node => true}, State3#raft_state.votes),
    {candidate, State4} = server_cast(?VOTE_RPC(3, Name, node2, true)), % node2 votes yes
    ?assertEqual(#{Node => true, node2 => true}, State4#raft_state.votes),
    {candidate, State5} = server_cast(?VOTE_RPC(3, Name, node2, true)), % node2 votes yes again
    ?assertEqual(#{Node => true, node2 => true}, State5#raft_state.votes),
    {candidate, State6} = server_cast(?VOTE_RPC(3, Name, node3, false)), % node3 votes false
    ?assertEqual(#{Node => true, node2 => true, node3 => false}, State6#raft_state.votes),
    {leader, State7} = server_cast(?VOTE_RPC(3, Name, node4, true)), % node4 votes true (quorum reached)
    ?assertEqual(#{Node => true, node2 => true, node3 => false, node4 => true}, State7#raft_state.votes),

    % Add a few uncommitted log entries and then immediately advance to a later term
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {make_ref(), noop}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {make_ref(), noop}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {make_ref(), noop}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {make_ref(), noop}, high)),
    {leader, _} = server_invoke(state_timeout, heartbeat), % sync
    {follower, _} = server_cast(?NOTIFY_TERM_RPC(4, Name, node2)),

    % Become leader for the new term with uncommitted log entries
    {candidate, _} = server_invoke(state_timeout, election),
    {candidate, _} = server_cast(?VOTE_RPC(5, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(5, Name, node2, true)),
    {leader, _} = server_cast(?VOTE_RPC(5, Name, node3, true)),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(5, Name, node2, 7, false, 1, 1)),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(5, Name, node3, 7, false, 1, 1)),

    % Send real replication heartbeat but only find quorum up to index 3 (with term 3)
    %  * Leader cannot commit log entries from previous terms until it
    %    commits at least one entry from the current term
    {leader, _} = server_invoke(state_timeout, heartbeat),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(5, Name, node2, 1, true, 3, 1)),
    {leader, State8} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(5, Name, node3, 1, true, 3, 1)),
    ?assertEqual(1, State8#raft_state.commit_index),

    % Now replicate a quorum at 7 (with term 5) this time
    %  * Leader can now start committing log entries since it has
    %    replicated a quorum for its initial noop commit.
    {leader, _} = server_invoke(state_timeout, heartbeat),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(5, Name, node2, 4, true, 7, 1)),
    {leader, State9} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(5, Name, node3, 4, true, 7, 1)),
    ?assertEqual(7, State9#raft_state.commit_index),

    % Stop server
    ok = server_stop().


-spec election_three_members(Config :: ct_suite:ct_config()) -> ok.
election_three_members(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Candidates should only request votes from peers in membership
    % With 3 nodes, election quorum only requires two true votes.
    ConfigA = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3}
    ]),
    {follower, _} = server_start_and_bootstrap(ConfigA, Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {follower, _, ok} = server_call(?RESIGN_COMMAND),
    {candidate, _} = server_invoke(state_timeout, election),
    ?assertCast(Name, Node, ?VOTE_RPC(2, Name, Node, true)),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(2, Name, Node, normal, _, _)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(2, Name, Node, normal, _, _)),
    ?assertNotCast(Name, node4, ?REQUEST_VOTE_RPC(2, Name, Node, _, _, _)),
    ?assertNotCast(Name, node5, ?REQUEST_VOTE_RPC(2, Name, Node, _, _, _)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    {leader, _} = server_cast(?VOTE_RPC(2, Name, node2, true)),

    % Stop server
    ok = server_stop().

% Candidate - election quorum is based on config (4 nodes)
-spec election_four_members(Config :: ct_suite:ct_config()) -> term().
election_four_members(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % With 4 nodes, election quorum requires three true votes.
    ConfigA = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3},
        #raft_identity{name = Name, node = node4}
    ]),
    {follower, _} = server_start_and_bootstrap(ConfigA, Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {follower, _, ok} = server_call(?RESIGN_COMMAND),
    {candidate, _} = server_invoke(state_timeout, election),
    ?assertCast(Name, Node, ?VOTE_RPC(2, Name, Node, true)),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(2, Name, Node, normal, _, _)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(2, Name, Node, normal, _, _)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(2, Name, Node, normal, _, _)),
    ?assertNotCast(Name, node5, ?REQUEST_VOTE_RPC(2, Name, Node, _, _, _)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, node2, true)),
    {leader, _} = server_cast(?VOTE_RPC(2, Name, node3, true)),

    % Stop server
    ok = server_stop().

-spec request_vote(Config :: ct_suite:ct_config()) -> ok.
request_vote(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server
    {follower, _} = server_start_and_bootstrap(100, 2, Config),

    % Followers can only vote yes to a single peer each term
    {follower, StateA0} = server_cast(?REQUEST_VOTE_RPC(2, Name, node2, normal, 100, 2)),
    ?assertEqual(node2, StateA0#raft_state.voted_for),
    ?assertCast(Name, node2, ?VOTE_RPC(2, Name, Node, true)),
    {follower, StateA1} = server_cast(?REQUEST_VOTE_RPC(2, Name, node3, normal, 105, 2)),
    ?assertEqual(node2, StateA1#raft_state.voted_for),
    ?assertNotCast(Name, node3, ?VOTE_RPC(2, Name, Node, true)),
    {follower, StateA2} = server_cast(?REQUEST_VOTE_RPC(2, Name, node4, normal, 110, 2)),
    ?assertEqual(node2, StateA2#raft_state.voted_for),
    ?assertNotCast(Name, node4, ?VOTE_RPC(2, Name, Node, true)),
    {follower, StateA3} = server_cast(?REQUEST_VOTE_RPC(2, Name, node5, normal, 100, 4)),
    ?assertEqual(node2, StateA3#raft_state.voted_for),
    ?assertNotCast(Name, node5, ?VOTE_RPC(2, Name, Node, true)),

    % In a new term, a new peer can be voted for
    {follower, StateB0} = server_cast(?REQUEST_VOTE_RPC(3, Name, node3, normal, 100, 2)),
    ?assertEqual(node3, StateB0#raft_state.voted_for),
    ?assertCast(Name, node3, ?VOTE_RPC(3, Name, Node, true)),
    {follower, StateB1} = server_cast(?REQUEST_VOTE_RPC(3, Name, node2, normal, 100, 2)),
    ?assertEqual(node3, StateB1#raft_state.voted_for),
    ?assertNotCast(Name, node2, ?VOTE_RPC(3, Name, Node, true)),

    % Followers can only vote yes to a peer whose log is at least
    % as up-to-date as their own log (lexicographically on {Term, Index})
    {follower, StateC0} = server_cast(?REQUEST_VOTE_RPC(4, Name, node2, normal, 50, 2)),
    ?assertEqual(undefined, StateC0#raft_state.voted_for),
    ?assertNotCast(Name, node2, ?VOTE_RPC(4, Name, Node, true)),
    {follower, StateC1} = server_cast(?REQUEST_VOTE_RPC(5, Name, node2, normal, 150, 1)),
    ?assertEqual(undefined, StateC1#raft_state.voted_for),
    ?assertNotCast(Name, node2, ?VOTE_RPC(5, Name, Node, true)),
    {follower, StateC2} = server_cast(?REQUEST_VOTE_RPC(6, Name, node2, normal, 100, 1)),
    ?assertEqual(undefined, StateC2#raft_state.voted_for),
    ?assertNotCast(Name, node2, ?VOTE_RPC(6, Name, Node, true)),
    {follower, StateC3} = server_cast(?REQUEST_VOTE_RPC(7, Name, node2, normal, 100, 3)),
    ?assertEqual(node2, StateC3#raft_state.voted_for),
    ?assertCast(Name, node2, ?VOTE_RPC(7, Name, Node, true)),
    {follower, StateC4} = server_cast(?REQUEST_VOTE_RPC(8, Name, node2, normal, 101, 2)),
    ?assertEqual(node2, StateC4#raft_state.voted_for),
    ?assertCast(Name, node2, ?VOTE_RPC(8, Name, Node, true)),
    {follower, StateC5} = server_cast(?REQUEST_VOTE_RPC(9, Name, node2, normal, 50, 3)),
    ?assertEqual(node2, StateC5#raft_state.voted_for),
    ?assertCast(Name, node2, ?VOTE_RPC(9, Name, Node, true)),

    % Stop server
    ok = server_stop().

-spec request_vote_drop(Config :: ct_suite:ct_config()) -> ok.
request_vote_drop(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    SetHeartbeat = fun ({SN, S}) -> {SN, S#raft_state{leader_commit_index_ts = erlang:monotonic_time(millisecond)}} end,

    % Followers should reject normal vote requests when leader is active
    {follower, _} = server_start_and_bootstrap(Config),
    {follower, State0} = server_replace_state(SetHeartbeat),
    {follower, State0} = server_cast(?REQUEST_VOTE_RPC(10, Name, node2, normal, 10, 10)),
    ?assertCast(Name, node2, ?VOTE_RPC(_, Name, Node, false)),

    % Candidates should reject normal vote requests when leader is active
    {candidate, _} = server_invoke(state_timeout, election),
    {candidate, State1} = server_replace_state(SetHeartbeat),
    {candidate, State1} = server_cast(?REQUEST_VOTE_RPC(10, Name, node2, normal, 10, 10)),
    ?assertCast(Name, node2, ?VOTE_RPC(_, Name, Node, false)),

    % Leaders should reject normal vote requests because they are leader
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(5, true)),
    {leader, State2} = server_replace_state(SetHeartbeat),
    {leader, State2} = server_cast(?REQUEST_VOTE_RPC(10, Name, node2, normal, 10, 10)),
    ?assertCast(Name, node2, ?VOTE_RPC(_, Name, Node, false)),

    % Disabled should reject normal vote requests
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test")),
    {disabled, State3} = server_replace_state(SetHeartbeat),
    {disabled, State3} = server_cast(?REQUEST_VOTE_RPC(10, Name, node2, normal, 10, 10)),
    ?assertCast(Name, node2, ?VOTE_RPC(_, Name, Node, false)),

    % Stalled should reject normal vote requests when leader is active
    {stalled, _, ok} = server_call(?ENABLE_COMMAND),
    {stalled, State4} = server_replace_state(SetHeartbeat),
    {stalled, State4} = server_cast(?REQUEST_VOTE_RPC(10, Name, node2, normal, 10, 10)),
    ?assertCast(Name, node2, ?VOTE_RPC(_, Name, Node, false)),

    % Stop server
    ok = server_stop().

-spec append_entries(Config :: ct_suite:ct_config()) -> ok.
append_entries(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server
    {follower, _} = server_start_and_bootstrap(100, 2, Config),

    % Follower handles heartbeat at 100:2 (commit 101) with one new entry
    %  * Index and term of previous log index matches, so follower should
    %    append successfully
    %  * Commit index is 101 so follower should apply new log entry 101
    %    immediately
    %  * Response is issued before apply, so last applied index is 100
    {follower, State1} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 100, 2, [{2, {ref, noop}}], 101, 0)),
    ?assertSetTimeout({state_timeout, _, election}),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(2, Name, Node, 100, true, 101, 100)),
    ?assertEqual(101, wa_raft_log:last_index(State1#raft_state.log_view)), % log index updated
    ?assertEqual(101, State1#raft_state.commit_index), % commit index is updated
    ?assertEqual(101, State1#raft_state.last_applied), % last_applied is updated

    % Follower gets a heartbeat from a leader in a new term
    %  * Follower should advance to the new term as well as reset the
    %    election timeout (there are two resets, one for when the term
    %    advances, and one for the heartbeat)
    %  * Index and term of previous log index matches, so follower should
    %    append successfully
    %  * Commit index is 102 so follower should apply new log entry 102
    %    immediately
    %  * Response is issued before apply, so last applied index is 101
    clear_message_queue(),
    {follower, State2} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 101, 2, [{2, {ref, noop}}], 102, 0)),
    ?assertSetTimeout({state_timeout, _, election}),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 101, true, 102, 101)),
    ?assertEqual(3, State2#raft_state.current_term), % current term advanced
    ?assertEqual(102, wa_raft_log:last_index(State2#raft_state.log_view)), % log entry is appended
    ?assertEqual(102, State2#raft_state.commit_index), % commit index is updated
    ?assertEqual(102, State2#raft_state.last_applied), % log entry is applied

    % Follower gets a heartbeat but it does not have the previous log entry
    %  * Previous log entry does not exist so the append fails
    %  * Follower should still reset election timeout
    clear_message_queue(),
    {follower, State3} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 105, 2, [{2, {ref, noop}}], 102, 0)),
    ?assertSetTimeout({state_timeout, _, election}),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 105, false, 102, 102)),
    ?assertEqual(3, State3#raft_state.current_term), % current term preserved
    ?assertEqual(102, wa_raft_log:last_index(State3#raft_state.log_view)), % log entries preserved
    ?assertEqual(102, State2#raft_state.commit_index), % commit index is unchanged
    ?assertEqual(102, State3#raft_state.last_applied), % no new applies

    % Follower gets a heartbeat with a duplicate log entry
    %  * Index and term of previous log index matches, and the terms of the
    %    new log entries match, so follower should append successfully
    %  * Commit index is unchanged so follower should not apply
    clear_message_queue(),
    {follower, State4} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 101, 2, [{2, {ref, noop}}, {2, {ref, noop}}], 102, 0)),
    ?assertSetTimeout({state_timeout, _, election}),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 101, true, 103, 102)),
    ?assertEqual(103, wa_raft_log:last_index(State4#raft_state.log_view)), % only append 1
    ?assertEqual(102, State4#raft_state.commit_index), % commit index is unchanged
    ?assertEqual(102, State4#raft_state.last_applied), % last_applied is not updated - commit index is not changed

    % Follower gets an empty heartbeat with a new commit index
    %  * Index and term of previous log index matches so follower
    %    should append successfully
    %  * Commit index is advanced so follower should apply existing
    %    log entry 103
    %  * Response is issued before apply, so last applied index is 102
    clear_message_queue(),
    {follower, State5} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 103, 2, [], 103, 0)),
    ?assertSetTimeout({state_timeout, _, election}),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 103, true, 103, 102)),
    ?assertEqual(103, wa_raft_log:last_index(State5#raft_state.log_view)), % nothing is appended
    ?assertEqual(103, State5#raft_state.commit_index), % commit index is updated
    ?assertEqual(103, State5#raft_state.last_applied), % last applied is updated

    % Follower gets an empty heartbeat with a commit index past the end of the log
    %  * Index and term of previous log index matches so follower
    %    should append successfully
    %  * Commit index is limited to the last log index
    clear_message_queue(),
    {follower, State6} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 103, 2, [], 105, 0)),
    ?assertSetTimeout({state_timeout, _, election}),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 103, true, 103, 103)),
    ?assertEqual(103, wa_raft_log:last_index(State6#raft_state.log_view)), % nothing is appended
    ?assertEqual(103, State6#raft_state.commit_index), % commit index is not changed
    ?assertEqual(103, State6#raft_state.last_applied), % last applied is not changed

    % Stop server
    ok = server_stop().

-spec append_entries_witness(Config :: ct_suite:ct_config()) -> ok.
append_entries_witness(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server
    clear_message_queue(),
    {witness, _} = server_start_witness_and_bootstrap(100, 2, Config),

    clear_message_queue(),
    {witness, State1} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 100, 2, [{2, {ref, noop}}], 101, 0)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(2, Name, Node, 100, true, 101, 100)),
    ?assertEqual(101, wa_raft_log:last_index(State1#raft_state.log_view)), % log index updated
    ?assertEqual(101, State1#raft_state.commit_index), % commit index is updated
    ?assertEqual(101, State1#raft_state.last_applied), % last_applied is updated

    clear_message_queue(),
    {witness, _} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 101, 2, [], 101, 0)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(2, Name, Node, 101, true, 101, _)),

    ok = server_stop().

% Leader - a witness must never be sent log entries beyond the highest index
% any full member has acknowledged (its max match index), while a full follower in
% the very same heartbeat still receives everything up to the log end. A witness
% that runs ahead of the full replicas outranks them in the vote up-to-dateness
% check and wedges leader elections.
-spec append_entries_witness_match_cap(Config :: ct_suite:ct_config()) -> ok.
append_entries_witness_match_cap(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Full members: self, node2, node3, node4, node5; witnesses: node4, node5.
    {follower, _} = server_start_and_bootstrap(?SERVER_CLUSTER_CONFIG_WITH_WITNESS(Config), Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Seed the leader's log up to index 6 (index 1 holds the promotion noop). A
    % single heartbeat flushes all batched commits into the log.
    LastLogIndex = 6,
    ok = lists:foreach(
        fun (_) -> server_cast(?COMMIT_COMMAND(?FROM(), {make_ref(), noop}, high)) end,
        lists:seq(1, 5)
    ),
    {leader, SeededState} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(LastLogIndex, wa_raft_log:last_index(SeededState#raft_state.log_view)),

    % Set match indices so the highest index acknowledged by any member is 4 --
    % strictly below the leader's log end (6) -- with a full follower (node2) at
    % that maximum and no member (including the witnesses) above it. Rewind the
    % next index of one full follower (node2) and one witness (node4) below the
    % maximum so the heartbeat window would otherwise reach the log end.
    MaxMatchIndex = 4,
    ReplicateFrom = 2,
    {leader, _} = server_replace_state(
        fun ({leader, State}) ->
            {leader, State#raft_state{
                match_indices = #{node2 => MaxMatchIndex, node3 => 3, node4 => 2, node5 => 2},
                next_indices = (State#raft_state.next_indices)#{node2 => ReplicateFrom, node4 => ReplicateFrom}
            }}
        end
    ),

    % Trigger a single replication round.
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % The witness heartbeat must stop at the max match index: the highest index
    % it replicates (PrevLogIndex + number of entries) may not exceed it, and it
    % must fall strictly short of the log end so the cap is doing real work.
    {cast, {_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, WitnessPrevLogIndex, _, WitnessEntries, _, _)}} =
        ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    WitnessMaxIndex = WitnessPrevLogIndex + length(WitnessEntries),
    % The witness still replicates the acknowledged tail (a real heartbeat, not a
    % degenerate empty one).
    ?assertNotEqual([], WitnessEntries),
    ?assertLessThanOrEqual(
        WitnessMaxIndex, MaxMatchIndex,
        "witness must not be sent entries past the max follower match index"
    ),
    ?assertLessThan(
        WitnessMaxIndex, LastLogIndex,
        "witness cap must fall short of the leader's log end"
    ),

    % The same heartbeat to a full follower still carries everything up to the
    % log end, confirming the cap is witness-specific.
    {cast, {_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, FullPrevLogIndex, _, FullEntries, _, _)}} =
        ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    FullMaxIndex = FullPrevLogIndex + length(FullEntries),
    ?assertGreaterThan(
        FullMaxIndex, MaxMatchIndex,
        "full follower must still receive entries past the max match index"
    ),

    ok = server_stop().

% Leader - with two witnesses in a 3 full + 2 witness cluster, the replication
% cap is driven only by full-member match indices, so a witness never runs ahead
% of the surviving full replicas even when the other witness (or a stale down
% replica) would raise the all-member maximum. Covers one-full-down and
% two-full-down states across different full/witness replication positions.
-spec append_entries_two_witness_full_member_cap(Config :: ct_suite:ct_config()) -> ok.
append_entries_two_witness_full_member_cap(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Full members: self, node2, node3; witnesses: node4, node5.
    {follower, _} = server_start_and_bootstrap(?SERVER_CLUSTER_CONFIG_WITH_WITNESS(Config), Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Seed the leader's log up to index 6 (index 1 holds the promotion noop).
    LastLogIndex = 6,
    ok = lists:foreach(
        fun (_) -> server_cast(?COMMIT_COMMAND(?FROM(), {make_ref(), noop}, high)) end,
        lists:seq(1, 5)
    ),
    {leader, SeededState} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(LastLogIndex, wa_raft_log:last_index(SeededState#raft_state.log_view)),

    % State 1 -- one full replica (node3) is down and both witnesses have raced
    % ahead of the surviving full replicas. The full-member maximum is node2's
    % index (4), so the witnesses must be capped there even though their own
    % match indices (6) would raise the all-member maximum to the log end.
    FullMemberMax = 4,
    {leader, _} = server_replace_state(
        fun ({leader, State}) ->
            {leader, State#raft_state{
                match_indices = #{node2 => FullMemberMax, node3 => 0, node4 => 6, node5 => 6},
                next_indices = (State#raft_state.next_indices)#{node2 => 2, node5 => 2}
            }}
        end
    ),
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % The witness is capped at the surviving full-member maximum, not at the other
    % witness's higher position, and stops short of the log end.
    {cast, {_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, WitnessPrevLogIndex, _, WitnessEntries, _, _)}} =
        ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    WitnessMaxIndex = WitnessPrevLogIndex + length(WitnessEntries),
    ?assertNotEqual([], WitnessEntries),
    ?assertLessThanOrEqual(
        WitnessMaxIndex, FullMemberMax,
        "witness must not be sent entries past the full-member max with two witnesses"
    ),
    ?assertLessThan(
        WitnessMaxIndex, LastLogIndex,
        "two-witness cap must fall short of the log end"
    ),

    % A surviving full follower still receives entries up to the log end.
    {cast, {_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, FullPrevLogIndex, _, FullEntries, _, _)}} =
        ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertGreaterThan(
        FullPrevLogIndex + length(FullEntries), FullMemberMax,
        "full follower must still receive entries past the full-member max"
    ),

    % State 2 -- two full replicas (node2, node3) are down, leaving only the
    % leader as a live full member. The full-member maximum collapses to the
    % leader's own recorded index (0), so both witnesses are held to empty
    % heartbeats and can never outrank the one surviving full replica.
    {leader, _} = server_replace_state(
        fun ({leader, State}) ->
            {leader, State#raft_state{
                match_indices = #{node2 => 0, node3 => 0, node4 => 6, node5 => 6},
                next_indices = (State#raft_state.next_indices)#{node4 => 2, node5 => 2}
            }}
        end
    ),
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),

    {cast, {_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, Witness4Entries, _, _)}} =
        ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertEqual(
        [], Witness4Entries,
        "with only the leader as a live full member, witnesses receive no new entries"
    ),
    {cast, {_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, Witness5Entries, _, _)}} =
        ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertEqual(
        [], Witness5Entries,
        "with only the leader as a live full member, witnesses receive no new entries"
    ),

    ok = server_stop().

% Leader - no new log. expect empty heartbeat periodically
-spec heartbeat(Config :: ct_suite:ct_config()) -> term().
heartbeat(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Leader should immediately send a heartbeat and set heartbeat timeout upon becoming leader.
    % This heartbeat should contain the leader's starting noop.
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, State0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertSetTimeout({state_timeout, _, heartbeat}),
    ?assertEqual(#{node2 => 2, node3 => 2, node4 => 2, node5 => 2}, State0#raft_state.next_indices),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),

    % After heartbeat timeout, leader should send another heartbeat and set
    % another heartbeat timeout
    clear_message_queue(),
    {leader, State1} = server_invoke(state_timeout, heartbeat),
    ?assertSetTimeout({state_timeout, _, heartbeat}),
    ?assertEqual(#{node2 => 2, node3 => 2, node4 => 2, node5 => 2}, State1#raft_state.next_indices),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [], 0, 0)),

    % Stop server
    ok = server_stop().

% Leader - only send heartbeats to peers in membership
-spec heartbeat_three_members(Config :: ct_suite:ct_config()) -> ok.
heartbeat_three_members(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Promote the leader with only 3 nodes
    %  * Promotion with config forcibly adds a new config log entry to the end
    %    of the log before transitioning to leader.
    ConfigA = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3}
    ]),
    {follower, _} = server_start_and_bootstrap(ConfigA, Config),
    {leader, StateA0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertSetTimeout({state_timeout, _, heartbeat}),
    ?assertMatch(#{node2 := _, node3 := _}, StateA0#raft_state.next_indices),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, _, 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, _, 0, 0)),
    ?assertNotCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertNotCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),

    % Stop server
    ok = server_stop().

% Leader - only send heartbeats to peers in membership
-spec heartbeat_four_members(Config :: ct_suite:ct_config()) -> ok.
heartbeat_four_members(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Promote the leader with 4 nodes now
    ConfigB = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3},
        #raft_identity{name = Name, node = node4}
    ]),
    {follower, _} = server_start_and_bootstrap(ConfigB, Config),
    {leader, StateB0, ok} = server_call(?PROMOTE_COMMAND(2, true)),
    ?assertSetTimeout({state_timeout, _, heartbeat}),
    ?assertMatch(#{node2 := _, node3 := _, node4 := _}, StateB0#raft_state.next_indices),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, 0)),
    ?assertNotCast(Name, node5, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),

    % Stop server
    ok = server_stop().

% Leader - Next and match index is set on append entries responses
-spec replication_index(Config :: ct_suite:ct_config()) -> term().
replication_index(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(10, 1, Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(2, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 10, 1, [{2, _}], 10, _)),

    % Add a log entry - pipelining
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {ref, noop}, high)),
    {leader, State0} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 11, 2, [{2, {ref, noop}}], 10, _)),
    ?assertEqual(12, wa_raft_log:last_index(State0#raft_state.log_view)),

    % Follower responds with missing log - send from zero
    %  * The log entry at zero always exists, so the leader will attempt to start
    %    sending from 0:0 here.
    clear_message_queue(),
    {leader, State1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 10, false, 0, 0)),
    ?assertEqual(#{node2 => 1, node3 => 13, node4 => 13, node5 => 13}, State1#raft_state.next_indices),
    ?assertEqual(#{}, State1#raft_state.match_indices),
    % TODO T246543927 Currently crashes due to a bug when sending heartbeats.
    % Leader attempts to read from 0 to existing log because log entry at 0
    % exists.
    % {leader, _} = server_invoke(state_timeout, heartbeat),
    % ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, [], 10, _)),

    % Follower responds with lagging log - leader missing log
    %  * When the leader is missing a log entry to send, it sends a heartbeat
    %    at the end of the log instead and attempts to use alternate means to
    %    replicate to the follower.
    clear_message_queue(),
    {leader, State2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 10, false, 8, 8)),
    ?assertEqual(#{node2 => 9, node3 => 13, node4 => 13, node5 => 13}, State2#raft_state.next_indices),
    ?assertEqual(#{}, State2#raft_state.match_indices),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 12, 2, [], 10, _)),

    % Follower responds with advanced log (post-snapshot reset)
    %  * We need to allow the follower to notify the leader that it has applied a
    %    snapshot by sending us an up-to-date end index for the follower's log.
    clear_message_queue(),
    {leader, State3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 0, false, 10, 10)),
    ?assertEqual(#{node2 => 11, node3 => 13, node4 => 13, node5 => 13}, State3#raft_state.next_indices),
    ?assertEqual(#{}, State3#raft_state.match_indices),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 10, 1, [{2, _} | _], 10, _)),

    % Follower replication is normal - pipelining
    clear_message_queue(),
    {leader, State4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 10, true, 12, 10)),
    ?assertEqual(#{node2 => 13, node3 => 13, node4 => 13, node5 => 13}, State4#raft_state.next_indices),
    ?assertEqual(#{node2 => 12}, State4#raft_state.match_indices),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 12, 2, [], 10, _)),

    % Follower out of order response - pipelining
    clear_message_queue(),
    {leader, State5} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 10, true, 11, 10)),
    ?assertEqual(#{node2 => 13, node3 => 13, node4 => 13, node5 => 13}, State5#raft_state.next_indices),
    ?assertEqual(#{node2 => 11}, State5#raft_state.match_indices),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 12, 2, [], 10, _)),

    % Follower out of order response - missing log
    clear_message_queue(),
    {leader, State6} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 11, false, 11, 10)),
    ?assertEqual(#{node2 => 12, node3 => 13, node4 => 13, node5 => 13}, State6#raft_state.next_indices),
    ?assertEqual(#{node2 => 11}, State6#raft_state.match_indices),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 11, 2, [{2, _} | _], 10, _)),

    % Stop server
    ok = server_stop().

% Commit a log entry at 2:1
-spec commit(Config :: ct_suite:ct_config()) -> ok.
commit(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Begin commit for a new log entry at log index 2. The log entry should be batched
    From0 = ?FROM(),
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(From0, {ref0, noop}, high)),
    ?assertNotCast(_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),

    % A heartbeat timeout ends commit batching
    %  * Leader must persist batched log entries to log
    %  * Leader must start replication for added log entry
    {leader, State0} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(#{2 => {From0, high}}, State0#raft_state.queued),
    ?assertEqual({ok, {1, {ref0, noop}}}, wa_raft_log:get(State0#raft_state.log_view, 2)),
    ?assertMatch(#{node2 := 3, node3 := 3, node4 := 3, node5 := 3}, State0#raft_state.next_indices),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, noop}}], 0, 0)),

    % Acknowledged log entries before quorum do not advance commit index
    clear_message_queue(),
    {leader, State1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 0)),
    ?assertEqual(0, State1#raft_state.commit_index),
    ?assertEqual(0, State1#raft_state.last_applied),

    % Acknowledged log entries that form quorum advance commit index and are applied
    clear_message_queue(),
    {leader, State2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, 2, 0)),
    ?assertEqual(2, State2#raft_state.commit_index),
    ?assertEqual(2, State2#raft_state.last_applied),
    ?assertApplyOp(From0, {2, {1, {ref0, undefined, noop}}}),

    % Stop server
    ok = server_stop().

-spec commit_label(Config :: ct_suite:ct_config()) -> ok.
commit_label(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Set some environments related to log labelling.
    % We install a log label function that monotonically increases the label by one.
    ok = application:set_env(?RAFT_APPLICATION, raft_label_module, wa_raft_test_label),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Begin commit for two new log entries at 2:1. Leader should initially batch the commit
    From0 = ?FROM(),
    Command0 = {write, table, key1, value1},
    From1 = ?FROM(),
    Command1 = {write, table, key2, value2},
    From2 = ?FROM(),
    Command2 = noop,

    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(From0, {ref0, Command0}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(From1, {ref1, Command1}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(From2, {ref2, Command2}, high)),
    ?assertNotCast(_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),

    % A heartbeat timeout ends commit batching
    %  * Leader must persist batched log entries to log
    %  * Leader must start replication for added log entry
    {leader, State0} = server_invoke(state_timeout, heartbeat),
    ?assertEqual({ok, {1, {ref0, 1, Command0}}}, wa_raft_log:get(State0#raft_state.log_view, 2)),
    ?assertEqual({ok, {1, {ref1, 2, Command1}}}, wa_raft_log:get(State0#raft_state.log_view, 3)),
    ?assertEqual({ok, {1, {ref2, 2, Command2}}}, wa_raft_log:get(State0#raft_state.log_view, 4)),
    ?assertMatch(#{node2 := 5, node3 := 5, node4 := 5, node5 := 5}, State0#raft_state.next_indices),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, 1, Command0}}, {1, {ref1, 2, Command1}}, {1, {ref2, 2, Command2}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, 1, Command0}}, {1, {ref1, 2, Command1}}, {1, {ref2, 2, Command2}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, 1, Command0}}, {1, {ref1, 2, Command1}}, {1, {ref2, 2, Command2}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {ref0, 1, Command0}}, {1, {ref1, 2, Command1}}, {1, {ref2, 2, Command2}}], 0, 0)),

    % Acknowledged log entries before quorum do not advance commit index
    clear_message_queue(),
    {leader, State1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 0)),
    ?assertEqual(0, State1#raft_state.commit_index),
    ?assertEqual(0, State1#raft_state.last_applied),

    % Acknowledged log entries that form quorum advance commit index and are applied
    clear_message_queue(),
    {leader, State2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, 2, 0)),
    ?assertEqual(2, State2#raft_state.commit_index),
    ?assertEqual(2, State2#raft_state.last_applied),
    ?assertApplyOp(From0, {2, {1, {ref0, 1, Command0}}}),

    {leader, State3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 2, true, 3, 2)),
    ?assertEqual(2, State3#raft_state.commit_index),
    ?assertEqual(2, State3#raft_state.last_applied),
    {leader, State4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 2, true, 3, 2)),
    ?assertEqual(3, State4#raft_state.commit_index),
    ?assertEqual(3, State4#raft_state.last_applied),
    ?assertApplyOp(From1, {3, {1, {ref1, 2, Command1}}}),

    {leader, State5} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 3, true, 4, 3)),
    ?assertEqual(3, State5#raft_state.commit_index),
    ?assertEqual(3, State5#raft_state.last_applied),
    {leader, State6} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 3, true, 4, 3)),
    ?assertEqual(4, State6#raft_state.commit_index),
    ?assertEqual(4, State6#raft_state.last_applied),
    ?assertApplyOp(From2, {4, {1, {ref2, 2, Command2}}}),

    ok = application:set_env(?RAFT_APPLICATION, raft_label_module, undefined),

    % Stop server
    ok = server_stop().

-spec commit_witness(Config :: ct_suite:ct_config()) -> ok.
commit_witness(Config) ->

    {witness, _} = server_start_witness_and_bootstrap(Config),

    Ref0 = make_ref(),
    clear_message_queue(),

    ?assertMatch({witness, _}, server_cast(?COMMIT_COMMAND(?FROM(Ref0), {ref0, noop}, high))),
    ?assertReceive({Ref0, {error, not_leader}}),

    ok = server_stop().

-spec commit_follower(Config :: ct_suite:ct_config()) -> ok.
commit_follower(Config) ->

    {follower, _} = server_start_and_bootstrap(Config),

    Ref0 = make_ref(),
    clear_message_queue(),

    ?assertMatch({follower, _}, server_cast(?COMMIT_COMMAND(?FROM(Ref0), {ref0, noop}, high))),
    ?assertReceive({Ref0, {error, not_leader}}),

    ok = server_stop().

-spec commit_cancelled_truncate(Config :: ct_suite:ct_config()) -> ok.
commit_cancelled_truncate(Config) ->
    Name = ?SERVER_NAME(Config),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Add a commit request and queue it by appending it to the log
    Ref0 = make_ref(),
    From0 = ?FROM(Ref0),
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(From0, {ref0, noop}, high)),
    {leader, State0} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(#{2 => {From0, high}}, State0#raft_state.queued),

    % Loss of leadership should not dequeue the commit
    {follower, State1} = server_cast(?NOTIFY_TERM_RPC(2, Name, node2)),
    ?assertEqual(#{2 => {From0, high}}, State1#raft_state.queued),

    % Truncation of a queued log entry should cancel the commit
    {follower, State2} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 1, 1, [{2, {ref1, noop}}], 0, 0)),
    ?assertEqual(#{}, State2#raft_state.queued),
    ?assertReceive({Ref0, {error, not_leader}}),

    % Stop server
    ok = server_stop().

-spec snapshot_leader_witness(Config :: ct_suite:ct_config()) -> term().
snapshot_leader_witness(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(?SERVER_CLUSTER_CONFIG_WITH_WITNESS(Config), Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    % No follower has acknowledged any entry yet (max follower match index 0),
    % so witnesses receive an empty heartbeat rather than the entry.
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [], 0, 0)),

    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State0} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    {leader, State1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node4, 0, true, 1, 0)),
    {leader, State2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node5, 0, true, 1, 0)),
    % Ensure that regular replica and witness replica have same commit index
    ?assertEqual(1, State0#raft_state.commit_index),
    ?assertEqual(1, State1#raft_state.commit_index),
    ?assertEqual(1, State2#raft_state.commit_index),

    % Add another commit
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {refB1, noop}, high)),
    {leader, State4} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(1, State4#raft_state.commit_index),

    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refB1, noop}}], 1, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refB1, noop}}], 1, _)),
    % The highest index any follower has acknowledged is 1, so the new entry at
    % index 2 is capped out and witnesses receive an empty heartbeat.
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [], 1, _)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [], 1, _)),
    {leader, State5} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 1)),
    ?assertEqual(1, State5#raft_state.commit_index),
    {leader, State6} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(1, State6#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 2, 1, [], 1, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 2, 1, [], 1, _)),
    % node2 has now acknowledged index 2, raising the max follower match index
    % to 2, so the witnesses may finally hold that entry -- still one behind the
    % full replicas, whose next index already advanced past it.
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refB1, noop}}], 1, _)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refB1, noop}}], 1, _)),

    % Stop server
    ok = server_stop().

-spec commit_two_witness(Config :: ct_suite:ct_config()) -> term().
commit_two_witness(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server with two witnesses
    ConfigA = wa_raft_server:make_config(
        [
            #raft_identity{name = Name, node = Node},
            #raft_identity{name = Name, node = node2},
            #raft_identity{name = Name, node = node3}
        ],
        [
            #raft_identity{name = Name, node = node4},
            #raft_identity{name = Name, node = node5}
        ]
    ),
    {follower, _} = server_start_and_bootstrap(ConfigA, Config),
    {leader, StateA0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertEqual(0, StateA0#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, _, 0, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, _, 0, _)),
    {leader, StateA1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    ?assertEqual(1, StateA1#raft_state.commit_index),
    {leader, StateA2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, StateA2#raft_state.commit_index),
    {leader, StateA3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node4, 0, true, 1, 0)),
    ?assertEqual(1, StateA3#raft_state.commit_index),
    {leader, StateA4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node5, 0, true, 1, 0)),
    ?assertEqual(1, StateA4#raft_state.commit_index),

    % Add another commit
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {refA1, noop}, high)),
    {leader, StateA5} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(1, StateA5#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refA1, noop}}], 1, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refA1, noop}}], 1, _)),
    {leader, StateA6} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 1)),
    ?assertEqual(2, StateA6#raft_state.commit_index),
    {leader, StateA7} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, 2, 1)),
    ?assertEqual(2, StateA7#raft_state.commit_index),
    {leader, StateA8} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node4, 1, true, 2, 1)),
    ?assertEqual(2, StateA8#raft_state.commit_index),
    {leader, StateA9} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node5, 1, true, 2, 1)),
    ?assertEqual(2, StateA9#raft_state.commit_index),

    % Stop server
    ok = server_stop().

-spec commit_one_witness(Config :: ct_suite:ct_config()) -> term().
commit_one_witness(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Now switch to having 4 nodes and a witness
    %  * Since there are now 4 nodes, it requires three nodes to form a commit quorum.
    ConfigB = wa_raft_server:make_config(
        [
            #raft_identity{name = Name, node = Node},
            #raft_identity{name = Name, node = node2},
            #raft_identity{name = Name, node = node3},
            #raft_identity{name = Name, node = node4}
        ],
        [
            #raft_identity{name = Name, node = node5}
        ]
    ),
    {follower, _} = server_start_and_bootstrap(ConfigB, Config),
    {leader, StateB0, ok} = server_call(?PROMOTE_COMMAND(2, true)),
    ?assertEqual(0, StateB0#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, _)),
    {leader, StateB1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 0, true, 1, 0)),
    ?assertEqual(0, StateB1#raft_state.commit_index),
    {leader, StateB2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, StateB2#raft_state.commit_index),
    {leader, StateB3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node4, 0, true, 1, 0)),
    ?assertEqual(1, StateB3#raft_state.commit_index),
    {leader, StateB4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node5, 0, true, 1, 0)),
    ?assertEqual(1, StateB4#raft_state.commit_index),

    % Add another commit
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {refB1, noop}, high)),
    {leader, StateB5} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(1, StateB5#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 2, [{2, {refB1, noop}}], 1, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 2, [{2, {refB1, noop}}], 1, _)),
    {leader, StateB6} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 1, true, 2, 1)),
    ?assertEqual(1, StateB6#raft_state.commit_index),
    {leader, StateB7} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 1, true, 2, 1)),
    ?assertEqual(2, StateB7#raft_state.commit_index),
    {leader, StateB8} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node4, 1, true, 2, 1)),
    ?assertEqual(2, StateB8#raft_state.commit_index),
    {leader, StateB9} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node5, 1, true, 2, 1)),
    ?assertEqual(2, StateB9#raft_state.commit_index),

    % Stop server
    ok = server_stop().

%% Leader - commit quorum is based on config
-spec commit_three_members(Config :: ct_suite:ct_config()) -> term().
commit_three_members(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Promote the leader with only 3 nodes
    %  * Since there are only 3 nodes, it only requires two nodes to form a commit quorum.
    ConfigA = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3}
    ]),
    {follower, _} = server_start_and_bootstrap(ConfigA, Config),
    {leader, StateA0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertEqual(0, StateA0#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, _, 0, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, _, 0, _)),
    {leader, StateA1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    ?assertEqual(1, StateA1#raft_state.commit_index),
    {leader, StateA2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, StateA2#raft_state.commit_index),

    % Add another commit
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {refA1, noop}, high)),
    {leader, StateA3} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(1, StateA3#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refA1, noop}}], 1, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {refA1, noop}}], 1, _)),
    {leader, StateA4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 1)),
    ?assertEqual(2, StateA4#raft_state.commit_index),
    {leader, StateA5} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, 2, 1)),
    ?assertEqual(2, StateA5#raft_state.commit_index),

    % Stop server
    ok = server_stop().

%% Leader - commit quorum is based on config
-spec commit_four_members(Config :: ct_suite:ct_config()) -> term().
commit_four_members(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Now switch to having 4 nodes
    %  * Since there are now 4 nodes, it requires three nodes to form a commit quorum.
    ConfigB = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3},
        #raft_identity{name = Name, node = node4}
    ]),
    {follower, _} = server_start_and_bootstrap(ConfigB, Config),
    {leader, StateB0, ok} = server_call(?PROMOTE_COMMAND(2, true)),
    ?assertEqual(0, StateB0#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, 0, 0, _, 0, _)),
    {leader, StateB1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 0, true, 1, 0)),
    ?assertEqual(0, StateB1#raft_state.commit_index),
    {leader, StateB2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, StateB2#raft_state.commit_index),

    % Add another commit
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {refB1, noop}, high)),
    {leader, StateB3} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(1, StateB3#raft_state.commit_index),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 2, [{2, {refB1, noop}}], 1, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 2, [{2, {refB1, noop}}], 1, _)),
    {leader, StateB4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 1, true, 2, 1)),
    ?assertEqual(1, StateB4#raft_state.commit_index),
    {leader, StateB5} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 1, true, 2, 1)),
    ?assertEqual(2, StateB5#raft_state.commit_index),

    % Stop server
    ok = server_stop().

%% Candidate buffers commits during election, and those commits are
%% processed by the leader after winning the election.
-spec commit_candidate(Config :: ct_suite:ct_config()) -> ok.
commit_candidate(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Enable candidate buffering for this test
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_CANDIDATE_BUFFER_REQUESTS, true),

    % Setup server and promote to term 1 (noop at index 1)
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {follower, _, ok} = server_call(?RESIGN_COMMAND),

    % Trigger election to become candidate at term 2
    {candidate, _} = server_invoke(state_timeout, election),
    clear_message_queue(),

    % Buffer commits while in candidate state
    From0 = ?FROM(),
    {candidate, State0} = server_cast(?COMMIT_COMMAND(From0, {ref0, noop}, high)),
    ?assertEqual([{From0, {ref0, noop}}], State0#raft_state.pending_high),

    From1 = ?FROM(),
    {candidate, State1} = server_cast(?COMMIT_COMMAND(From1, {ref1, noop}, high)),
    ?assertEqual([{From1, {ref1, noop}}, {From0, {ref0, noop}}], State1#raft_state.pending_high),

    % Win election — quorum of votes triggers transition to leader.
    % On leader entry, buffered commits are processed during the initial
    % heartbeat (no explicit noop since has_pending_commits returns true).
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, node2, true)),
    {leader, State2} = server_cast(?VOTE_RPC(2, Name, node3, true)),

    % Pending commits should have been flushed into the log
    ?assertEqual([], State2#raft_state.pending_high),
    ?assertEqual({ok, {2, {ref0, noop}}}, wa_raft_log:get(State2#raft_state.log_view, 2)),
    ?assertEqual({ok, {2, {ref1, noop}}}, wa_raft_log:get(State2#raft_state.log_view, 3)),

    % Leader entry heartbeat should have replicated the entries
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 1, [{2, {ref0, noop}}, {2, {ref1, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 1, [{2, {ref0, noop}}, {2, {ref1, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 1, [{2, {ref0, noop}}, {2, {ref1, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(2, Name, Node, 1, 1, [{2, {ref0, noop}}, {2, {ref1, noop}}], 0, 0)),

    % Get quorum acks — commits should be applied
    clear_message_queue(),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 1, true, 3, 0)),
    {leader, State3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 1, true, 3, 0)),
    ?assertEqual(3, State3#raft_state.commit_index),
    ?assertApplyOp(From0, {2, {2, {ref0, undefined, noop}}}),
    ?assertApplyOp(From1, {3, {2, {ref1, undefined, noop}}}),

    ok = server_stop().

%% Buffered commits are cancelled with {error, not_leader} when the
%% candidate loses the election by receiving a heartbeat from a leader.
-spec commit_cancelled_candidate(Config :: ct_suite:ct_config()) -> ok.
commit_cancelled_candidate(Config) ->
    Name = ?SERVER_NAME(Config),

    % Enable candidate buffering for this test
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_CANDIDATE_BUFFER_REQUESTS, true),

    % Setup server and promote to term 1 (noop at index 1)
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {follower, _, ok} = server_call(?RESIGN_COMMAND),

    % Trigger election to become candidate at term 2
    {candidate, _} = server_invoke(state_timeout, election),
    clear_message_queue(),

    % Buffer commits while in candidate state
    Ref0 = make_ref(),
    {candidate, _} = server_cast(?COMMIT_COMMAND(?FROM(Ref0), {ref0, noop}, high)),
    Ref1 = make_ref(),
    {candidate, _} = server_cast(?COMMIT_COMMAND(?FROM(Ref1), {ref1, noop}, high)),

    % Receive heartbeat from a leader in a newer term — election fails
    {follower, State0} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual([], State0#raft_state.pending_high),
    ?assertEqual([], State0#raft_state.pending_low),

    % Callers should receive {error, not_leader}
    ?assertReceive({Ref0, {error, not_leader}}),
    ?assertReceive({Ref1, {error, not_leader}}),

    ok = server_stop().

% TODO T246543941 Extend read test to verify the flow when commit index >
% last applied index.
-spec read(Config :: ct_suite:ct_config()) -> ok.
read(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    Queues = ?SERVER_QUEUES(Config),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),

    % Get acks from followers
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State0} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State0#raft_state.commit_index),

    ?assert(Queues =/= undefined),

    ?assertEqual([], wa_raft_queue:query_reads(Queues, infinity)),
    ?assertEqual([], State0#raft_state.pending_high),
    ?assertNot(State0#raft_state.pending_read),
    Ref = make_ref(),

    ok = wa_raft_queue:reserve_read(Queues),
    {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

    ?assertMatch([{{1, _}, noop}], wa_raft_queue:query_reads(Queues, infinity)),
    ?assertEqual([], State1#raft_state.pending_high),
    ?assert(State1#raft_state.pending_read),

    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {?READ_OP, noop}}], 1, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {?READ_OP, noop}}], 1, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {?READ_OP, noop}}], 1, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, [{1, {?READ_OP, noop}}], 1, 0)),

    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 1)),
    {leader, State2} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, 2, 1)),
    ?assertEqual(2, State2#raft_state.commit_index),

    ?assertReceive({Ref, _}),

    % Stop server
    ok = server_stop().

%% Leader read_after clamps the queued read index to at least the caller's
%% MinIndex when it exceeds the effective commit floor, and leaves the floor
%% unchanged when MinIndex is at or below it.
-spec read_after(Config :: ct_suite:ct_config()) -> ok.
read_after(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    Queues = ?SERVER_QUEUES(Config),

    % Setup server and promote to term 1
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),

    % Reach commit_index = 1 so the base floor is max(1, FirstLogIndex) = 1
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State0} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State0#raft_state.commit_index),
    ?assert(Queues =/= undefined),

    % MinIndex above the base floor: read is queued at MinIndex.
    RefHigh = make_ref(),
    ok = wa_raft_queue:reserve_read(Queues),
    {leader, _} = server_cast(?READ_COMMAND(?FROM(RefHigh), read_high, 5)),
    ?assertMatch([{{5, _}, read_high}], wa_raft_queue:query_reads(Queues, infinity)),

    % MinIndex below the base floor: read is queued at the floor.
    RefLow = make_ref(),
    ok = wa_raft_queue:reserve_read(Queues),
    {leader, _} = server_cast(?READ_COMMAND(?FROM(RefLow), read_low, 0)),
    Reads = wa_raft_queue:query_reads(Queues, infinity),
    ?assertEqual(2, length(Reads)),
    ?assert(lists:any(fun({{I, _}, C}) -> I =:= 1 andalso C =:= read_low end, Reads)),
    ?assert(lists:any(fun({{I, _}, C}) -> I =:= 5 andalso C =:= read_high end, Reads)),

    ok = server_stop().

%% Candidate submits reads to the storage queue during election,
%% and those reads are served after winning and establishing quorum.
-spec read_candidate(Config :: ct_suite:ct_config()) -> ok.
read_candidate(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    Queues = ?SERVER_QUEUES(Config),

    % Enable candidate buffering for this test
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_CANDIDATE_BUFFER_REQUESTS, true),

    % Setup server and promote to term 1 (noop at index 1)
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),

    % Get acks from followers to establish quorum in term 1
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State0} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State0#raft_state.commit_index),

    % Resign and trigger election
    {follower, _, ok} = server_call(?RESIGN_COMMAND),
    {candidate, _} = server_invoke(state_timeout, election),
    clear_message_queue(),

    % Submit reads while in candidate state (ReadIndex = last_index + 1 = 2)
    ?assert(Queues =/= undefined),
    Ref = make_ref(),
    ok = wa_raft_queue:reserve_read(Queues),
    {candidate, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),
    ?assert(State1#raft_state.pending_read),
    ?assertMatch([{{2, _}, noop}], wa_raft_queue:query_reads(Queues, infinity)),

    % Win election — candidate transitions to leader
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, node2, true)),
    {leader, State2} = server_cast(?VOTE_RPC(2, Name, node3, true)),

    % After leader entry, the pending read should have produced a read
    % noop entry (in addition to the noop for the new term).
    ?assertNot(State2#raft_state.pending_read),

    % Get quorum acks to commit the entries in the new term
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 1, true, 3, 1)),
    {leader, State3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 1, true, 3, 1)),
    ?assertEqual(3, State3#raft_state.commit_index),

    % The read should now be fulfilled
    ?assertReceive({Ref, _}),

    ok = server_stop().

%% Candidate read_after clamps the buffered read index to at least the caller's
%% MinIndex when it exceeds last_index + 1, and leaves that base floor
%% unchanged when MinIndex is at or below it.
-spec read_after_candidate(Config :: ct_suite:ct_config()) -> ok.
read_after_candidate(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    Queues = ?SERVER_QUEUES(Config),

    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_CANDIDATE_BUFFER_REQUESTS, true),

    % Setup server and promote to term 1 (noop at index 1)
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),

    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State0} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State0#raft_state.commit_index),

    % Resign and trigger an election so we're in candidate state with
    % last_index + 1 = 2 as the base floor.
    {follower, _, ok} = server_call(?RESIGN_COMMAND),
    {candidate, _} = server_invoke(state_timeout, election),
    clear_message_queue(),
    ?assert(Queues =/= undefined),

    % MinIndex above the base floor: read is buffered at MinIndex.
    RefHigh = make_ref(),
    ok = wa_raft_queue:reserve_read(Queues),
    {candidate, StateHigh} = server_cast(?READ_COMMAND(?FROM(RefHigh), read_high, 5)),
    ?assert(StateHigh#raft_state.pending_read),
    ?assertMatch([{{5, _}, read_high}], wa_raft_queue:query_reads(Queues, infinity)),

    % MinIndex at/below the base floor: read is buffered at the floor (2).
    RefLow = make_ref(),
    ok = wa_raft_queue:reserve_read(Queues),
    {candidate, _} = server_cast(?READ_COMMAND(?FROM(RefLow), read_low, 1)),
    Reads = wa_raft_queue:query_reads(Queues, infinity),
    ?assertEqual(2, length(Reads)),
    ?assert(lists:any(fun({{I, _}, C}) -> I =:= 2 andalso C =:= read_low end, Reads)),
    ?assert(lists:any(fun({{I, _}, C}) -> I =:= 5 andalso C =:= read_high end, Reads)),

    ok = server_stop().

%% Establishes a healthy leader with quorum and returns the leader state
%% along with the current term.
-spec setup_leader_with_quorum(Config :: ct_suite:ct_config()) -> {atom(), Name :: atom(), Term :: wa_raft_log:log_term(), #raft_state{}}.
setup_leader_with_quorum(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),

    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State#raft_state.commit_index),
    ?assertNotEqual(undefined, State#raft_state.leader_quorum_ts),
    clear_message_queue(),
    {leader, Name, 1, State}.

%% Set the leader read lease knob for the current testcase and ensure it is
%% reset on testcase end. Callers must call `application:unset_env/2`
%% themselves at the end of the testcase if they want to be tidy — the test
%% helper resets the environment between suites.
-spec set_lease_ms(LeaseMs :: non_neg_integer()) -> ok.
set_lease_ms(LeaseMs) ->
    application:set_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS, LeaseMs).

%% Healthy leader with lease enabled serves reads via `apply_read` without
%% appending a new log entry.
-spec read_lease_hit(Config :: ct_suite:ct_config()) -> ok.
read_lease_hit(Config) ->
    ok = set_lease_ms(1000),
    try
        {leader, Name, Term, State0} = setup_leader_with_quorum(Config),
        Queues = ?SERVER_QUEUES(Config),
        ?assert(Queues =/= undefined),
        LastIndex = wa_raft_log:last_index(State0#raft_state.log_view),

        Ref = make_ref(),
        ok = wa_raft_queue:reserve_read(Queues),
        ?assertEqual(1, wa_raft_queue:read_queue_size(Queues)),
        {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

        %% Lease-hit path bypasses submit_read and pending_read.
        ?assertNot(State1#raft_state.pending_read),
        ?assertEqual([], wa_raft_queue:query_reads(Queues, infinity)),

        %% No new log entry appended by the read.
        ?assertEqual(LastIndex, wa_raft_log:last_index(State1#raft_state.log_view)),

        %% Storage answered the read synchronously.
        ?assertReceive({Ref, ok}),

        %% The lease-hit path must release the reserved read-queue slot;
        %% otherwise every hit permanently leaks a slot and after
        %% `RAFT_MAX_PENDING_READS` hits all strong reads on the table
        %% would be rejected with `read_queue_full`.
        ?assertEqual(0, wa_raft_queue:read_queue_size(Queues)),

        %% Sanity: no APPEND_ENTRIES was cast for the read (nothing new to
        %% replicate — the read never touched the log).
        ?assertNotCast(Name, _, ?APPEND_ENTRIES_RPC(Term, Name, _, _, _, [_ | _], _, _)),

        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% With lease disabled (default), behavior is byte-identical to today — every
%% read goes through the submit_read path and produces a noop log entry.
-spec read_lease_miss_disabled(Config :: ct_suite:ct_config()) -> ok.
read_lease_miss_disabled(Config) ->
    application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS),
    {leader, _Name, _Term, _State0} = setup_leader_with_quorum(Config),
    Queues = ?SERVER_QUEUES(Config),
    ?assert(Queues =/= undefined),

    Ref = make_ref(),
    ok = wa_raft_queue:reserve_read(Queues),
    {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

    ?assert(State1#raft_state.pending_read),
    ?assertMatch([{{1, _}, noop}], wa_raft_queue:query_reads(Queues, infinity)),
    ok = server_stop().

%% After lease elapses (leader_quorum_ts is far in the past), read falls back to
%% submit_read.
-spec read_lease_miss_stale(Config :: ct_suite:ct_config()) -> ok.
read_lease_miss_stale(Config) ->
    ok = set_lease_ms(100),
    try
        {leader, _Name, _Term, _State0} = setup_leader_with_quorum(Config),
        Queues = ?SERVER_QUEUES(Config),
        ?assert(Queues =/= undefined),

        %% Force leader_quorum_ts far into the past so the lease is stale.
        StaleTs = erlang:monotonic_time(millisecond) - 10_000,
        server_replace_state(
            fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = StaleTs}} end
        ),

        Ref = make_ref(),
        ok = wa_raft_queue:reserve_read(Queues),
        {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

        ?assert(State1#raft_state.pending_read),
        ?assertMatch([{{1, _}, noop}], wa_raft_queue:query_reads(Queues, infinity)),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% Freshly-elected leader before its current-term noop commits — lease is
%% suppressed until the current-term noop has been committed.
-spec read_lease_miss_not_current_term(Config :: ct_suite:ct_config()) -> ok.
read_lease_miss_not_current_term(Config) ->
    ok = set_lease_ms(1000),
    try
        Name = ?SERVER_NAME(Config),
        Queues = ?SERVER_QUEUES(Config),

        {follower, _} = server_start_and_bootstrap(Config),
        {leader, State0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
        ?assert(State0#raft_state.commit_index < State0#raft_state.first_current_term_log_index),
        ?assert(Queues =/= undefined),

        %% Manually plant a leader_quorum_ts even though no quorum has been
        %% established on the current term's noop, to make sure the
        %% current-term gate — not the quorum-ts gate — is what suppresses
        %% the lease here.
        Now = erlang:monotonic_time(millisecond),
        server_replace_state(
            fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = Now}} end
        ),

        Ref = make_ref(),
        ok = wa_raft_queue:reserve_read(Queues),
        {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

        ?assert(State1#raft_state.pending_read),
        ?assertNotEqual(undefined, Name),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% Leader with `leader_quorum_ts = undefined` (never established quorum) —
%% lease suppressed.
-spec read_lease_miss_no_quorum_ts(Config :: ct_suite:ct_config()) -> ok.
read_lease_miss_no_quorum_ts(Config) ->
    ok = set_lease_ms(1000),
    try
        {leader, _Name, _Term, _State0} = setup_leader_with_quorum(Config),
        Queues = ?SERVER_QUEUES(Config),
        ?assert(Queues =/= undefined),

        server_replace_state(
            fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = undefined}} end
        ),

        Ref = make_ref(),
        ok = wa_raft_queue:reserve_read(Queues),
        {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

        ?assert(State1#raft_state.pending_read),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% Read arriving at an index past `last_applied` cannot be served by the
%% lease — falls back to submit_read.
-spec read_lease_miss_not_applied(Config :: ct_suite:ct_config()) -> ok.
read_lease_miss_not_applied(Config) ->
    ok = set_lease_ms(1000),
    try
        {leader, _Name, _Term, State0} = setup_leader_with_quorum(Config),
        Queues = ?SERVER_QUEUES(Config),
        ?assert(Queues =/= undefined),

        %% Force last_applied behind commit_index to simulate a slow apply.
        server_replace_state(
            fun ({SN, S}) -> {SN, S#raft_state{last_applied = 0}} end
        ),
        ?assertEqual(1, State0#raft_state.commit_index),

        Ref = make_ref(),
        ok = wa_raft_queue:reserve_read(Queues),
        {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

        ?assert(State1#raft_state.pending_read),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% Handover in progress — reads are already rejected by the earlier clause,
%% but even if we manually set only `handover_lease_state = in_progress`
%% without setting `handover`, the lease must still be suppressed.
-spec read_lease_miss_handover_in_progress(Config :: ct_suite:ct_config()) -> ok.
read_lease_miss_handover_in_progress(Config) ->
    ok = set_lease_ms(1000),
    try
        {leader, _Name, _Term, _State0} = setup_leader_with_quorum(Config),
        Queues = ?SERVER_QUEUES(Config),
        ?assert(Queues =/= undefined),

        server_replace_state(
            fun ({SN, S}) -> {SN, S#raft_state{handover_lease_state = in_progress}} end
        ),

        Ref = make_ref(),
        ok = wa_raft_queue:reserve_read(Queues),
        {leader, State1} = server_cast(?READ_COMMAND(?FROM(Ref), noop, undefined)),

        ?assert(State1#raft_state.pending_read),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% After a HANDOVER_FAILED, the lease state resets to `unaffected`
%% immediately (no need to wait for a fresh quorum round).
-spec read_lease_handover_failed_rearms(Config :: ct_suite:ct_config()) -> ok.
read_lease_handover_failed_rearms(Config) ->
    ok = set_lease_ms(1000),
    try
        {leader, Name, Term, _State0} = setup_leader_with_quorum(Config),

        %% Initiate handover.
        {leader, #raft_state{handover = {node2, Ref, _}} = State1, {ok, node2}} =
            server_call(?HANDOVER_COMMAND(node2)),
        ?assertEqual(in_progress, State1#raft_state.handover_lease_state),

        %% HANDOVER_FAILED clears both handover and the lease state.
        {leader, State2} = server_cast(?HANDOVER_FAILED_RPC(Term, Name, node2, Ref)),
        ?assertEqual(undefined, State2#raft_state.handover),
        ?assertEqual(unaffected, State2#raft_state.handover_lease_state),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

%% After a handover timeout, the lease state transitions to
%% `pending_reconfirmation` and `heartbeat_reply_ts` is cleared, so the
%% lease stays disabled until a fresh post-timeout quorum round completes,
%% at which point `update_quorum_ts` moves the state back to `unaffected`.
-spec read_lease_handover_timeout_rearms(Config :: ct_suite:ct_config()) -> ok.
read_lease_handover_timeout_rearms(Config) ->
    ok = set_lease_ms(1000),
    try
        Name = ?SERVER_NAME(Config),
        {leader, Name, Term, _State0} = setup_leader_with_quorum(Config),

        %% Initiate handover, then force its timeout to be in the past.
        {leader, #raft_state{handover = {node2, Ref, _}} = State1, {ok, node2}} =
            server_call(?HANDOVER_COMMAND(node2)),
        ?assertEqual(in_progress, State1#raft_state.handover_lease_state),
        server_replace_state(
            fun ({SN, S}) ->
                {SN, S#raft_state{
                    handover = {node2, Ref, erlang:monotonic_time(millisecond) - 1_000_000}
                }}
            end
        ),

        %% Firing the state timeout observes the expired handover and
        %% transitions the lease state to pending_reconfirmation, clearing
        %% heartbeat_reply_ts to force a fresh quorum round.
        clear_message_queue(),
        {leader, State2} = server_invoke(state_timeout, heartbeat),
        ?assertEqual(undefined, State2#raft_state.handover),
        ?assertEqual(pending_reconfirmation, State2#raft_state.handover_lease_state),
        ?assertEqual(#{}, State2#raft_state.heartbeat_reply_ts),

        %% A fresh quorum of heartbeat replies flips the state back to
        %% unaffected.
        {leader, _} =
            server_cast(?APPEND_ENTRIES_RESPONSE_RPC(Term, Name, node2, 1, true, 1, 1)),
        {leader, State3} =
            server_cast(?APPEND_ENTRIES_RESPONSE_RPC(Term, Name, node3, 1, true, 1, 1)),
        ?assertEqual(unaffected, State3#raft_state.handover_lease_state),
        ?assertNotEqual(undefined, State3#raft_state.leader_quorum_ts),
        ok = server_stop()
    after
        application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_READ_LEASE_MS)
    end.

-spec truncate(Config :: ct_suite:ct_config()) -> ok.
truncate(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup server at 100:2
    {follower, _} = server_start_and_bootstrap(100, 2, Config),

    % Follower gets heartbeat at 100:2 with 101 ~ 103 (commit 101)
    %  * Follower should successfully append log entries
    %  * Follower should apply log entry 101
    %  * Heartbeat response is generated before apply, so last applied index is 100
    {follower, State0} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 100, 2, [{2, {101, noop}}, {2, {102, noop}}, {2, {103, noop}}], 101, 0)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(2, Name, Node, 100, true, 103, 100)),
    ?assertEqual(103, wa_raft_log:last_index(State0#raft_state.log_view)), % current last log entry is 103
    ?assertEqual(101, State0#raft_state.last_applied), % last applied is 101

    % Follower gets a heartbeat from the new leader for term 3
    %  * The term for log entries 102 and 103 mismatch so they should
    %    be overwritten
    {follower, State1} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node3, 101, 2, [{3, {102, noop}}, {3, {103, noop}}, {3, {104, noop}}], 101, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 101, true, 104, 101)),
    ?assertEqual(104, wa_raft_log:last_index(State1#raft_state.log_view)),
    ?assertEqual({ok, {2, {101, noop}}}, wa_raft_log:get(State1#raft_state.log_view, 101)), % entry 101 is kept
    ?assertEqual({ok, {3, {102, noop}}}, wa_raft_log:get(State1#raft_state.log_view, 102)), % entry 102 is replaced
    ?assertEqual({ok, {3, {103, noop}}}, wa_raft_log:get(State1#raft_state.log_view, 103)), % entry 103 is replaced
    ?assertEqual({ok, {3, {104, noop}}}, wa_raft_log:get(State1#raft_state.log_view, 104)), % entry 104 is replaced

    % Follower gets a heartbeat from the new leader for term 4
    %  * The term for log entry 102 (3) does not match the term (4) provided
    %    for the previous log index. The log should be truncated past 102
    {follower, State2} = server_cast(?APPEND_ENTRIES_RPC(4, Name, node2, 102, 4, [{4, {103, noop}}, {4, {104, noop}}], 101, 0)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(4, Name, Node, 102, false, 101, 101)),
    ?assertEqual(101, wa_raft_log:last_index(State2#raft_state.log_view)), % new entries are not appended
    ?assertEqual({ok, {2, {101, noop}}}, wa_raft_log:get(State2#raft_state.log_view, 101)), % entry 101 is unchanged
    ?assertEqual(not_found, wa_raft_log:get(State2#raft_state.log_view, 102)), % entry 102 is truncated
    ?assertEqual(not_found, wa_raft_log:get(State2#raft_state.log_view, 103)), % entry 103 is truncated
    ?assertEqual(not_found, wa_raft_log:get(State2#raft_state.log_view, 104)), % entry 104 is truncated

    % Stop server
    ok = server_stop().

-spec update_info(Config :: ct_suite:ct_config()) -> ok.
update_info(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),

    % Setup server
    {follower, _} = server_start_and_bootstrap(100, 2, Config),

    % Test that follower identifies and updates leader properly.
    %  * Follower sets leader when getting heartbeat
    %  * Follower clears leader when advancing term
    {follower, StateA0} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateA0#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({2, node2, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {follower, StateA1} = server_cast(?NOTIFY_TERM_RPC(3, Name, node3)),
    ?assertEqual(undefined, StateA1#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({3, undefined, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {follower, StateA2} = server_cast(?APPEND_ENTRIES_RPC(4, Name, node4, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node4}, StateA2#raft_state.leader),
    ?assertEqual(node4, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({4, node4, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),

    % Test that candidate identifies and updates leader
    %  * Candidate clears leader and publishes self as candidate when an election starts
    %  * Candidate switches to follower and sets leader on heartbeat
    {follower, StateB0} = server_cast(?APPEND_ENTRIES_RPC(6, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateB0#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    {candidate, StateB1} = server_invoke(state_timeout, election),
    ?assertEqual(undefined, StateB1#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({7, undefined, Node}, wa_raft_info:get_current_term_info(Table, Partition)),
    {follower, StateB2} = server_cast(?APPEND_ENTRIES_RPC(7, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateB2#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({7, node2, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),

    % Test that leader sets itself as leader
    %  * Leader sets self as leader and clears candidate when election is complete
    %  * Leader sets self as leader when promoted
    %  * Leader should clear leader when resigning
    %  * Leader should keep leader when promoting to self
    {candidate, StateC0} = server_invoke(state_timeout, election),
    ?assertEqual(undefined, StateC0#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({8, undefined, Node}, wa_raft_info:get_current_term_info(Table, Partition)),
    {candidate, _} = server_cast(?VOTE_RPC(8, Name, node(), true)),
    {candidate, _} = server_cast(?VOTE_RPC(8, Name, node2, true)),
    {leader, StateC1} = server_cast(?VOTE_RPC(8, Name, node3, true)),
    ?assertEqual(#raft_identity{name = Name, node = node()}, StateC1#raft_state.leader),
    ?assertEqual(node(), wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({8, Node, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {leader, StateC2, ok} = server_call(?PROMOTE_COMMAND(9, true)),
    ?assertEqual(#raft_identity{name = Name, node = node()}, StateC2#raft_state.leader),
    ?assertEqual(node(), wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({9, Node, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {follower, StateC3, ok} = server_call(?RESIGN_COMMAND),
    ?assertEqual(undefined, StateC3#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({9, undefined, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),

    % Test that disabled identifies and updates leader
    %  * Disabled clears leader on initial entry from leader
    %  * Disabled clears leader on term advance
    %  * Disabled sets leader when getting first heartbeat
    %  * Reenabling does not clear leader on initial entry
    {candidate, _} = server_invoke(state_timeout, election),
    {candidate, _} = server_cast(?VOTE_RPC(10, Name, node(), true)),
    {candidate, _} = server_cast(?VOTE_RPC(10, Name, node2, true)),
    {leader, _} = server_cast(?VOTE_RPC(10, Name, node3, true)),
    {disabled, StateD0, ok} = server_call(?DISABLE_COMMAND("Test")),
    ?assertEqual(undefined, StateD0#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({10, undefined, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {disabled, StateD1} = server_cast(?NOTIFY_TERM_RPC(11, Name, node3)),
    ?assertEqual(undefined, StateD1#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({11, undefined, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {disabled, StateD2} = server_cast(?APPEND_ENTRIES_RPC(11, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateD2#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({11, node2, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {stalled, StateD3, ok} = server_call(?ENABLE_COMMAND),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateD3#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({11, node2, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),

    % Test that stalled identifies and updates leader properly.
    %  * Stalled clears leader on term advance
    %  * Stalled sets leader when getting first heartbeat
    %  * Disabled does not clear leader on initial entry from non-leader
    {stalled, StateF0} = server_cast(?APPEND_ENTRIES_RPC(11, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateF0#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    {stalled, StateF1} = server_cast(?NOTIFY_TERM_RPC(12, Name, node3)),
    ?assertEqual(undefined, StateF1#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({12, undefined, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {stalled, StateF2} = server_cast(?APPEND_ENTRIES_RPC(12, Name, node4, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node4}, StateF2#raft_state.leader),
    ?assertEqual(node4, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({12, node4, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {disabled, StateF3, ok} = server_call(?DISABLE_COMMAND("Test")),
    ?assertEqual(#raft_identity{name = Name, node = node4}, StateF3#raft_state.leader),
    ?assertEqual(node4, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({12, node4, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),

    % Stop server
    ok = server_stop().

%% Test that witness identifies and updates leader and info properly
-spec update_info_witness(Config :: ct_suite:ct_config()) -> ok.
update_info_witness(Config) ->
    Name = ?SERVER_NAME(Config),
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),

    {witness, _} = server_start_witness_and_bootstrap(Config),

    {witness, StateA0} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node2}, StateA0#raft_state.leader),
    ?assertEqual(node2, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({2, node2, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {witness, StateA1} = server_cast(?NOTIFY_TERM_RPC(3, Name, node3)),
    ?assertEqual(undefined, StateA1#raft_state.leader),
    ?assertEqual(undefined, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({3, undefined, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),
    {witness, StateA2} = server_cast(?APPEND_ENTRIES_RPC(4, Name, node4, 0, 0, [], 0, 0)),
    ?assertEqual(#raft_identity{name = Name, node = node4}, StateA2#raft_state.leader),
    ?assertEqual(node4, wa_raft_info:get_leader(Table, Partition)),
    ?assertEqual({4, node4, undefined}, wa_raft_info:get_current_term_info(Table, Partition)),

    ok = server_stop().

-spec promote(Config :: ct_suite:ct_config()) -> ok.
promote(Config) ->
    % Setup server
    {stalled, _} = server_start(Config),

    % Stalled cannot be promoted, bootstrap is required
    {stalled, State0, {error, invalid_state}} = server_call(?PROMOTE_COMMAND(5, true)),
    ?assertEqual(0, State0#raft_state.current_term),

    % Follower can be force promoted in the current term if leader is unknown
    {follower, _, ok} = server_call(?BOOTSTRAP_COMMAND(#raft_log_pos{index = 1, term = 5}, ?SERVER_CLUSTER_CONFIG(Config), #{})),
    {leader, State1, ok} = server_call(?PROMOTE_COMMAND(5, true)),
    ?assertEqual(5, State1#raft_state.current_term),

    % Promotion should be disallowed if the node has recently gotten (or sent) a heartbeat
    server_replace_state(fun ({SN, S}) -> {SN, S#raft_state{leader_commit_index_ts = erlang:monotonic_time(millisecond)}} end),
    {leader, State2, {error, rejected}} = server_call(?PROMOTE_COMMAND(10, false)),
    ?assertEqual(5, State2#raft_state.current_term),

    % Reset heartbeat info and try to promote again. Clear both freshness
    % fields since the `?PROMOTE_COMMAND` gate reads their max.
    server_replace_state(fun ({State, Data}) -> {State, Data#raft_state{leader_quorum_ts = undefined, leader_commit_index_ts = undefined}} end),

    % Promotion must be to newer term when leader is known
    {leader, State3, {error, invalid_term}} = server_call(?PROMOTE_COMMAND(5, false)),
    ?assertEqual(5, State3#raft_state.current_term),

    % Promotion should be allowed if the node has not recently received (or sent) a heartbeat
    {leader, State4, ok} = server_call(?PROMOTE_COMMAND(15, false)),
    ?assertEqual(15, State4#raft_state.current_term),

    % Force promotion must be to newer term
    {leader, State5, {error, invalid_term}} = server_call(?PROMOTE_COMMAND(15, true)),
    ?assertEqual(15, State5#raft_state.current_term),

    % Force promotion ignores heartbeat
    {leader, State6, ok} = server_call(?PROMOTE_COMMAND(20, true)),
    ?assertEqual(20, State6#raft_state.current_term),

    % Stalled after enable still cannot be promoted, bootstrap or snapshot is required
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test")),
    {stalled, _, ok} = server_call(?ENABLE_COMMAND),
    {stalled, State7, {error, invalid_state}} = server_call(?PROMOTE_COMMAND(25, false)),
    ?assertEqual(20, State7#raft_state.current_term),

    % Stop server
    ok = server_stop().

-spec promote_witness(Config :: ct_suite:ct_config()) -> ok.
promote_witness(Config) ->
    % Setup witness server with config
    {witness, State8} = server_start_witness_and_bootstrap(100, 20, Config),
    ?assertEqual(20, State8#raft_state.current_term),

    % Witness cannot be promoted
    {witness, State9, {error, invalid_state}} = server_call(?PROMOTE_COMMAND(30, false)),
    ?assertEqual(20, State9#raft_state.current_term),

    % Witness cannot be force-promoted
    {witness, State10, {error, invalid_state}} = server_call(?PROMOTE_COMMAND(35, true)),
    ?assertEqual(20, State10#raft_state.current_term),

    ok = server_stop().

-spec resign(Config :: ct_suite:ct_config()) -> ok.
resign(Config) ->
    % Setup server
    {follower, _} = server_start_and_bootstrap(Config),

    % Leader should transition to follower upon resigning
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {follower, _, ok} = server_call(?RESIGN_COMMAND),

    % Follower cannot resign
    ?assertMatch({follower, _, {error, not_leader}}, server_call(?RESIGN_COMMAND)),

    % Candidate cannot resign
    {candidate, _} = server_invoke(state_timeout, election),
    ?assertMatch({candidate, _, {error, not_leader}}, server_call(?RESIGN_COMMAND)),

    % Disabled cannot resign
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test")),
    ?assertMatch({disabled, _, {error, not_leader}}, server_call(?RESIGN_COMMAND)),

    % Stalled cannot resign
    {stalled, _, ok} = server_call(?ENABLE_COMMAND),
    ?assertMatch({stalled, _, {error, not_leader}}, server_call(?RESIGN_COMMAND)),

    % Stop server
    ok = server_stop(),

    % Witness cannot resign
    {witness, _} = server_start_witness_and_bootstrap(Config),
    ?assertMatch({witness, _, {error, not_leader}}, server_call(?RESIGN_COMMAND)),

    ok = server_stop().

-spec witness(Config :: ct_suite:ct_config()) -> ok.
witness(Config) ->
   % Node = node(),

    % Start server
    {witness, _State0} = server_start_witness_and_bootstrap(Config),

    ok = server_stop().

-spec disable(Config :: ct_suite:ct_config()) -> ok.
disable(Config) ->
    Name = ?SERVER_NAME(Config),

    % Setup server
    {stalled, _} = server_start(Config),

    % Test stalled -> disabled transition
    {disabled, State0, ok} = server_call(?DISABLE_COMMAND("Test disable.")),
    ?assertEqual("Test disable.", State0#raft_state.disable_reason),

    % Disabled servers will still update term and handle AppendEntries RPCs
    % to update leader but should not respond to them
    ok = clear_message_queue(),
    {disabled, State1} = server_cast(?APPEND_ENTRIES_RPC(2, Name, node2, 20, 2, [], 20, 0)),
    ?assertEqual(2, State1#raft_state.current_term),
    ?assertEqual(#raft_identity{name = Name, node = node2}, State1#raft_state.leader),
    ?assertNotCast(_, _, 100),

    % Verify that RPCs are disabled in disabled state
    % Each API will test its disabled behavior individually
    ok = clear_message_queue(),
    ?assertEqual({disabled, State1}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 20, false, 10, 5))),
    ?assertEqual({disabled, State1}, server_cast(?REQUEST_VOTE_RPC(2, Name, node2, normal, 20, 2))),
    ?assertEqual({disabled, State1}, server_cast(?REQUEST_VOTE_RPC(2, Name, node2, force, 20, 2))),
    ?assertEqual({disabled, State1}, server_cast(?VOTE_RPC(2, Name, node2, true))),
    ?assertEqual({disabled, State1}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertEqual({disabled, State1}, server_cast(?HANDOVER_FAILED_RPC(2, Name, node2, make_ref()))),
    ?assertNotCast(_, _, 100),

    % Test disabled -> disabled transition
    {disabled, State2, ok} = server_call(?DISABLE_COMMAND("Test re-disable.")),
    ?assertEqual("Test re-disable.", State2#raft_state.disable_reason),

    % Verify that RPCs are still disabled
    ok = clear_message_queue(),
    ?assertEqual({disabled, State2}, server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 20, false, 10, 5))),
    ?assertEqual({disabled, State2}, server_cast(?REQUEST_VOTE_RPC(2, Name, node2, normal, 20, 2))),
    ?assertEqual({disabled, State2}, server_cast(?REQUEST_VOTE_RPC(2, Name, node2, force, 20, 2))),
    ?assertEqual({disabled, State2}, server_cast(?VOTE_RPC(2, Name, node2, true))),
    ?assertEqual({disabled, State2}, server_cast(?HANDOVER_RPC(2, Name, node2, make_ref(), 20, 2, []))),
    ?assertEqual({disabled, State2}, server_cast(?HANDOVER_FAILED_RPC(2, Name, node2, make_ref()))),
    ?assertNotCast(_, _, 100),

    % Test re-enable disabled -> stalled
    {stalled, State3, ok} = server_call(?ENABLE_COMMAND),
    ?assertEqual(undefined, State3#raft_state.disable_reason),

    % Test promotion rejected for stalled state
    {stalled, _, {error, invalid_state}} = server_call(?PROMOTE_COMMAND(3, true)),

    % Restart server while disabled
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test disable again.")),
    ok = server_stop(),
    {disabled, State4} = server_start(Config),
    ?assertEqual("Test disable again.", State4#raft_state.disable_reason),

    % Stop server
    ok = server_stop().

-spec disable_leader(Config :: ct_suite:ct_config()) -> ok.
disable_leader(Config) ->
    % Test leader -> disabled transition
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test disable.")),

    % Stop server
    ok = server_stop().

-spec disable_follower(Config :: ct_suite:ct_config()) -> ok.
disable_follower(Config) ->
    % Test follower -> disabled transition
    {follower, _} = server_start_and_bootstrap(Config),
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test disable.")),

    % Stop server
    ok = server_stop().

-spec disable_candidate(Config :: ct_suite:ct_config()) -> ok.
disable_candidate(Config) ->
    % Test candidate -> disabled transition
    {follower, _} = server_start_and_bootstrap(Config),
    {candidate, _, ok} = server_call(?TRIGGER_ELECTION_COMMAND(current)),
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test disable.")),

    % Stop server
    ok = server_stop().

-spec disable_witness(Config :: ct_suite:ct_config()) -> ok.
disable_witness(Config) ->

    % Test witness -> disabled transition
    {witness, _} = server_start_witness_and_bootstrap(Config),
    {disabled, State0, ok} = server_call(?DISABLE_COMMAND("Test disable.")),
    ?assertEqual("Test disable.", State0#raft_state.disable_reason),

    % Test re-enable disabled -> stalled
    {stalled, State3, ok} = server_call(?ENABLE_COMMAND),
    ?assertEqual(undefined, State3#raft_state.disable_reason),

    % Stop server
    ok = server_stop().

-spec handover(Config :: ct_suite:ct_config()) -> ok.
handover(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Start server
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, #raft_state{current_term = Term}, ok} = server_call(?PROMOTE_COMMAND(2, true)),

    % Attempt to handover to invalid node
    {leader, State1, {error, invalid_peer}} = server_call(?HANDOVER_COMMAND(notapeernode)),
    ?assertEqual(Term, State1#raft_state.current_term),
    ?assertEqual(undefined, State1#raft_state.handover),

    % Attempt to handover to self
    Self = node(),
    {leader, State2, {ok, Self}} = server_call(?HANDOVER_COMMAND(Self)),
    ?assertEqual(Term, State2#raft_state.current_term),
    ?assertEqual(undefined, State2#raft_state.handover),

    % Attempt to handover to valid peer node
    {leader, #raft_state{handover = {node2, Ref3, _} = Handover3} = State3, {ok, node2}} = server_call(?HANDOVER_COMMAND(node2)),
    ?assertEqual(Term, State3#raft_state.current_term),
    ?assertCast(Name, node2, ?HANDOVER_RPC(Term, Name, Node, Ref3, 0, 0, [{2, {_, noop}}])),

    % During handover, heartbeat replication is suppressed
    clear_message_queue(),
    {leader, State4} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(Handover3, State4#raft_state.handover),
    ?assertNotCast(_, _, ?APPEND_ENTRIES_RPC(_, Name, Node, _, _, _, _, _)),

    % Handover times out after some time when the peer node does not become leader and replication resumes.
    clear_message_queue(),
    server_replace_state(fun ({State, Data}) -> {State, Data#raft_state{handover = {node2, Ref3, erlang:monotonic_time(millisecond) - 1000000}}} end),
    {leader, State5} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(Term, State5#raft_state.current_term),
    ?assertEqual(undefined, State5#raft_state.handover),
    ?assertCast(Name, _, ?APPEND_ENTRIES_RPC(_, Name, Node, _, _, _, _, _)),

    % Attempt to handover but it is cancelled by a HandoverFailed
    %  * Only if we get a HandoverFailed RPC from the correct node with the
    %    correct tag (a reference) then actually cancel the handover.
    {leader, #raft_state{handover = {node2, Ref6, _} = Handover6}, {ok, node2}} = server_call(?HANDOVER_COMMAND(node2)),
    {leader, State7} = server_cast(?HANDOVER_FAILED_RPC(Term, Name, node2, make_ref())),
    ?assertEqual(Handover6, State7#raft_state.handover),
    {leader, State8} = server_cast(?HANDOVER_FAILED_RPC(Term, Name, node3, Ref6)),
    ?assertEqual(Handover6, State8#raft_state.handover),
    {leader, State9} = server_cast(?HANDOVER_FAILED_RPC(Term, Name, node2, Ref6)),
    ?assertEqual(undefined, State9#raft_state.handover),

    % During a handover, commit and read requests are immediately rejected
    % with notify_redirect so clients can redirect to the new leader.
    {leader, #raft_state{handover = {node2, Ref10, _}}, {ok, node2}} = server_call(?HANDOVER_COMMAND(node2)),

    Ref11 = make_ref(),
    clear_message_queue(),
    {leader, State11} = server_cast(?COMMIT_COMMAND(?FROM(Ref11), {ref11, noop}, high)),
    ?assertEqual([], State11#raft_state.pending_high),
    ?assertEqual(1, wa_raft_log:last_index(State11#raft_state.log_view)),
    ?assertReceive({Ref11, {error, {notify_redirect, node2}}}),

    Ref12 = make_ref(),
    clear_message_queue(),
    {leader, State12} = server_cast(?READ_COMMAND(?FROM(Ref12), noop, undefined)),
    ?assertEqual([], State12#raft_state.pending_high),
    ?assertNot(State12#raft_state.pending_read),
    ?assertEqual(1, wa_raft_log:last_index(State12#raft_state.log_view)),
    ?assertReceive({Ref12, {error, {notify_redirect, node2}}}),

    % Heartbeat during handover still suppresses replication
    {leader, State13} = server_invoke(state_timeout, heartbeat),
    ?assertEqual([], State13#raft_state.pending_high),
    ?assertEqual(1, wa_raft_log:last_index(State13#raft_state.log_view)),
    ?assertNotCast(_, _, ?APPEND_ENTRIES_RPC(_, Name, Node, _, _, _, _, _)),

    % If the handover fails, no pending commits/reads to handle (already rejected).
    {leader, State14} = server_cast(?HANDOVER_FAILED_RPC(Term, Name, node2, Ref10)),
    ?assertEqual(undefined, State14#raft_state.handover),
    ?assertEqual([], State14#raft_state.pending_high),
    ?assertNot(State14#raft_state.pending_read),
    ?assertEqual(1, wa_raft_log:last_index(State14#raft_state.log_view)),

    % Attempt a handover that succeeds (since we get a vote request from the peer)
    NextTerm = Term + 1,
    {leader, _, {ok, node2}} = server_call(?HANDOVER_COMMAND(node2)),

    Ref16A = make_ref(),
    Ref16B = make_ref(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(Ref16A), {ref13a, noop}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(Ref16B), {ref13b, noop}, high)),
    ?assertReceive({Ref16A, {error, {notify_redirect, node2}}}),
    ?assertReceive({Ref16B, {error, {notify_redirect, node2}}}),

    {follower, State16} = server_cast(?REQUEST_VOTE_RPC(NextTerm, Name, node2, force, 2, Term)),
    ?assertEqual(NextTerm, State16#raft_state.current_term),
    ?assertEqual(undefined, State16#raft_state.handover),
    ?assertCast(Name, node2, ?VOTE_RPC(NextTerm, Name, Node, true)),

    % Stop server
    ok = server_stop().

-spec handover_follower(Config :: ct_suite:ct_config()) -> ok.
handover_follower(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup follower
    {follower, _} = server_start_and_bootstrap(2, 2, Config),

    % Send rejected handover RPC
    Ref0 = make_ref(),
    {follower, State0} = server_cast(?HANDOVER_RPC(2, Name, node2, Ref0, 5, 5, [])),
    ?assertEqual(2, State0#raft_state.current_term),
    ?assertCast(Name, node2, ?HANDOVER_FAILED_RPC(2, Name, Node, Ref0)),

    % Send ok handover RPC
    %  * New candidate should send vote requests with type force
    Ref1 = make_ref(),
    {candidate, State1} = server_cast(?HANDOVER_RPC(2, Name, node2, Ref1, 2, 2, [{2, {ref, noop}}])),
    ?assertEqual(3, wa_raft_log:last_index(State1#raft_state.log_view)),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(3, Name, Node, force, _, _)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(3, Name, Node, force, _, _)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(3, Name, Node, force, _, _)),
    ?assertCast(Name, node5, ?REQUEST_VOTE_RPC(3, Name, Node, force, _, _)),

    % Stop server
    ok = server_stop().

-spec handover_witness(Config :: ct_suite:ct_config()) -> ok.
handover_witness(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup witness
    {witness, _} = server_start_witness_and_bootstrap(2, 2, Config),

    % Send rejected handover RPC
    Ref0 = make_ref(),
    {witness, State0} = server_cast(?HANDOVER_RPC(2, Name, node2, Ref0, 5, 5, [])),
    ?assertEqual(2, State0#raft_state.current_term),
    ?assertCast(Name, node2, ?HANDOVER_FAILED_RPC(2, Name, Node, Ref0)),

    ok = server_stop().

% Leader is handling requests to add a member to the cluster
-spec add_member(Config :: ct_suite:ct_config()) -> ok.
add_member(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup a leader with an uncommitted noop
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, State0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertEqual(1, wa_raft_log:last_index(State0#raft_state.log_view)),
    ?assertEqual(0, State0#raft_state.commit_index),
    ?assertEqual(0, State0#raft_state.last_applied),

    % Leaders should reject membership changes before they have replicated
    % a quorum in the current term.
    {leader, State0, {error, no_quorum}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node2}}, undefined)),

    % Let leader successfully replicate its initial noop to a quorum.
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State1#raft_state.commit_index),
    ?assertEqual(1, State1#raft_state.last_applied),

    % Membership operations with the wrong index are rejected
    {leader, State1, {error, config_index_mismatch}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, node6}}, 10)),

    % Leaders should not add duplicate members to the membership
    {leader, State1, {error, already_member}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, Node}}, undefined)),

    % Otherwise, since there is no pending reconfiguration, permit one.
    {leader, State2, {ok, #raft_log_pos{index = 2, term = 1}}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, node6}}, undefined)),
    ?assertMatch({ok, {1, {_, {config, #{membership := _}}}}}, wa_raft_log:get(State2#raft_state.log_view, 2)),

    % Since there is a pending reconfiguration, do not allow another.
    {leader, State2, {error, not_ready}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, node7}}, undefined)),

    % Leader should now replicate to an additional node
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node6, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),

    % Let leader form a quorum committing the config (with 6 nodes now)
    {leader, State3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node6, 1, true, 2, 1)),
    ?assertEqual(1, State3#raft_state.commit_index),
    ?assertEqual(1, State3#raft_state.last_applied),
    {leader, State4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node5, 1, true, 2, 1)),
    ?assertEqual(1, State4#raft_state.commit_index),
    ?assertEqual(1, State4#raft_state.last_applied),
    {leader, State5} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node4, 1, true, 2, 1)),
    ?assertEqual(2, State5#raft_state.commit_index),
    ?assertEqual(2, State5#raft_state.last_applied),

    % Do not allow adding the node again
    {leader, State5, {error, already_member}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, node6}}, undefined)),

    % Leader can now perform another reconfiguration
    {leader, State6, {ok, #raft_log_pos{index = 3, term = 1}}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, node7}}, undefined)),
    ?assertMatch({ok, {1, {_, {config, #{membership := _}}}}}, wa_raft_log:get(State6#raft_state.log_view, 3)),

    % Stop server
    ok = server_stop().

% Follower gets new configs adding members
-spec add_member_follower(Config :: ct_suite:ct_config()) -> ok.
add_member_follower(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup a follower
    {follower, _} = server_start_and_bootstrap(1, 1, Config),

    Node1 = #raft_identity{name = Name, node = node()},
    Node2 = #raft_identity{name = Name, node = node2},
    Node3 = #raft_identity{name = Name, node = node3},
    Node4 = #raft_identity{name = Name, node = node4},
    Node5 = #raft_identity{name = Name, node = node5},
    Node6 = #raft_identity{name = Name, node = node6},
    Node7 = #raft_identity{name = Name, node = node7},
    ConfigA = wa_raft_server:make_config([Node1, Node2, Node3, Node4, Node5, Node6]),
    ConfigB = wa_raft_server:make_config([Node1, Node2, Node3, Node4, Node5, Node6, Node7]),

    % Follower gets two new config entries each adding a member from the leader
    {follower, State0} = server_cast(?APPEND_ENTRIES_RPC(1, Name, node2, 1, 1, [{1, {ref, {config, ConfigA}}}, {1, {ref, {config, ConfigB}}}], 2, 0)),
    ?assertEqual(2, State0#raft_state.commit_index),
    ?assertEqual(2, State0#raft_state.last_applied),
    ?assertEqual(3, wa_raft_log:last_index(State0#raft_state.log_view)),
    ?assertEqual({ok, #raft_log_pos{index = 2, term = 1}, ConfigA}, wa_raft_storage:config(State0#raft_state.storage)),

    % Follower times out and starts a new election (now with 7 nodes)
    % Leader replicates to all nodes after being follower
    {candidate, _} = server_invoke(state_timeout, election),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, node6, true)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, node5, true)),
    clear_message_queue(),
    {leader, _} = server_cast(?VOTE_RPC(2, Name, node4, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node6, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node7, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),

    % Uncommitted config is removed by subsequent leader
    {follower, State1} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node3, 3, 3, [], 3, 0)),
    ?assertEqual(2, wa_raft_log:last_index(State1#raft_state.log_view)),

    % Follower times out and starts a new election (now with only 6 nodes because config was rolled back)
    clear_message_queue(),
    {candidate, _} = server_invoke(state_timeout, election),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertCast(Name, node5, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertCast(Name, node6, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertNotCast(Name, node7, ?REQUEST_VOTE_RPC(4, Name, Node, _, _, _)),
    {candidate, _} = server_cast(?VOTE_RPC(4, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(4, Name, node6, true)),
    {candidate, _} = server_cast(?VOTE_RPC(4, Name, node5, true)),
    {leader, _} = server_cast(?VOTE_RPC(4, Name, node4, true)),

    % Stop server
    ok = server_stop().

% Leader is handling requests to remove a member from the cluster
-spec remove_member(Config :: ct_suite:ct_config()) -> ok.
remove_member(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup a leader with an uncommitted noop
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, State0, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertEqual(1, wa_raft_log:last_index(State0#raft_state.log_view)),
    ?assertEqual(0, State0#raft_state.commit_index),
    ?assertEqual(0, State0#raft_state.last_applied),

    % Leaders should reject membership changes before they have replicated
    % a quorum in the current term.
    {leader, State0, {error, no_quorum}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node5}}, undefined)),

    % Let leader successfully replicate its initial noop to a quorum.
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, State1} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, State1#raft_state.commit_index),
    ?assertEqual(1, State1#raft_state.last_applied),

    % Leaders should not remove themselves from the membership
    {leader, State1, {error, cannot_remove_self}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, Node}}, undefined)),

    % Leaders should not remove a non-member from the membership
    {leader, State1, {error, not_a_participant}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node6}}, undefined)),

    % Otherwise, since there is no pending reconfiguration, permit one.
    {leader, State2, {ok, #raft_log_pos{index = 2, term = 1}}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node5}}, undefined)),
    ?assertMatch({ok, {1, {_, {config, #{membership := _}}}}}, wa_raft_log:get(State2#raft_state.log_view, 2)),

    % Since there is a pending reconfiguration, do not allow another.
    {leader, State2, {error, not_ready}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node4}}, undefined)),

    % Leader should now replicate to an additional node
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),
    ?assertNotCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),

    % Let leader form a quorum committing the config (with 4 nodes now)
    {leader, State3} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, 2, 1)),
    ?assertEqual(1, State3#raft_state.commit_index),
    ?assertEqual(1, State3#raft_state.last_applied),
    {leader, State4} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, 2, 1)),
    ?assertEqual(2, State4#raft_state.commit_index),
    ?assertEqual(2, State4#raft_state.last_applied),

    % Do not allow removing the node again
    {leader, State4, {error, not_a_participant}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node5}}, undefined)),

    % Leader can now perform another reconfiguration
    {leader, State5, {ok, #raft_log_pos{index = 3, term = 1}}} = server_call(?ADJUST_CONFIG_COMMAND({remove, {Name, node4}}, undefined)),
    ?assertMatch({ok, {1, {_, {config, #{membership := _}}}}}, wa_raft_log:get(State5#raft_state.log_view, 3)),

    % Stop server
    ok = server_stop().

% Follower gets new configs removing members
-spec remove_member_follower(Config :: ct_suite:ct_config()) -> ok.
remove_member_follower(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Setup a follower
    {follower, _} = server_start_and_bootstrap(1, 1, Config),

    Node1 = #raft_identity{name = Name, node = node()},
    Node2 = #raft_identity{name = Name, node = node2},
    Node3 = #raft_identity{name = Name, node = node3},
    Node4 = #raft_identity{name = Name, node = node4},
    ConfigA = wa_raft_server:make_config([Node1, Node2, Node3, Node4]),
    ConfigB = wa_raft_server:make_config([Node1, Node2, Node3]),

    % Follower gets two new config entries each removing a member from the leader
    {follower, State0} = server_cast(?APPEND_ENTRIES_RPC(1, Name, node2, 1, 1, [{1, {ref, {config, ConfigA}}}, {1, {ref, {config, ConfigB}}}], 2, 0)),
    ?assertEqual(2, State0#raft_state.commit_index),
    ?assertEqual(2, State0#raft_state.last_applied),
    ?assertEqual({ok, #raft_log_pos{index = 2, term = 1}, ConfigA}, wa_raft_storage:config(State0#raft_state.storage)),

    % Follower times out and starts a new election (now with 3 nodes)
    % Leader replicates to only nodes in latest config after being follower
    {candidate, _} = server_invoke(state_timeout, election),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    clear_message_queue(),
    {leader, _} = server_cast(?VOTE_RPC(2, Name, node2, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertNotCast(Name, node4, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),
    ?assertNotCast(Name, node5, ?APPEND_ENTRIES_RPC(2, Name, Node, _, _, _, _, _)),

    % Uncommitted config is removed by subsequent leader
    {follower, State1} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node3, 3, 3, [], 3, 0)),
    ?assertEqual(2, wa_raft_log:last_index(State1#raft_state.log_view)),

    % Follower times out and starts a new election (now with 4 nodes because config was rolled back)
    clear_message_queue(),
    {candidate, _} = server_invoke(state_timeout, election),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(4, Name, Node, normal, _, _)),
    ?assertNotCast(Name, node5, ?REQUEST_VOTE_RPC(4, Name, Node, _, _, _)),
    {candidate, _} = server_cast(?VOTE_RPC(4, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(4, Name, node2, true)),
    {leader, _} = server_cast(?VOTE_RPC(4, Name, node3, true)),

    % Stop server
    ok = server_stop().

%% Commits are batched up to a certain limit during the heartbeat interval
-spec commit_batch(Config :: ct_suite:ct_config()) -> ok.
commit_batch(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Set some environments related to commit batching
    ok = application:set_env(?RAFT_APPLICATION, raft_commit_batch_max, 2),

    % Setup leader
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Assert that one commit does not immediately trigger replication
    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {2, noop}, high)),
    ?assertNotCast(_, _, ?APPEND_ENTRIES_RPC(1, Name, Node, _, _, _, _, _)),

    % More commits then trigger replication because the max batch limit is 2.
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {3, noop}, high)),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {4, noop}, high)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, _, _, _)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, _, _, _)),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, _, _, _)),
    ?assertCast(Name, node5, ?APPEND_ENTRIES_RPC(1, Name, Node, 1, 1, _, _, _)),

    % Stop server
    ok = server_stop().

% Leader - give up leader role if election weight is 0
-spec node_weight(Config :: ct_suite:ct_config()) -> term().
node_weight(Config) ->
    % Setup server
    {follower, _} = server_start_and_bootstrap(Config),

    % Promote to leader
    {leader, State0, ok} = server_call(?PROMOTE_COMMAND(4, true)),
    ?assertEqual(4, State0#raft_state.current_term),

    % Heartbeat is ok with normal election weight
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % Leader does not resign upon heartbeat with zero election weight
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_ELECTION_WEIGHT, 0),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ok = application:unset_env(?RAFT_APPLICATION, ?RAFT_ELECTION_WEIGHT),

    % Leader does resign upon heartbeat while ineligible
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_LEADER_ELIGIBLE, false),
    {follower, _} = server_invoke(state_timeout, heartbeat),

    % Triggering new elections is rejected while ineligible
    {follower, State1, {error, ineligible}} = server_call(?TRIGGER_ELECTION_COMMAND(current)),
    ?assertEqual(4, State1#raft_state.current_term),

    % Follower should not attempt an election when ineligible
    {follower, State2} = server_invoke(state_timeout, election),
    ?assertEqual(4, State2#raft_state.current_term),

    % Follower should not attempt an election when its weight is zero
    ok = application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_ELIGIBLE),
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_ELECTION_WEIGHT, 0),
    {follower, State3} = server_invoke(state_timeout, election),
    ?assertEqual(4, State3#raft_state.current_term),
    ok = application:unset_env(?RAFT_APPLICATION, ?RAFT_ELECTION_WEIGHT),

    % Follower should attempt elections when eligible and when weight is non-zero
    {candidate, State4} = server_invoke(state_timeout, election),
    ?assertEqual(5, State4#raft_state.current_term),

    % Triggering new elections should be allowed now
    %  * Candidate will immediately start election for the next term.
    {candidate, State5, ok} = server_call(?TRIGGER_ELECTION_COMMAND(current)),
    ?assertEqual(6, State5#raft_state.current_term),

    % Stop server
    ok = server_stop().

% Leader - step down when check quorum is enabled and quorum is stale
-spec check_quorum(Config :: ct_suite:ct_config()) -> term().
check_quorum(Config) ->
    Name = ?SERVER_NAME(Config),

    % Setup server and promote to leader
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    clear_message_queue(),

    % With check quorum disabled (default), leader should not resign on heartbeat
    % regardless of quorum staleness (no heartbeat replies received yet)
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % Enable check quorum
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_LEADER_CHECK_QUORUM, true),

    % Leader entry seeds `leader_quorum_ts` to the promotion time, so the new
    % leader gets the standard liveness grace window before CheckQuorum can
    % force a resignation. Without that seed, this heartbeat would step down.
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % Promote to leader again and establish a quorum via heartbeat replies
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(2, true)),
    clear_message_queue(),

    % Simulate heartbeat replies from followers to establish quorum
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node2, 0, true, 1, 0)),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(2, Name, node3, 0, true, 1, 0)),
    clear_message_queue(),

    % With fresh quorum, leader should remain leader on heartbeat timeout
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % Simulate stale quorum by setting leader_quorum_ts to a very old timestamp
    StaleTs = erlang:monotonic_time(millisecond) - 60_000,
    server_replace_state(fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = StaleTs}} end),

    % Leader should resign because quorum is stale
    {follower, _} = server_invoke(state_timeout, heartbeat),

    % Disable check quorum and verify leader does not resign with stale quorum
    ok = application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_CHECK_QUORUM),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(3, true)),
    clear_message_queue(),
    server_replace_state(fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = StaleTs}} end),
    {leader, _} = server_invoke(state_timeout, heartbeat),

    % Stop server
    ok = server_stop().

% Leader - forced promotion under CheckQuorum must not resign on the next
% heartbeat tick. Regression guard: leader entry must seed `leader_quorum_ts`
% and clear `heartbeat_reply_ts` so the new leader is not immediately declared
% stale by `leader_eligible/1` on its first heartbeat tick.
-spec check_quorum_force_promote_multi_node(Config :: ct_suite:ct_config()) -> term().
check_quorum_force_promote_multi_node(Config) ->
    % Enable check quorum *before* the force-promote so the leader-entry
    % seeding is the only thing keeping the new leader alive.
    ok = application:set_env(?RAFT_APPLICATION, ?RAFT_LEADER_CHECK_QUORUM, true),

    % Bootstrap a multi-node cluster; node starts as follower with no
    % candidate phase, so `heartbeat_reply_ts` is empty.
    {follower, _} = server_start_and_bootstrap(Config),

    % Force-promote directly from follower to leader.
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    clear_message_queue(),

    % Without any peer heartbeat reply being injected, the leader must remain
    % leader for the duration of the standard liveness grace period.
    {leader, _} = server_invoke(state_timeout, heartbeat),

    ok = application:unset_env(?RAFT_APPLICATION, ?RAFT_LEADER_CHECK_QUORUM),

    % Stop server
    ok = server_stop().

% Non-leader `enter_state` must not touch the timestamp fields, so a
% leader→follower transition cannot log "no longer live after receiving
% leader heartbeat at undefined" simply by re-entering follower state.
-spec enter_follower_does_not_flap_liveness(Config :: ct_suite:ct_config()) -> term().
enter_follower_does_not_flap_liveness(Config) ->
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Plant a known `leader_commit_index_ts` on the leader before stepping down.
    KnownTs = erlang:monotonic_time(millisecond) - 5,
    {leader, _} = server_replace_state(
        fun ({SN, S}) -> {SN, S#raft_state{leader_commit_index_ts = KnownTs}} end
    ),

    % Resign: transitions leader → follower via enter_state. Under the split,
    % non-leader entry must leave the commit-freshness field alone.
    {follower, StateAfter, ok} = server_call(?RESIGN_COMMAND),
    ?assertEqual(KnownTs, StateAfter#raft_state.leader_commit_index_ts),

    ok = server_stop().

% `?PROMOTE_COMMAND` freshness gate must reject when either
% `leader_quorum_ts` or `leader_commit_index_ts` is recent, exercising the
% `max(...)` over both signals.
-spec promote_gate_considers_both_signals(Config :: ct_suite:ct_config()) -> term().
promote_gate_considers_both_signals(Config) ->
    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {follower, _, ok} = server_call(?RESIGN_COMMAND),

    Now = erlang:monotonic_time(millisecond),

    % Recent leader_quorum_ts alone must block promotion.
    server_replace_state(
        fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = Now, leader_commit_index_ts = undefined}} end
    ),
    {follower, _, {error, rejected}} = server_call(?PROMOTE_COMMAND(next, false)),

    % Recent leader_commit_index_ts alone must also block promotion.
    server_replace_state(
        fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = undefined, leader_commit_index_ts = Now}} end
    ),
    {follower, _, {error, rejected}} = server_call(?PROMOTE_COMMAND(next, false)),

    % Clearing both should allow promotion.
    server_replace_state(
        fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = undefined, leader_commit_index_ts = undefined}} end
    ),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(next, false)),

    ok = server_stop().

% A leader that steps down while both freshness signals are recent must not
% be reported as stale by `wa_raft_info:get_stale/2`. Aging both signals
% beyond the freshness threshold must flip staleness to true.
-spec stepped_down_leader_is_not_stale(Config :: ct_suite:ct_config()) -> term().
stepped_down_leader_is_not_stale(Config) ->
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),
    Name = ?SERVER_NAME(Config),

    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Drive noop commit to completion so both leader_quorum_ts and
    % leader_commit_index_ts are populated in this tenure.
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, StateL} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assert(StateL#raft_state.leader_quorum_ts =/= undefined),
    ?assert(StateL#raft_state.leader_commit_index_ts =/= undefined),
    ?assertEqual(1, StateL#raft_state.commit_index),

    % Resign — non-leader entry preserves the two timestamps.
    {follower, StateF, ok} = server_call(?RESIGN_COMMAND),
    ?assert(StateF#raft_state.leader_quorum_ts =/= undefined),
    ?assert(StateF#raft_state.leader_commit_index_ts =/= undefined),
    ?assertEqual(false, wa_raft_info:get_stale(Table, Partition)),

    % Plant both timestamps far in the past; run update_status via a
    % state_timeout invocation so the cached status is refreshed.
    Stale = erlang:monotonic_time(millisecond) - 60_000,
    server_replace_state(
        fun ({SN, S}) -> {SN, S#raft_state{leader_quorum_ts = Stale, leader_commit_index_ts = Stale}} end
    ),
    {_, _} = server_invoke(state_timeout, election),
    ?assertEqual(true, wa_raft_info:get_stale(Table, Partition)),

    ok = server_stop().

% `leader_advance_commit_index/1` must move `leader_commit_index_ts` when the
% quorum-driven commit index advances, and must NOT move it on a heartbeat
% tick that does not advance the commit index.
-spec leader_commit_advance_bumps_ts(Config :: ct_suite:ct_config()) -> term().
leader_commit_advance_bumps_ts(Config) ->
    Name = ?SERVER_NAME(Config),

    {follower, _} = server_start_and_bootstrap(Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),

    % Drive noop commit through so the leader gets an initial
    % leader_commit_index_ts captured at time T0.
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, StateT0} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    ?assertEqual(1, StateT0#raft_state.commit_index),
    T0 = StateT0#raft_state.leader_commit_index_ts,
    ?assert(T0 =/= undefined),

    % A heartbeat tick with no new match-index advancement must leave
    % leader_commit_index_ts alone. `leader_commit_and_replicate/1` runs no
    % commit-index advance in a multi-node cluster, so no explicit sleep is
    % needed to distinguish T0 from a subsequent tick.
    {leader, StateTick} = server_invoke(state_timeout, heartbeat),
    ?assertEqual(T0, StateTick#raft_state.leader_commit_index_ts),

    ok = server_stop().

-spec participant_trim_index(Config :: ct_suite:ct_config()) -> ok.
participant_trim_index(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),
    Table = ?SERVER_TABLE(Config),
    Partition = ?SERVER_PARTITION(Config),
    Self = #raft_identity{name = Name, node = Node},
    Node2 = #raft_identity{name = Name, node = node2},
    Node3 = #raft_identity{name = Name, node = node3},
    Participant = #raft_identity{name = Name, node = node4},
    ClusterConfig = wa_raft_server:make_config(
        [Self, Node2, Node3, Participant],
        [Self, Node2, Node3],
        []
    ),

    ok = application:set_env(?RAFT_APPLICATION, raft_max_log_records_per_file, 0),
    ok = application:set_env(?RAFT_APPLICATION, raft_max_log_records, 0),

    {follower, _} = server_start_and_bootstrap(100, 2, ClusterConfig, Config),
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(3, true)),

    clear_message_queue(),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 100, true, 101, 100)),
    {leader, StateBeforeParticipant} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node3, 100, true, 101, 100)),
    ?assertEqual(101, StateBeforeParticipant#raft_state.commit_index),
    ?assertEqual(100, wa_raft_log:first_index(StateBeforeParticipant#raft_state.log_view)),

    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node4, 100, true, 101, 100)),
    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(3, Name, Node, 101, 3, [], 101, 101)),

    clear_message_queue(),
    {leader, _} = server_cast(?COMMIT_COMMAND(?FROM(), {ref, noop}, high)),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    {leader, StateAfterParticipant} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node2, 101, true, 102, 101)),
    ?assertEqual(102, StateAfterParticipant#raft_state.commit_index),
    ?assertEqual(101, wa_raft_log:first_index(StateAfterParticipant#raft_state.log_view)),

    ok = meck:new(wa_raft_snapshot_catchup, [passthrough, no_link]),
    ok = meck:expect(wa_raft_snapshot_catchup, catchup,
        fun (App, RaftName, Follower, CatchupTable, CatchupPartition, Witness) ->
            server_notify(host(), snapshot_catchup, {App, RaftName, Follower, CatchupTable, CatchupPartition, Witness}),
            ok
        end),

    clear_message_queue(),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node4, 101, false, 0, 0)),
    ?assertNotify(snapshot_catchup, {_, Name, node4, Table, Partition, false}, 250),

    {leader, StateAfterSnapshot} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(3, Name, node4, 0, false, 101, 101)),
    ?assertMatch(#{node4 := 102}, StateAfterSnapshot#raft_state.next_indices),

    clear_message_queue(),
    {leader, _} = server_invoke(state_timeout, heartbeat),
    ?assertCast(Name, node4, ?APPEND_ENTRIES_RPC(3, Name, Node, 101, 3, [{3, _}], 102, 101)),

    ok = server_stop().

-spec trim_index(Config :: ct_suite:ct_config()) -> ok.
trim_index(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    % Configure RAFT to always trim when a rotate is triggered
    ok = application:set_env(?RAFT_APPLICATION, raft_max_log_records_per_file, 0),
    ok = application:set_env(?RAFT_APPLICATION, raft_max_log_records, 0),

    % Setup follower
    {follower, _} = server_start_and_bootstrap(100, 2, Config),

    % Send an append with new log entries but without new commits
    {follower, _} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 100, 2, [{3, {101, noop}}, {3, {102, noop}}, {3, {103, noop}}], 100, 100)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 100, true, 103, 100)),

    % Send an append with new commits
    {follower, _} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 103, 3, [], 102, 100)),
    % Heartbeat response is issued before apply occurs, so last applied index is only 100
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 103, true, 103, 100)),

    % Send an append with new commits and new trim index (so follower will apply and rotate)
    {follower, #raft_state{log_view = View0}} = server_cast(?APPEND_ENTRIES_RPC(3, Name, node2, 103, 3, [], 103, 102)),
    % Heartbeat response is issued before apply occurs, so last applied index is only 102.
    % As the log entry is not yet applied, follower will report 101.
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RESPONSE_RPC(3, Name, Node, 103, true, 103, _)),
    ?assertEqual(102, wa_raft_log:first_index(View0)),

    % Stop server
    ok = server_stop().

-spec get_current_config(Config :: ct_suite:ct_config()) -> ok.
get_current_config(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Test 1: get_current_config returns empty config in stalled state (before bootstrap)
    {stalled, _} = server_start(Config),
    EmptyConfig = wa_raft_server:make_config(),
    {stalled, _, RetrievedConfig1} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(EmptyConfig, RetrievedConfig1),

    % Test 2: get_current_config returns cluster config after bootstrap
    ClusterConfig = ?SERVER_CLUSTER_CONFIG(Config),
    {follower, _, ok} = server_call(?BOOTSTRAP_COMMAND(#raft_log_pos{index = 0, term = 0}, ClusterConfig, #{})),
    {follower, _, RetrievedConfig2} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(ClusterConfig, RetrievedConfig2),

    % Test 3: get_current_config returns updated config after promoting to leader
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    {leader, _, RetrievedConfig3} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(ClusterConfig, RetrievedConfig3),

    % Test 4: get_current_config returns new config after adding a member
    % First, commit the initial noop to allow config changes
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 0, true, 1, 0)),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 0, true, 1, 0)),
    % Add a new member
    {leader, _, {ok, #raft_log_pos{index = ConfigIndex}}} = server_call(?ADJUST_CONFIG_COMMAND({add, {Name, node6}}, undefined)),
    % Replicate the config change to a quorum
    {leader, _} = server_invoke(state_timeout, heartbeat),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node2, 1, true, ConfigIndex, 1)),
    {leader, _} = server_cast(?APPEND_ENTRIES_RESPONSE_RPC(1, Name, node3, 1, true, ConfigIndex, 1)),
    % Get config after adding member
    NewConfig = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3},
        #raft_identity{name = Name, node = node4},
        #raft_identity{name = Name, node = node5},
        #raft_identity{name = Name, node = node6}
    ]),
    {leader, _, RetrievedConfig4} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(NewConfig, RetrievedConfig4),

    % Test 5: Verify get_current_config does not modify server state
    {leader, StateBefore, _} = server_call(?STATUS_COMMAND),
    {leader, _, _} = server_call(?CURRENT_CONFIG_COMMAND),
    {leader, StateAfter, _} = server_call(?STATUS_COMMAND),
    ?assertEqual(StateBefore, StateAfter),

    % Stop server
    ok = server_stop().

-spec get_current_config_different_states(Config :: ct_suite:ct_config()) -> ok.
get_current_config_different_states(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    % Setup a cluster config
    ClusterConfig = wa_raft_server:make_config([
        #raft_identity{name = Name, node = Node},
        #raft_identity{name = Name, node = node2},
        #raft_identity{name = Name, node = node3}
    ]),

    % Test 1: get_current_config works in stalled state
    {stalled, _} = server_start(Config),
    {stalled, _, ConfigStalled} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertMatch(#{version := _, membership := _}, ConfigStalled),

    % Bootstrap to get out of stalled state
    {follower, _, ok} = server_call(?BOOTSTRAP_COMMAND(#raft_log_pos{index = 0, term = 0}, ClusterConfig, #{})),

    % Test 2: get_current_config works in follower state
    {follower, _, ConfigFollower} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(ClusterConfig, ConfigFollower),

    % Test 3: get_current_config works after promoting to leader
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    ?assertCast(Name, node2, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    ?assertCast(Name, node3, ?APPEND_ENTRIES_RPC(1, Name, Node, 0, 0, [{1, {_, noop}}], 0, 0)),
    {leader, _, ConfigLeader} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(ClusterConfig, ConfigLeader),

    % Test 4: get_current_config works after resigning from leader
    {follower, _, ok} = server_call(?RESIGN_COMMAND),
    {follower, _, ConfigResigned} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(ClusterConfig, ConfigResigned),

    % Test 5: Trigger election to test in candidate state
    {candidate, _} = server_invoke(state_timeout, election),
    {candidate, _, ConfigCandidate} = server_call(?CURRENT_CONFIG_COMMAND),
    ?assertEqual(ClusterConfig, ConfigCandidate),

    % Stop server
    ok = server_stop().

-spec pre_vote_election(Config :: ct_suite:ct_config()) -> ok.
pre_vote_election(Config) ->
    Node = node(),
    Name = ?SERVER_NAME(Config),

    application:set_env(wa_raft, raft_election_pre_vote, true),

    % Setup server as follower at term 1, log index 1
    {follower, State0} = server_start_and_bootstrap(1, 1, Config),
    ?assertEqual(1, State0#raft_state.current_term),

    % Trigger election timeout → candidate with pre-vote
    %  * Term should NOT advance during pre-vote phase
    %  * Pre-vote ref should be set
    %  * REQUEST_PRE_VOTE should be sent to all peers
    %  * No REQUEST_VOTE should be sent yet
    {candidate, State1} = server_invoke(state_timeout, election),
    ?assertEqual(1, State1#raft_state.current_term),
    Ref = State1#raft_state.pre_vote_ref,
    ?assertNotEqual(undefined, Ref),
    ?assertCast(Name, node2, ?REQUEST_PRE_VOTE_RPC(1, Name, Node, Ref)),
    ?assertCast(Name, node3, ?REQUEST_PRE_VOTE_RPC(1, Name, Node, Ref)),
    ?assertCast(Name, node4, ?REQUEST_PRE_VOTE_RPC(1, Name, Node, Ref)),
    ?assertCast(Name, node5, ?REQUEST_PRE_VOTE_RPC(1, Name, Node, Ref)),
    ?assertNotCast(Name, Node, ?VOTE_RPC(_, Name, Node, _)),
    ?assertNotCast(Name, node2, ?REQUEST_VOTE_RPC(_, Name, Node, _, _, _)),

    % First pre-vote response: node2 supports (self + node2 = 2/5, no majority yet)
    {candidate, State2} = server_cast(?PRE_VOTE_RPC(1, Name, node2, Ref, true, 0, 0)),
    ?assertEqual(1, State2#raft_state.current_term),

    % Second pre-vote response: node3 supports (self + node2 + node3 = 3/5, majority!)
    %  * Election should start: term advances to 2
    %  * REQUEST_VOTE should be sent to all peers
    %  * Self-vote should be cast
    clear_message_queue(),
    {candidate, State3} = server_cast(?PRE_VOTE_RPC(1, Name, node3, Ref, true, 0, 0)),
    ?assertEqual(2, State3#raft_state.current_term),
    ?assertCast(Name, Node, ?VOTE_RPC(2, Name, Node, true)),
    ?assertCast(Name, node2, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node3, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node4, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),
    ?assertCast(Name, node5, ?REQUEST_VOTE_RPC(2, Name, Node, normal, 1, 1)),

    % Issue 2 regression check: pre_vote_ref must be cleared by advance_term
    ?assertEqual(undefined, State3#raft_state.pre_vote_ref),
    ?assertEqual(#{}, State3#raft_state.pre_votes),

    % Late-arriving pre-vote with old ref should be ignored (no second election)
    {candidate, State3b} = server_cast(?PRE_VOTE_RPC(1, Name, node4, Ref, true, 0, 0)),
    ?assertEqual(2, State3b#raft_state.current_term),

    % Complete the real election: collect votes to become leader
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, Node, true)),
    {candidate, _} = server_cast(?VOTE_RPC(2, Name, node2, true)),
    {leader, State4} = server_cast(?VOTE_RPC(2, Name, node3, true)),
    ?assertEqual(2, State4#raft_state.current_term),

    % Stop server
    ok = server_stop().

-spec pre_vote_rejected(Config :: ct_suite:ct_config()) -> ok.
pre_vote_rejected(Config) ->
    Name = ?SERVER_NAME(Config),

    application:set_env(wa_raft, raft_election_pre_vote, true),

    % Setup follower and trigger election → candidate with pre-vote
    {follower, _} = server_start_and_bootstrap(1, 1, Config),
    {candidate, State0} = server_invoke(state_timeout, election),
    ?assertEqual(1, State0#raft_state.current_term),
    Ref = State0#raft_state.pre_vote_ref,
    ?assertNotEqual(undefined, Ref),
    clear_message_queue(),

    % Send rejections from a majority of peers (3 out of 5 → majority opposition)
    {candidate, _} = server_cast(?PRE_VOTE_RPC(1, Name, node2, Ref, false, 0, 0)),
    {candidate, _} = server_cast(?PRE_VOTE_RPC(1, Name, node3, Ref, false, 0, 0)),
    {follower, State1} = server_cast(?PRE_VOTE_RPC(1, Name, node4, Ref, false, 0, 0)),

    % Should fall back to follower without advancing term
    ?assertEqual(1, State1#raft_state.current_term),

    % Stop server
    ok = server_stop().

-spec pre_vote_request(Config :: ct_suite:ct_config()) -> ok.
pre_vote_request(Config) ->
    Name = ?SERVER_NAME(Config),
    Node = node(),

    application:set_env(wa_raft, raft_election_pre_vote, true),

    Ref = make_ref(),

    % 1. Follower with missing leader (no recent heartbeat) → should grant pre-vote
    {follower, _} = server_start_and_bootstrap(1, 1, Config),
    {follower, _} = server_cast(?REQUEST_PRE_VOTE_RPC(1, Name, node2, Ref)),
    ?assertCast(Name, node2, ?PRE_VOTE_RPC(1, Name, Node, Ref, true, 1, 1)),

    % 2. Follower with active leader (recent heartbeat) → should deny pre-vote
    SetHeartbeat = fun ({SN, S}) -> {SN, S#raft_state{leader_commit_index_ts = erlang:monotonic_time(millisecond)}} end,
    {follower, _} = server_replace_state(SetHeartbeat),
    Ref2 = make_ref(),
    {follower, _} = server_cast(?REQUEST_PRE_VOTE_RPC(1, Name, node2, Ref2)),
    ?assertCast(Name, node2, ?PRE_VOTE_RPC(1, Name, Node, Ref2, false, 1, 1)),

    % 3. Leader → should deny pre-vote (leader exists — it's us)
    {leader, _, ok} = server_call(?PROMOTE_COMMAND(1, true)),
    clear_message_queue(),
    Ref3 = make_ref(),
    {leader, _} = server_cast(?REQUEST_PRE_VOTE_RPC(1, Name, node2, Ref3)),
    ?assertCast(Name, node2, ?PRE_VOTE_RPC(1, Name, Node, Ref3, false, _, _)),

    % 4. Disabled → should not respond to pre-vote at all
    {disabled, _, ok} = server_call(?DISABLE_COMMAND("Test")),
    clear_message_queue(),
    Ref4 = make_ref(),
    {disabled, _} = server_cast(?REQUEST_PRE_VOTE_RPC(1, Name, node2, Ref4)),
    ?assertNotCast(Name, node2, ?PRE_VOTE_RPC(_, Name, Node, Ref4, _, _, _)),

    % Stop server
    ok = server_stop().
