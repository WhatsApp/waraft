%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_sanity_e2e_SUITE).
-oncall("whatsapp_msgd").
-compile(warn_missing_spec_all).

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

-export([
    basic/1,
    restart/1,
    catchup/1,
    full_catchup/1,
    reelection/1,
    disable_restart/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_rpc.hrl").

-define(STATUS_CHECK_INTERVAL_MS, 1000).

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 300}}] ++ wa_raft_test_helper:suite().

-spec init_per_suite(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_suite(Config) ->
    wa_raft_test_helper:setup_environment(Config).

-spec end_per_suite(Config :: ct_suite:ct_config()) -> ok.
end_per_suite(Config) ->
    wa_raft_test_helper:teardown_environment(Config),
    ok.

-spec init_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_group(Group, Config0) ->
    Config1 = [{group, Group} | Config0],
    Config2 = wa_raft_cluster_test_helper:setup_cluster(3, Config1),
    Config2.

-spec end_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_group(_, _Config) ->
    ok.

-spec init_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_testcase(Testcase, Config0) ->
    Config1 = [{testcase, Testcase} | Config0],

    % Set up partition path and reset application flags
    [
        begin
            ok = wa_raft_test_helper:set_database_path(Node, Config1),
            ok = wa_raft_test_helper:set_app_option(Node, ?RAFT_ELECTION_WEIGHT, 0),
            ok = wa_raft_test_helper:unset_app_option(Node, dist_transport_chunk_size),
            ok = wa_raft_test_helper:unset_app_option(Node, ?RAFT_LOG_ROTATION_INTERVAL)
        end || Node <- proplists:get_value(nodes, Config1)
    ],

    % Reset the cluster by wiping and bootstrapping
    wa_raft_cluster_test_helper:reset_cluster(Config1),
    Config1.

-spec end_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_, _) ->
    ok.

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, integration_tests}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {integration_tests, [
            basic,
            restart,
            reelection,
            catchup,
            full_catchup,
            disable_restart
        ]}
    ].

%%--------------------------------------------------------------------
%% HELPERS

-spec write_batch(Node :: node(), From :: integer(), To :: integer(), ValueDelta :: integer()) -> ok.
write_batch(Node, From, To, ValueDelta) ->
    [wa_raft_test_helper:write(Node, Key, Key + ValueDelta) || Key <- lists:seq(From, To)],
    ct:print("Wrote ~p-~p to ~p", [From, To, Node]),
    ok.

-spec check_batch(Node :: node(), From :: integer(), To :: integer(), ValueDelta :: integer()) -> ok.
check_batch(Node, From, To, ValueDelta) ->
    [
        begin
            Value = read_with_retry(Node, Key, 10),
            ?assertEqual(Key + ValueDelta, Value, Node)
        end || Key <- lists:seq(From, To)
    ],
    ct:print("Verified ok ~p-~p on ~p", [From, To, Node]),
    ok.

-spec read_with_retry(Node :: node(), Key :: integer(), Retries :: non_neg_integer()) -> term().
read_with_retry(Node, Key, 0) ->
    {ok, Value} = wa_raft_test_helper:read(Node, Key),
    Value;
read_with_retry(Node, Key, Retries) ->
    case wa_raft_test_helper:read(Node, Key) of
        {ok, Value} ->
            Value;
        {error, {stale, _}} ->
            ct:sleep(500),
            read_with_retry(Node, Key, Retries - 1);
        Other ->
            {ok, Value} = Other,
            Value
    end.

-spec check_data(Node :: node(), ExpectedLastApplied :: integer(), From :: integer(), To :: integer(), ValueDelta :: integer()) -> ok.
check_data(Node, ExpectedLastApplied, From, To, ValueDelta) ->
    wait_for_last_applied(Node, ExpectedLastApplied),
    check_batch(Node, From, To, ValueDelta).

-spec last_applied(Node :: node()) -> integer().
last_applied(Node) ->
    Status = wa_raft_test_helper:get_storage_status(Node),
    proplists:get_value(last_applied, Status).

-spec wait_for(Predicate :: fun(() -> boolean()), Timeout :: integer() | infinity) -> ok | {error, timeout}.
wait_for(Predicate, infinity) ->
    wait_for_impl(Predicate, infinity);
wait_for(Predicate, Timeout) ->
    wait_for_impl(Predicate, erlang:monotonic_time(millisecond) + Timeout).

-spec wait_for_impl(Predicate :: fun(() -> boolean()), Epoch :: integer() | infinity) -> ok | {error, timeout}.
wait_for_impl(Predicate, Epoch) ->
    case erlang:monotonic_time(millisecond) >= Epoch of
        true ->
            {error, timeout};
        false ->
            case Predicate() of
                true ->
                    ok;
                false ->
                    ct:sleep(?STATUS_CHECK_INTERVAL_MS),
                    wait_for_impl(Predicate, Epoch)
            end
    end.

-spec wait_for_server_status(Node :: node(), Field :: atom(), Value :: term()) -> ok.
wait_for_server_status(Node, Field, Value) ->
    wait_for_server_status(Node, Field, Value, infinity).

-spec wait_for_server_status(Node :: node(), Field :: atom(), Value :: term(), Timeout :: integer() | infinity) -> ok.
wait_for_server_status(Node, Field, Value, Timeout) ->
    ct:print("Waiting for ~0p server state field ~0p to be ~0p.", [Node, Field, Value]),
    ?assertEqual(ok, wait_for(fun () -> wa_raft_test_helper:get_server_status(Node, Field) =:= Value end, Timeout)).

-spec wait_for_leader(Node :: node()) -> ok.
wait_for_leader(Node) ->
    wait_for_server_status(Node, state, leader).

-spec wait_for_last_applied(node(), integer()) -> ok.
wait_for_last_applied(Node, LastApplied) ->
    wait_for_last_applied(Node, LastApplied, infinity).

-spec wait_for_last_applied(node(), integer(), integer() | infinity) -> ok.
wait_for_last_applied(Node, LastApplied, Timeout) ->
    ct:print("Waiting for ~0p to reach index ~0p.", [Node, LastApplied]),
    ?assertEqual(ok, wait_for(fun () -> last_applied(Node) =:= LastApplied end, Timeout)).

%%--------------------------------------------------------------------
%% TEST CASES

%% Start the cluster, write some data, and check all other nodes
-spec basic(Config :: ct_suite:ct_config()) -> ok.
basic(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Leader = wa_raft_cluster_test_helper:get_leader(Config),

    % write on leader: key 1 - 100
    write_batch(Leader, 1, 100, 10),
    LastApplied = last_applied(Leader),

    % verify value on all nodes
    [check_data(Node, LastApplied, 1, 100, 10) || Node <- Nodes],
    ok.

%% Restart a follower. No data change.
-spec restart(Config :: ct_suite:ct_config()) -> ok.
restart(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Leader = wa_raft_cluster_test_helper:get_leader(Config),

    % write on leader: key 1 - 100
    write_batch(Leader, 1, 100, 10),
    LastApplied = last_applied(Leader),
    [wait_for_last_applied(Node, LastApplied) || Node <- Nodes],

    % Restart a follower and verify it still has data
    Follower = hd([Node || Node <- Nodes, Node =/= Leader]),
    wa_raft_test_helper:stop_server(Follower),
    wa_raft_test_helper:start_server(Follower),
    wa_raft_cluster_test_helper:wait_for_ready(Config),
    wait_for_last_applied(Follower, LastApplied),
    check_batch(Follower, 1, 100, 10),

    ok.

%% Shutdown leader, leader is elected again, then write and check data
-spec reelection(Config :: ct_suite:ct_config()) -> ok.
reelection(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Leader = wa_raft_cluster_test_helper:get_leader(Config),
    Follower = hd([Node || Node <- Nodes, Node =/= Leader]),

    % allow the follower to be elected
    ok = wa_raft_test_helper:unset_app_option(Follower, ?RAFT_ELECTION_WEIGHT),

    % write on leader: keys 1 - 10
    write_batch(Leader, 1, 10, 10),
    LastApplied0 = last_applied(Leader),
    [check_data(Node, LastApplied0, 1, 10, 10) || Node <- Nodes],

    % stop leader
    wa_raft_test_helper:stop_server(Leader),

    % check that the follower node has become leader
    wait_for_leader(Follower),

    % write after new leader is elected
    write_batch(Follower, 1, 10, 20),
    LastApplied1 = last_applied(Follower),

    % start old leader
    wa_raft_test_helper:start_server(Leader),
    wa_raft_cluster_test_helper:wait_for_ready(Config),
    [check_data(Node, LastApplied1, 1, 10, 20) || Node <- Nodes],
    ok.

%% Shut down one node and catch up with the leader using logs
-spec catchup(Config :: ct_suite:ct_config()) -> ok.
catchup(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Leader = wa_raft_cluster_test_helper:get_leader(Config),
    Follower = hd([Node || Node <- Nodes, Node =/= Leader]),

    % write on node 1: key 1 - 100
    write_batch(Leader, 1, 50, 20),

    % Restart a follower normally so logs are used to catch up
    wa_raft_test_helper:stop_server(Follower),
    write_batch(Leader, 1, 50, 21),
    wa_raft_test_helper:start_server(Follower),
    LastApplied = last_applied(Leader),
    wait_for_last_applied(Follower, LastApplied),

    % verify data on the follower
    check_batch(Follower, 1, 50, 21),
    ok.

%% Shut down one node and catch up with the leader using a snapshot
-spec full_catchup(Config :: ct_suite:ct_config()) -> ok.
full_catchup(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Leader = wa_raft_cluster_test_helper:get_leader(Config),
    Follower = hd([Node || Node <- Nodes, Node =/= Leader]),

    % set a limit on the number of log entries before rotation
    [wa_raft_test_helper:set_app_option(Node, ?RAFT_LOG_ROTATION_INTERVAL, 50) || Node <- Nodes],

    % limit chunk size (data file in snapshot is at least 1K)
    [wa_raft_test_helper:set_app_option(Node, dist_transport_chunk_size, 1000) || Node <- Nodes],

    % write on leader: keys 1 - 100
    write_batch(Leader, 1, 100, 30),

    % erase the data on a follower
    wa_raft_test_helper:stop_server(Follower),
    wa_raft_test_helper:wipe_server(Follower),

    % write more data on leader: keys 1 - 60
    write_batch(Leader, 1, 60, 31),

    % restart the follower and wait for it to catch up
    wa_raft_test_helper:start_server(Follower),
    LastApplied = last_applied(Leader),
    wait_for_last_applied(Follower, LastApplied),

    % verify data on the follower
    check_batch(Follower, 1, 60, 31),
    check_batch(Follower, 61, 100, 30),
    ok.

-spec disable_restart(Config :: ct_suite:ct_config()) -> ok.
disable_restart(Config) ->
    Leader = wa_raft_cluster_test_helper:get_leader(Config),

    % disable partition
    ok = wa_raft_test_helper:call_server(Leader, ?DISABLE_COMMAND("Test disable.")),
    wait_for_server_status(Leader, state, disabled),

    % partition should still be disabled after restart
    wa_raft_test_helper:stop_server(Leader),
    wa_raft_test_helper:start_server(Leader),
    wait_for_server_status(Leader, state, disabled),
    wait_for_server_status(Leader, disable_reason, "Test disable."),

    ok.
