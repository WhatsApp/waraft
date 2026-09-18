% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_cluster_test_helper).
-oncall("whatsapp_msgd").
-compile(warn_missing_spec_all).

-export([
    setup_cluster/2,
    teardown_cluster/1,
    reset_cluster/1,
    wait_for_ready/1,
    get_leader/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-define(TABLE, test).
-define(PARTITION, 1).

-spec setup_cluster(Count :: non_neg_integer(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
setup_cluster(Count, Config0) ->
    setup_dist(),
    Nodes = [setup_node(Index) || Index <- lists:seq(1, Count)],
    Config1 = [{nodes, Nodes} | Config0],
    [setup_raft(Node, Config0) || Node <- Nodes],
    ok = bootstrap(Config1),
    Config1.

-spec setup_dist() -> ok.
setup_dist() ->
    node() =:= 'nonode@nohost' andalso
        begin
            % distribution is not started, ensure epmd is
            (erl_epmd:names("localhost") =:= {error, address}) andalso
                ([] = os:cmd("epmd -daemon")),
            Name = list_to_atom("test" ++ os:getpid() ++ "@localhost"),
            {ok, _} = net_kernel:start([Name, shortnames])
        end,
    ok.

-spec setup_node(Index :: pos_integer()) -> Node :: node().
setup_node(Index) ->
    Name = "node" ++ integer_to_list(Index) ++ "-" ++ os:getpid(),
    {ok, Pid, Node} = ?CT_PEER(#{name => Name, args => ["-kernel", "prevent_overlapping_partitions", "false"]}),
    unlink(Pid),
    register(Node, Pid),
    erpc:call(Node, code, add_pathsz, [code:get_path()]),
    Node.

-spec setup_raft(Node :: node(), Config :: ct_suite:ct_config()) -> ok.
setup_raft(Node, Config) ->
    wa_raft_test_helper:set_database_path(Node, Config),
    wa_raft_test_helper:set_app_option(Node, raft_heartbeat_interval_ms, 50),
    wa_raft_test_helper:set_app_option(Node, raft_election_weight, 0),
    wa_raft_test_helper:set_app_option(Node, raft_election_timeout_ms, 400),
    wa_raft_test_helper:set_app_option(Node, raft_election_timeout_ms_max, 500),
    wa_raft_test_helper:set_app_option(Node, raft_catchup_rpc_timeout_ms, 2000),
    wa_raft_test_helper:set_app_option(Node, raft_snapshot_catchup_completed_backoff_ms, 0),
    wa_raft_test_helper:set_app_option(Node, raft_snapshot_catchup_failed_backoff_ms, 0),
    wa_raft_test_helper:set_app_option(Node, raft_commit_batch_interval_ms, 0),

    {ok, _} = erpc:call(Node, application, ensure_all_started, [?RAFT_APPLICATION]),
    RaftArgs = #{table => ?TABLE, partition => ?PARTITION},
    RaftChildSpecs = erpc:call(Node, wa_raft_sup, child_spec, [?RAFT_APPLICATION, [RaftArgs]]),
    {ok, _} = erpc:call(Node, supervisor, start_child, [wa_raft_app_sup, RaftChildSpecs]),
    ok.

-spec bootstrap(Config :: ct_suite:ct_config()) -> ok.
bootstrap(Config) ->
    Server = wa_raft_server:default_name(?TABLE, ?PARTITION),
    Nodes = proplists:get_value(nodes, Config),
    Leader = hd(Nodes),
    Members = [#raft_identity{name = Server, node = Node} || Node <- Nodes],
    Position = #raft_log_pos{index = 1, term = 1},
    ClusterConfig = wa_raft_server:make_config(Members),
    [wa_raft_server:bootstrap({Server, Node}, Position, ClusterConfig, #{}) || Node <- Nodes],
    wa_raft_server:trigger_election({Server, Leader}),
    ok.

-spec wait_for_node(Node :: node()) -> ok.
wait_for_node(Node) ->
    Stale = erpc:call(Node, wa_raft_info, get_stale, [?TABLE, ?PARTITION]),
    Live = erpc:call(Node, wa_raft_info, get_live, [?TABLE, ?PARTITION]),
    case not Stale andalso Live of
        true ->
            ok;
        false ->
            ct:sleep(10),
            wait_for_node(Node)
    end.

-spec teardown_cluster(Config :: ct_suite:ct_config()) -> ok.
teardown_cluster(Config) ->
    Nodes = lists:reverse(proplists:get_value(nodes, Config)),
    [teardown_node(Node) || Node <- Nodes],
    ok.

-spec teardown_node(Node :: node()) -> ok.
teardown_node(Node) ->
    wa_raft_test_helper:stop_server(Node),
    case whereis(Node) of
        Pid when is_pid(Pid) -> peer:stop(Pid);
        undefined -> ok
    end,
    wait_until_node_shutdown(Node).

-spec wait_until_node_shutdown(Node :: node()) -> ok.
wait_until_node_shutdown(Node) ->
    case lists:member(Node, nodes()) of
        true ->
            timer:sleep(100),
            wait_until_node_shutdown(Node);
        _ ->
            ok
    end.

-spec reset_cluster(Config :: ct_suite:ct_config()) -> ok.
reset_cluster(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    [wa_raft_test_helper:stop_server(Node) || Node <- Nodes],
    [wa_raft_test_helper:start_server(Node) || Node <- Nodes],
    ok = bootstrap(Config),
    ok = wait_for_ready(Config),
    ok.

-spec wait_for_ready(Config :: ct_suite:ct_config()) -> ok.
wait_for_ready(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    [wait_for_node(Node) || Node <- Nodes],
    ok.

-spec get_leader(Config :: ct_suite:ct_config()) -> Leader :: node() | undefined.
get_leader(Config) ->
    Node = hd(proplists:get_value(nodes, Config)),
    case erpc:call(Node, wa_raft_info, get_leader, [?TABLE, ?PARTITION]) of
        undefined ->
            ct:sleep(100),
            get_leader(Config);
        Leader ->
            Leader
    end.
