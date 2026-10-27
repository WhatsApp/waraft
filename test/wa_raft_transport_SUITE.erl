% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_SUITE).
-oncall("whatsapp_kvdb").
-compile(warn_missing_spec_all).

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0
]).

%% Unit tests
-export([
    safe_relative_paths_accepted/1,
    traversal_paths_rejected/1,
    absolute_paths_rejected/1,
    worker_failures/1,
    worker_shutdown/1
]).

%% Transport worker behavior callbacks
-export([
    transport_init/1,
    transport_send/3,
    transport_terminate/2
]).

-include_lib("assert/include/assert.hrl").

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 30}}].

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, unit}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {unit, [
            safe_relative_paths_accepted,
            traversal_paths_rejected,
            absolute_paths_rejected,
            worker_failures,
            worker_shutdown
        ]}
    ].

%%--------------------------------------------------------------------
%% UNIT TESTS

-define(ROOT, "/var/data/transport/1").

-spec safe_relative_paths_accepted(Config :: ct_suite:ct_config()) -> ok.
safe_relative_paths_accepted(_Config) ->
    Files = [
        {1, "000000.dat", 10},
        {2, "subdir/000000.dat", 20},
        {3, "a/b/c/file", 30}
    ],
    ?assertEqual(
        {ok, [
            {1, "000000.dat", filename:join(?ROOT, "000000.dat"), 10},
            {2, "subdir/000000.dat", filename:join(?ROOT, "subdir/000000.dat"), 20},
            {3, "a/b/c/file", filename:join(?ROOT, "a/b/c/file"), 30}
        ]},
        wa_raft_transport:resolve_transport_files(?ROOT, Files)
    ),
    ok.

-spec traversal_paths_rejected(Config :: ct_suite:ct_config()) -> ok.
traversal_paths_rejected(_Config) ->
    [
        ?assertEqual(
            {error, invalid_file_path},
            wa_raft_transport:resolve_transport_files(?ROOT, [{1, Path, 10}])
        )
     || Path <- ["..", "../x", "../../../../tmp/x", "a/../../b", "subdir/../../etc/passwd"]
    ],
    %% A safe file mixed with an unsafe file still fails the whole transport.
    ?assertEqual(
        {error, invalid_file_path},
        wa_raft_transport:resolve_transport_files(?ROOT, [
            {1, "ok.dat", 10},
            {2, "../escape", 20}
        ])
    ),
    ok.

-spec absolute_paths_rejected(Config :: ct_suite:ct_config()) -> ok.
absolute_paths_rejected(_Config) ->
    [
        ?assertEqual(
            {error, invalid_file_path},
            wa_raft_transport:resolve_transport_files(?ROOT, [{1, Path, 10}])
        )
     || Path <- ["/tmp/x", "/etc/passwd", "/"]
    ],
    ok.

-spec worker_failures(Config :: ct_suite:ct_config()) -> ok.
worker_failures(_Config) ->
    Owner = self(),
    ok = mock_transport(),

    ?assertEqual({stop, transport_not_found}, wa_raft_transport_worker:init({0, ?MODULE})),
    ok = application:set_env(wa_raft, transport_worker_test_behavior, init_stop),
    ?assertEqual({stop, init_failed}, wa_raft_transport_worker:init({1, ?MODULE})),

    PreviousTrapExit = process_flag(trap_exit, true),
    try
        ok = application:set_env(wa_raft, transport_worker_test_behavior, send_stop),
        ok = assert_worker_failure(2, {stop, send_failed}, {shutdown, send_failed}),
        ok = application:set_env(wa_raft, transport_worker_test_behavior, send_exception),
        ok = assert_worker_failure(3, {error, send_crash}, {shutdown, {error, send_crash}}),

        InitCounter = atomics:new(1, []),
        ok = application:set_env(wa_raft, transport_worker_test_behavior, {init_fail_after, InitCounter, 1}),
        ok = application:set_env(wa_raft, transport_worker_test_controller, Owner),
        {ok, Supervisor} = wa_raft_transport_sup:start_link(),
        ?assertEqual({error, init_failed}, wa_raft_transport_sup:start_workers(4, ?MODULE, 2)),
        ?assertEqual(2, atomics:get(InitCounter, 1)),
        ?assertEqual([], supervisor:which_children(wa_raft_transport_sup)),

        ok = wa_raft_dist_transport:transport_start(5),
        DistWorker = receive_ready_worker(5),
        ?assertEqual(1, length(supervisor:which_children(wa_raft_transport_sup))),
        ok = supervisor:terminate_child(wa_raft_transport_sup, DistWorker),
        ?assertEqual([], supervisor:which_children(wa_raft_transport_sup)),
        ok = gen_server:stop(Supervisor)
    after
        process_flag(trap_exit, PreviousTrapExit)
    end.

-spec worker_shutdown(Config :: ct_suite:ct_config()) -> ok.
worker_shutdown(_Config) ->
    ok = mock_transport(),
    ok = application:unset_env(wa_raft, transport_worker_test_controller),
    ok = application:set_env(wa_raft, transport_worker_test_behavior, send_continue),
    PreviousTrapExit = process_flag(trap_exit, true),
    try
        {ok, Supervisor} = wa_raft_transport_sup:start_link(),

        ok = wa_raft_transport_sup:start_workers(1, ?MODULE, 1),
        Shutdown = receive_sending_worker(1),
        ok = supervisor:terminate_child(wa_raft_transport_sup, Shutdown),
        ?assertEqual({transport_complete, Shutdown, 1, 1, {stop, shutdown}}, receive_message()),
        ?assertEqual({transport_terminate, Shutdown, shutdown}, receive_message()),

        ok = wa_raft_transport_sup:start_workers(2, ?MODULE, 1),
        Linked = receive_sending_worker(2),
        ok = linked_exit(Linked, normal),
        _ = sys:get_state(Linked),
        ?assertEqual(timeout, receive_message()),
        Monitor = monitor(process, Linked),
        ok = linked_exit(Linked, linked_crash),
        ?assertEqual({transport_complete, Linked, 2, 1, {stop, linked_crash}}, receive_message()),
        ?assertEqual({transport_terminate, Linked, linked_crash}, receive_message()),
        ?assertEqual({'DOWN', Monitor, process, Linked, linked_crash}, receive_message()),
        ok = gen_server:stop(Supervisor)
    after
        process_flag(trap_exit, PreviousTrapExit)
    end.

-spec mock_transport() -> ok.
mock_transport() ->
    Owner = self(),
    ok = meck:new(wa_raft_transport, [passthrough]),
    ok = meck:expect(wa_raft_transport, transport_info, fun
        (0) ->
            not_found;
        (_ID) ->
            {ok, #{
                peer => node(),
                module => ?MODULE,
                meta => #{type => transfer, table => test, partition => 1}
            }}
    end),
    ok = meck:expect(wa_raft_transport, next_file, fun(_ID) -> {ok, 1} end),
    ok = meck:expect(wa_raft_transport, start_file, fun(_ID, _FileID) -> ok end),
    ok = meck:expect(wa_raft_transport, complete_file, fun(ID, FileID, Status) ->
        Owner ! {transport_complete, self(), ID, FileID, Status},
        ok
    end),
    ok = application:set_env(wa_raft, transport_worker_test_owner, Owner).

-spec receive_sending_worker(wa_raft_transport:transport_id()) -> pid().
receive_sending_worker(ID) ->
    receive
        {transport_sending, Pid, ID} -> Pid
    after 1000 ->
        ct:fail({transport_worker_not_sending, ID})
    end.

-spec linked_exit(pid(), Reason :: term()) -> ok.
linked_exit(Worker, Reason) ->
    {Pid, Monitor} = spawn_monitor(fun() ->
        link(Worker),
        exit(Reason)
    end),
    receive
        {'DOWN', Monitor, process, Pid, Reason} -> ok
    after 1000 ->
        ct:fail({linked_process_not_down, Reason})
    end.

-spec assert_worker_failure(
    wa_raft_transport:transport_id(),
    CompletionStatus :: term(),
    ExitReason :: term()
) -> ok.
assert_worker_failure(ID, CompletionStatus, ExitReason) ->
    {ok, Pid} = wa_raft_transport_worker:start_link(ID, ?MODULE),
    ?assertEqual(
        {transport_complete, Pid, ID, 1, CompletionStatus},
        receive_message()
    ),
    ?assertEqual(
        {transport_terminate, Pid, ExitReason},
        receive_message()
    ),
    ?assertEqual(
        {'EXIT', Pid, ExitReason},
        receive_message()
    ),
    ok.

-spec receive_ready_worker(wa_raft_transport:transport_id()) -> pid().
receive_ready_worker(ID) ->
    receive
        {transport_worker_ready, Pid, ID} -> Pid
    after 1000 ->
        ct:fail({transport_worker_not_ready, ID})
    end.

-spec receive_message() -> term().
receive_message() ->
    receive
        Message -> Message
    after 1000 ->
        timeout
    end.

-spec transport_init(node()) -> {ok, test_state} | {stop, init_failed}.
% elp:ignore W0008 (transport behavior callback, not a Common Test case)
transport_init(_Peer) ->
    case application:get_env(wa_raft, transport_worker_test_behavior) of
        {ok, init_stop} ->
            {stop, init_failed};
        {ok, {init_fail_after, Counter, SuccessfulStarts}} ->
            case atomics:add_get(Counter, 1, 1) =< SuccessfulStarts of
                true -> {ok, test_state};
                false -> {stop, init_failed}
            end;
        _ ->
            {ok, test_state}
    end.

-spec transport_send(
    wa_raft_transport:transport_id(),
    wa_raft_transport:file_id(),
    State :: test_state | sending
) -> {ok, test_state | sending} | {continue, sending} | {stop, send_failed, test_state | sending}.
transport_send(ID, _FileID, State) ->
    case application:get_env(wa_raft, transport_worker_test_behavior) of
        {ok, send_continue} when State =:= test_state ->
            {ok, Owner} = application:get_env(wa_raft, transport_worker_test_owner),
            Owner ! {transport_sending, self(), ID},
            {continue, sending};
        {ok, send_continue} ->
            {continue, sending};
        {ok, send_stop} ->
            {stop, send_failed, State};
        {ok, send_exception} ->
            erlang:error(send_crash);
        _ ->
            {ok, State}
    end.

-spec transport_terminate(Reason :: term(), State :: test_state | sending) -> ok.
transport_terminate(Reason, _State) ->
    {ok, Owner} = application:get_env(wa_raft, transport_worker_test_owner),
    Owner ! {transport_terminate, self(), Reason},
    ok.
