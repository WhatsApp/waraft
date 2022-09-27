%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% The OTP supervisor for supervising the RAFT partitions started on
%%% behalf of a dependent application as part of a single call to start
%%% partitions or a single RAFT bridge.
%%%
%%% This supervisor is a simple_one_for_one supervisor that will start
%%% each partition in sequence in `start_link` but stop each partition in
%%% parallel.
%%%
%%% The supervisor is flexible enough to start new RAFT partitions or stop
%%% existing RAFT partitions if desired.

-module(wa_raft_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% OTP supervision
-export([
    child_spec/1,
    child_spec/2,
    start_link/2
]).

%% Public API
-export([
    reg_name/1,
    add_partition/2,
    remove_partition/3
]).

%% Supervisor callbacks
-export([
    init/1
]).

-include("wa_raft.hrl").

-spec child_spec(InitialRaftArgsList :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(InitialRaftArgsList) ->
    {ok, Application} = application:get_application(),
    child_spec(Application, InitialRaftArgsList).

-spec child_spec(Application :: atom(), InitialRaftArgsList :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(Application, InitialRaftArgsList) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Application, InitialRaftArgsList]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link(Application :: atom(), InitialRaftArgsList :: [wa_raft:args()]) -> supervisor:startlink_ret().
start_link(Application, InitialRaftArgsList) ->
    case supervisor:start_link({local, reg_name(Application)}, ?MODULE, []) of
        {ok, InstancePid} = Result ->
            [{ok, _} = add_partition(InstancePid, RaftArgs) || RaftArgs <- InitialRaftArgsList],
            Result;
        Else ->
            Else
    end.

-spec reg_name(Application :: atom()) -> RegName :: atom().
reg_name(Application) ->
    ?RAFT_INSTANCE_SUP_NAME(Application).

-spec add_partition(SupervisorSpec :: pid() | atom(), RaftArgs :: wa_raft:args()) -> supervisor:startchild_ret().
add_partition(SupervisorSpec, RaftArgs) ->
    supervisor:start_child(SupervisorSpec, [RaftArgs]).

-spec remove_partition(SupervisorSpec :: pid() | atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok | {error, not_found | simple_one_for_one}.
remove_partition(SupervisorSpec, Table, Partition) ->
    case whereis(?RAFT_PART_SUP_NAME(Table, Partition)) of
        Pid when is_pid(Pid) -> supervisor:terminate_child(SupervisorSpec, Pid);
        _                    -> {error, not_found}
    end.

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 1}, [
        wa_raft_part_sup:child_spec()
    ]}}.
