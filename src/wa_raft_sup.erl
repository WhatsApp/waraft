%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Supervisor for supervising RAFT partitions started by a client application.
%%% As a `simple_one_for_one` supervisor, this supervisor can dynamically
%%% start and stop partitions and will stop partitions in parallel during
%%% shutdown.

-module(wa_raft_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% OTP supervision
-export([
    child_spec/1,
    child_spec/2,
    start_link/2
]).

%% API
-export([
    start_partition/2,
    start_partition_under_application/2,
    stop_partition/2,
    stop_partition/3,
    stop_partition_under_application/2,
    stop_partition_under_application/3
]).

%% Internal API
-export([
    reg_name/1
]).

%% Supervisor callbacks
-export([
    init/1
]).

%% Test API
-export([
    init_globals/0
]).

-include("wa_raft.hrl").

%%-------------------------------------------------------------------
%% OTP supervision
%%-------------------------------------------------------------------

-spec child_spec(Specs :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(Specs) ->
    {ok, Application} = application:get_application(),
    child_spec(Application, Specs).

-spec child_spec(Application :: atom(), Specs :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(Application, RaftArgs) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Application, RaftArgs]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link(Application :: atom(), Specs :: [wa_raft:args()]) -> supervisor:startlink_ret().
start_link(Application, RaftArgs) ->
    case supervisor:start_link({local, reg_name(Application)}, ?MODULE, Application) of
        {ok, Pid} = Result ->
            lists:foreach(fun (Spec) -> start_partition(Pid, Spec) end, RaftArgs),
            Result;
        Else ->
            Else
    end.

%%-------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------

-spec start_partition(Supervisor :: atom() | pid(), Spec :: wa_raft:args()) -> supervisor:startchild_ret().
start_partition(Supervisor, Spec) ->
    supervisor:start_child(Supervisor, [Spec]).

-spec start_partition_under_application(Application :: atom(), Spec :: wa_raft:args()) -> supervisor:startchild_ret().
start_partition_under_application(Application, Spec) ->
    start_partition(reg_name(Application), Spec).

-spec stop_partition(Supervisor :: atom() | pid(), Pid :: pid()) -> ok | {error, atom()}.
stop_partition(Supervisor, Pid) ->
    supervisor:terminate_child(Supervisor, Pid).

-spec stop_partition(Supervisor :: atom() | pid(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok | {error, atom()}.
stop_partition(Supervisor, Table, Partition) ->
    case whereis(wa_raft_part_sup:raft_sup(Table, Partition)) of
        Pid when is_pid(Pid) -> stop_partition(Supervisor, Pid);
        _                    -> {error, not_found}
    end.

-spec stop_partition_under_application(Application :: atom(), Pid :: pid()) -> ok | {error, atom()}.
stop_partition_under_application(Application, Pid) ->
    stop_partition(reg_name(Application), Pid).

-spec stop_partition_under_application(Application :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok | {error, atom()}.
stop_partition_under_application(Application, Table, Partition) ->
    stop_partition(reg_name(Application), Table, Partition).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

-spec reg_name(Application :: atom()) -> atom().
reg_name(Application) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Application)).

%%-------------------------------------------------------------------
%% Supervisor callbacks
%%-------------------------------------------------------------------

-spec init(Application :: atom()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Application) ->
    init_globals(),
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 1}, [wa_raft_part_sup:child_spec(Application)]}}.

%%-------------------------------------------------------------------
%% Test API
%%-------------------------------------------------------------------

-spec init_globals() -> ok.
init_globals() ->
    % TODO(hsun324) - T133215915: support multi-app usage by caching in an app-specific manner
    persistent_term:put(?RAFT_COUNTERS, counters:new(?RAFT_NUMBER_OF_GLOBAL_COUNTERS, [atomics])),
    persistent_term:put(raft_metrics_module, ?RAFT_CONFIG(raft_metrics_module, wa_raft_metrics)),
    persistent_term:put(raft_distribution_module, ?RAFT_CONFIG(raft_distribution_module, wa_raft_distribution)),
    ok.
