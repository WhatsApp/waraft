%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Top level OTP Supervisor for monitoring ALL raft processes.

-module(wa_raft_sup).
-compile(warn_missing_spec).
-author("shobhitg@fb.com").

-behaviour(supervisor).

%% API
-export([
    child_spec/1,
    start_link/1,
    init/1
]).

-export([
    init_globals/0
]).

-include("wa_raft.hrl").

-spec child_spec(RaftArgs :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(RaftArgs) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [RaftArgs]},
        restart => permanent,
        shutdown => infinity,
        modules => [?MODULE]
    }.

-spec start_link([term()]) -> {ok, Pid :: pid()} | ignore | wa_raft:error().
start_link(RaftArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, RaftArgs).

-spec init([wa_raft:args()]) -> {ok, {supervisor:sup_flags(), list(supervisor:child_spec())}}.
init(RaftArgs) ->
    init_globals(),
    wa_raft_info:init_tables(),
    wa_raft_transport:setup_tables(),
    ChildSpecs = [
        wa_raft_transport:child_spec(),
        wa_raft_transport_sup:child_spec(),
        wa_raft_dist_transport:child_spec(),
        wa_raft_part_top_sup:child_spec(RaftArgs)
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 1}, lists:flatten(ChildSpecs)}}.

-spec init_globals() -> ok.
init_globals() ->
    % it may trigger GC for all processes, but we only do it only during app start
    persistent_term:put(?RAFT_COUNTERS, counters:new(?RAFT_NUMBER_OF_GLOBAL_COUNTERS, [atomics])),
    persistent_term:put(raft_metrics_module, ?RAFT_CONFIG(raft_metrics_module, wa_raft_metrics)),
    persistent_term:put(raft_distribution_module, ?RAFT_CONFIG(raft_distribution_module, wa_raft_distribution)),
    ok.
