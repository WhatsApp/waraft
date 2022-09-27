%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% The top-level OTP supervisor started as part of the wa_raft application
%%% that supervises all core RAFT processes and services that are shared
%%% between RAFT partitions started by dependent applications.

-module(wa_raft_global_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% OTP supervision
-export([
    child_spec/0,
    start_link/0
]).

%% Supervisor callbacks
-export([
    init/1
]).

-ifdef(TEST).
-export([
    init_globals/0
]).
-endif.

-include("wa_raft.hrl").

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => infinity,
        modules => [?MODULE]
    }.

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), list(supervisor:child_spec())}}.
init([]) ->
    init_globals(),
    wa_raft_info:init_tables(),
    wa_raft_transport:setup_tables(),

    ChildSpecs = [
        wa_raft_transport:child_spec(),
        wa_raft_transport_sup:child_spec(),
        wa_raft_dist_transport:child_spec()
    ],

    {ok, {#{strategy => one_for_one, intensity => 5, period => 1}, ChildSpecs}}.

-spec init_globals() -> ok.
init_globals() ->
    % it may trigger GC for all processes, but we only do it only during app start
    persistent_term:put(?RAFT_COUNTERS, counters:new(?RAFT_NUMBER_OF_GLOBAL_COUNTERS, [atomics])),
    persistent_term:put(raft_metrics_module, ?RAFT_CONFIG(raft_metrics_module, wa_raft_metrics)),
    % TODO(hsun324): distribution module is better as an app-specific config
    persistent_term:put(raft_distribution_module, ?RAFT_CONFIG(raft_distribution_module, wa_raft_distribution)),
    ok.
