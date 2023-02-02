%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Application supervisor to be started by the wa_raft application for
%%% supervising services and resources shared between application-started
%%% RAFT processes.

-module(wa_raft_app_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% API
-export([
    start_link/0
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

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Arg :: term()) -> {ok, {supervisor:sup_flags(), list(supervisor:child_spec())}}.
init(_) ->
    % Cache certain commonly used configuration values.
    case application:get_env(?APP, raft_metrics_module) of
        {ok, Module} -> wa_raft_metrics:install(Module);
        _Other       -> ok
    end,

    % Setup tables used by shared services.
    wa_raft_info:init_tables(),
    wa_raft_transport:setup_tables(),
    wa_raft_log_catchup:init_tables(),

    % Setup shared resources
    init_globals(),

    % Configure startup of shared services.
    ChildSpecs = [
        wa_raft_transport:child_spec(),
        wa_raft_transport_sup:child_spec(),
        wa_raft_dist_transport:child_spec(),
        wa_raft_snapshot_catchup:child_spec()
    ],

    {ok, {#{strategy => one_for_one, intensity => 5, period => 1}, lists:flatten(ChildSpecs)}}.

%%-------------------------------------------------------------------
%% Test API
%%-------------------------------------------------------------------

-spec init_globals() -> ok.
init_globals() ->
    persistent_term:put(?RAFT_COUNTERS, counters:new(?RAFT_NUMBER_OF_GLOBAL_COUNTERS, [atomics])),
    ok.
