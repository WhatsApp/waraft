%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% OTP supervisor for handling workers responsible for actual data
%%% send and receive for RAFT transport mechanisms.

-module(wa_raft_transport_sup).
-compile(warn_missing_spec).
-author("hsun324@whatsapp.com").

-behaviour(supervisor).

%% OTP supervision callbacks
-export([
    child_spec/0,
    start_link/0
]).

%% supervisor callbacks
-export([
    init/1
]).

-include("wa_raft.hrl").

-define(RAFT_TRANSPORT_THREADS(), application:get_env(?APP, raft_transport_threads, 1)).

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%% ------------------------------------------------------------------------
%%%  supervisor callbacks
%%%

-spec init(any()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_) ->
    NumThreads = ?RAFT_TRANSPORT_THREADS(),
    Specs = [wa_raft_transport_worker:child_spec(N) || N <- lists:seq(1, NumThreads)],
    {ok, {#{strategy => one_for_all, intensity => 5, period => 1}, Specs}}.
