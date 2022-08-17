%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Supervisor responsible for managing workers responsible for the
%%% transport to a particular target node.

-module(wa_raft_transport_target_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% Internal API
-export([
    name/1
]).

%% OTP supervision callbacks
-export([
    child_spec/1,
    start_link/1
]).

%% Supervisor callbacks
-export([
    init/1
]).

-include("wa_raft.hrl").

-define(RAFT_TRANSPORT_THREADS(), application:get_env(?APP, raft_transport_threads, 1)).

%%% ------------------------------------------------------------------------
%%%  Internal API
%%%

-spec name(node()) -> atom().
name(Name) ->
    list_to_atom("raft_transport_target_sup_" ++ atom_to_list(Name)).

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%

-spec child_spec(node()) -> supervisor:child_spec().
child_spec(Node) ->
    #{
        id => Node,
        start => {?MODULE, start_link, [Node]},
        restart => temporary,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link(node()) -> supervisor:startlink_ret().
start_link(Node) ->
    supervisor:start_link({local, name(Node)}, ?MODULE, Node).

%%% ------------------------------------------------------------------------
%%%  supervisor callbacks
%%%

-spec init(node()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Node) ->
    NumThreads = ?RAFT_TRANSPORT_THREADS(),
    Specs = [wa_raft_transport_worker:child_spec(Node, N) || N <- lists:seq(1, NumThreads)],
    {ok, {#{strategy => one_for_all, intensity => 5, period => 1}, Specs}}.
