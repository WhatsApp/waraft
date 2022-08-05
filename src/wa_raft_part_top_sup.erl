%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% OTP Supervisor for monitoring supervisor of each raft partition processes.
%%% This supervisor is a simple_one_for_one supervisor that will stop each partition in parallel.
%%% The supervisor is flexible enough to start new raft partition if desired.

-module(wa_raft_part_top_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

-export([
    child_spec/1
]).

%% API
-export([
    start_link/1
]).

%% Supervisor callbacks
-export([
    init/1
]).

-spec child_spec([wa_raft:args()]) -> supervisor:child_spec().
child_spec(RaftArgs) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [RaftArgs]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link([term()]) -> supervisor:startlink_ret().
start_link(RaftArgs) ->
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok, _} = Result ->
            lists:foreach(
                fun(#{table := Table, partition := Partition} = RaftArg) ->
                    Name = wa_raft_part_sup:raft_sup(Table, Partition),
                    supervisor:start_child(?MODULE, [Name, RaftArg])
                end,
                RaftArgs),
            Result;
        Else ->
            Else
    end.

-spec init([]) -> {ok, {supervisor:sup_flags(), list(supervisor:child_spec())}}.
init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 10,
                 period => 1},
    ChildSpecs = [#{id => wa_raft_part_sup,
                    start => {wa_raft_part_sup, start_link, []},
                    shutdown => infinity}],
    {ok, {SupFlags, ChildSpecs}}.
