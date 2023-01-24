%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

%%%
%%% This supervisor starts 4 RAFT partitions under itself.
%%%

-module(kvstore_sup).
-compile(warn_missing_spec).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    Partitions = [1, 2, 3, 4],
    Args = [raft_args(P) || P <- Partitions],
    ChildSpecs = wa_raft_sup:child_spec(Args),
    {ok, {#{}, [ChildSpecs]}}.

%% Return raft arguments for the provided partition
-spec raft_args(wa_raft:partition()) -> wa_raft:args().
raft_args(Partition) ->
    #{
        %% Table name and partition uniquely identify a RAFT partition
        table => kvstore,
        partition => Partition,
        %% Use in-memory log (Implement your own to meet your needs)
        log_module => wa_raft_log_ets,
        %% Use in-memory local storage (Implement your own to meet your needs)
        storage_module => wa_raft_storage_ets
    }.
