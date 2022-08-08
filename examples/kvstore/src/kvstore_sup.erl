%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

%%%
%%% This module creates 4 raft instances on single server.
%%%
-module(kvstore_sup).
-compile(warn_missing_spec).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

-include_lib("wa_raft/include/wa_raft.hrl").

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    Args = raft_args([1, 2, 3, 4]),
    ChildSpecs = wa_raft_sup:child_spec(Args),
    {ok, {#{}, [ChildSpecs]}}.

%% Return raft arguments for all partitions
-spec raft_args([pos_integer()]) -> [wa_raft:args()].
raft_args(Partitions) ->
  [raft_arg(P) || P <- Partitions].

%% Return raft arguments for given partition
-spec raft_arg(pos_integer()) -> wa_raft:args().
raft_arg(Partition) ->
  #{
      table => kvstore,
      partition => Partition,
      %% Use single node cluster. You can add more nodes to create a raft cluster.
      nodes => [node()],
      %% Use in-memory log
      log_module => wa_raft_log_ets,
      %% Use in-memory local storage
      %% You can switch to local filesystem by using wa_raft_storage_fs as storage module
      storage_module => wa_raft_storage_ets
  }.
