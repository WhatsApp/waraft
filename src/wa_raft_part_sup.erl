%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% OTP Supervisor for monitoring RAFT processes. Correctness of RAFT
%%% relies on the consistency of the signaling between the processes,
%%% this supervisor is configured to restart all RAFT processes
%%% when any of them exits abnormally.

-module(wa_raft_part_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% OTP Supervision
-export([
    child_spec/0,
    start_link/1
]).

%% Internal API
-export([
    raft_sup/2
]).

%% Supervisor callbacks
-export([
    init/1
]).

-include("wa_raft.hrl").

%% Returns a spec suitable for use with a `simple_one_for_one` supervisor.
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

-spec start_link(Spec :: wa_raft:args()) -> supervisor:startlink_ret().
start_link(#{table := Table, partition := Partition} = RaftSpec) ->
    supervisor:start_link({local, raft_sup(Table, Partition)}, ?MODULE, RaftSpec).

-spec init(wa_raft:args()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Args) ->
    StorageModule = application:get_env(?APP, raft_storage_module, wa_raft_ast),
    code:ensure_loaded(StorageModule),
    Modules0 = case erlang:function_exported(StorageModule, child_spec, 1) of
                   true ->
                       [StorageModule];
                   _ ->
                       []
               end,
    Modules = Modules0 ++ [
        wa_raft_queue,
        wa_raft_storage,
        wa_raft_log,
        wa_raft_log_catchup,
        wa_raft_server,
        wa_raft_acceptor
    ],
    Specs = [M:child_spec(Args) || M <- Modules],
    {ok, {#{strategy => one_for_all, intensity => 10, period => 1}, Specs}}.

-spec raft_sup(wa_raft:table(), wa_raft:partition()) -> atom().
raft_sup(Table, Partition) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).
