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
    child_spec/1,
    start_link/2
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

%%-------------------------------------------------------------------
%% OTP supervision
%%-------------------------------------------------------------------

%% Returns a spec suitable for use with a `simple_one_for_one` supervisor.
-spec child_spec(Application :: atom()) -> supervisor:child_spec().
child_spec(Application) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Application]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link(Application :: atom(), Spec :: wa_raft:args()) -> supervisor:startlink_ret().
start_link(Application, Spec) ->
    #{table := Table, partition := Partition} = Options = normalize_spec(Application, Spec),
    supervisor:start_link({local, raft_sup(Table, Partition)}, ?MODULE, Options).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

-spec raft_sup(wa_raft:table(), wa_raft:partition()) -> atom().
raft_sup(Table, Partition) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

-spec normalize_spec(Application :: atom(), Spec :: wa_raft:args()) -> wa_raft:options().
normalize_spec(Application, #{table := Table, partition := Partition} = Spec) ->
    % TODO(hsun324) - T133215915: Application-specific default log/storage module
    #{
        application => Application,
        table => Table,
        partition => Partition,
        witness => maps:get(witness, Spec, false),
        log_module => maps:get(log_module, Spec, application:get_env(?APP, raft_log_module, wa_raft_log_ets)),
        storage_module => maps:get(storage_module, Spec, application:get_env(?APP, raft_storage_module, wa_raft_storage_ets))
    }.

%%-------------------------------------------------------------------
%% Supervisor callbacks
%%-------------------------------------------------------------------

-spec init(Options :: wa_raft:options()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    ChildSpecs = [
        wa_raft_queue:child_spec(Options),
        wa_raft_storage:child_spec(Options),
        wa_raft_log:child_spec(Options),
        wa_raft_log_catchup:child_spec(Options),
        wa_raft_server:child_spec(Options),
        wa_raft_acceptor:child_spec(Options)
    ],
    {ok, {#{strategy => one_for_all, intensity => 10, period => 1}, ChildSpecs}}.
