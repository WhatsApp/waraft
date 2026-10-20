%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_sup).

-moduledoc """
OTP supervisor for handling workers responsible for actual data
send and receive for RAFT transport mechanisms.
""".

-compile(warn_missing_spec_all).
-behaviour(supervisor).

%% Internal API
-export([
    start_worker/2,
    start_workers/3
]).

%% OTP supervision
-export([
    child_spec/0,
    start_link/0
]).

%% Supervisor
-export([
    init/1
]).

%%% ------------------------------------------------------------------------
%%%  Internal API
%%%

-spec start_worker(ID :: wa_raft_transport:transport_id(), Module :: module()) -> supervisor:startchild_ret().
start_worker(ID, Module) ->
    supervisor:start_child(?MODULE, [ID, Module]).

-spec start_workers(ID :: wa_raft_transport:transport_id(), Module :: module(), Count :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
start_workers(ID, Module, Count) ->
    start_workers(ID, Module, Count, []).

-spec start_workers(
    ID :: wa_raft_transport:transport_id(),
    Module :: module(),
    Count :: non_neg_integer(),
    Started :: [pid()]
) -> ok | {error, Reason :: term()}.
start_workers(_, _, Count, _) when Count =< 0 ->
    ok;
start_workers(ID, Module, Count, Started) ->
    case start_worker(ID, Module) of
        {ok, Pid} when is_pid(Pid) ->
            start_workers(ID, Module, Count - 1, [Pid | Started]);
        {ok, undefined} ->
            terminate_workers(Started),
            {error, worker_ignored};
        {error, _} = Error ->
            terminate_workers(Started),
            Error
    end.

-spec terminate_workers([pid()]) -> ok.
terminate_workers(Started) ->
    [supervisor:terminate_child(?MODULE, Pid) || Pid <- Started],
    ok.

%%% ------------------------------------------------------------------------
%%%  OTP supervision
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
%%%  Supervisor
%%%

-spec init(term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 5, period => 1}, [wa_raft_transport_worker:child_spec()]}}.
