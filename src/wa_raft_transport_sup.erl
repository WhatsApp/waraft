%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% OTP supervisor for handling workers responsible for actual data
%%% send and receive for RAFT transport mechanisms.

-module(wa_raft_transport_sup).
-compile(warn_missing_spec_all).
-behaviour(supervisor).

%% Internal API
-export([
    get_or_start/1
]).

%% OTP supervision callbacks
-export([
    child_spec/0,
    start_link/0
]).

%% supervisor callbacks
-export([
    init/1
]).

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%

-spec get_or_start(node()) -> atom().
get_or_start(Node) ->
    Name = wa_raft_transport_target_sup:name(Node),
    not is_pid(whereis(Name)) andalso
        supervisor:start_child(?MODULE, wa_raft_transport_target_sup:child_spec(Node)),
    Name.

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

-spec init(term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 1}, []}}.
