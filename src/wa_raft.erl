%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file defines dialyzer types.

-module(wa_raft).
-compile(warn_missing_spec).
-behaviour(application).

-export([
    start/2,
    stop/1
]).

-export_type([
    table/0,
    partition/0,
    error/0,
    args/0
]).

-type table() :: atom().
-type partition() :: pos_integer().
-type error() :: {error, term()}.

-type args() ::
    #{
        % Table name
        table := table(),
        % Partition number
        partition := partition(),
        % Log module
        log_module => module(),
        % Storage module
        storage_module => module(),
        % Witness flag
        witness => boolean()
    }.

-spec start(Type :: application:start_type(), Args :: term()) -> {ok, Pid :: pid()}.
start(normal, _Args) ->
    {ok, _Pid} = wa_raft_global_sup:start_link().

-spec stop(State :: term()) -> term().
stop(_State) ->
    ok.
