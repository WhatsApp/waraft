%% @format
%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Application implementation for wa_raft.

-module(wa_raft_app).
-compile(warn_missing_spec_all).
-behaviour(application).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

-spec start(StartType :: application:start_type(), StartArgs :: term()) -> {ok, pid()}.
start(normal, _Args) ->
    {ok, _Pid} = wa_raft_app_sup:start_link().

-spec stop(State :: term()) -> ok.
stop(_State) ->
    ok.
