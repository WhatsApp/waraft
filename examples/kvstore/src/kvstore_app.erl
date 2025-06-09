%% @format
%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(kvstore_app).
-compile(warn_missing_spec_all).

-behaviour(application).

%% API
-export([
    start/2,
    stop/1
]).

-spec start(application:start_type(), term()) -> {ok, pid()}.
start(normal, _Args) ->
    {ok, _Pid} = kvstore_sup:start_link().

-spec stop(term()) -> ok.
stop(_State) ->
    ok.
