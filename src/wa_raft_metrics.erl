%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Pluggable metrics interface to allow integration with different metrics system.
%%% The default implementation skips metrics logging and does nothing.

-module(wa_raft_metrics).
-compile(warn_missing_spec).
-author('huiliu@fb.com').

-export([
  count/1,
  countv/2,
  gather/2
]).

-export_type([
  metric/0,
  value/0
]).

-include("wa_raft.hrl").

-type metric() :: atom() | tuple().
-type value() :: integer().

-callback count(metric()) -> ok.
-callback countv(metric(), value()) -> ok.
-callback gather(metric(), value()) -> ok.

-spec count(metric()) -> ok.
count(_Metric) ->
    ok.

-spec countv(metric(), value()) -> ok.
countv(_Metric, _Value) ->
    ok.

-spec gather(metric(), value()) -> ok.
gather(_Metric, _Value) ->
    ok.
