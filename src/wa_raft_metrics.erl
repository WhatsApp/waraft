%% @format
%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Pluggable metrics interface to allow integration with different metrics system.
%%% The default implementation skips metrics logging and does nothing.

-module(wa_raft_metrics).
-compile(warn_missing_spec_all).

%% Public API
-export([
    install/1
]).

%% Default Implementation
-export([
    count/1,
    countv/2,
    gather/2,
    gather_latency/2
]).

%% Public Types
-export_type([
    metric/0,
    value/0
]).

-include_lib("wa_raft/include/wa_raft.hrl").

%%-------------------------------------------------------------------
%% RAFT Metrics Behaviour
%%-------------------------------------------------------------------

%% Report a single occurence of some metric.
-callback count(metric()) -> ok.
%% Report a number of occurences of some metric.
-callback countv(metric(), value()) -> ok.
%% Report the measured value of an occurence of some metric.
-callback gather(metric(), value()) -> ok.
%% Report the measured latency of an occurence of some metric.
-callback gather_latency(metric(), value()) -> ok.

%%-------------------------------------------------------------------
%% Public Types
%%-------------------------------------------------------------------

-type metric() :: atom() | tuple().
-type value() :: integer().

%%-------------------------------------------------------------------
%% Public API
%%-------------------------------------------------------------------

%% Replace the previously installed or default module used to report
%% RAFT metrics with the provided module.
-spec install(Module :: module()) -> ok.
install(Module) ->
    persistent_term:put(?RAFT_METRICS_MODULE_KEY, Module).

%%-------------------------------------------------------------------
%% Default Implementation
%%-------------------------------------------------------------------

-spec count(metric()) -> ok.
count(_Metric) ->
    ok.

-spec countv(metric(), value()) -> ok.
countv(_Metric, _Value) ->
    ok.

-spec gather(metric(), value()) -> ok.
gather(_Metric, _Value) ->
    ok.

-spec gather_latency(metric(), value()) -> ok.
gather_latency(_Metric, _Value) ->
    ok.
