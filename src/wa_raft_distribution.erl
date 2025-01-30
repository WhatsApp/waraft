%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Pluggable distribution interface. The default implementation uses Erlang
%%% distribution.

-module(wa_raft_distribution).
-compile(warn_missing_spec_all).

-export([
    cast/3,
    call/4,
    reply/3
]).

-include("wa_raft.hrl").

-type dest_addr() :: {Name :: atom(), Node :: node()}.

-export_type([
    dest_addr/0
]).

%%% ------------------------------------------------------------------------
%%%  Behaviour callbacks
%%%

-callback cast(dest_addr(), #raft_identifier{}, term()) -> term().
-callback call(dest_addr(), #raft_identifier{}, term(), integer() | infinity) -> term().
-callback reply(gen_server:from() | gen_statem:from(), #raft_identifier{}, term()) -> term().

%%% ------------------------------------------------------------------------
%%%  Erlang distribution default implementation
%%%

-spec cast(DestAddr :: dest_addr(), Identifier :: #raft_identifier{}, Message :: term()) -> term().
cast(DestAddr, _Identifier, Message) ->
    erlang:send(DestAddr, {'$gen_cast', Message}, [noconnect, nosuspend]).

-spec call(DestAddr :: dest_addr(), Identifier :: #raft_identifier{}, Message :: term(), Timeout :: integer() | infinity) -> term().
call(DestAddr, _Identifier, Message, Timeout) ->
    gen_server:call(DestAddr, Message, Timeout).

-spec reply(From :: gen_server:from() | gen_statem:from(), Identifier :: #raft_identifier{}, Reply :: term()) -> term().
reply(From, _Identifier, Reply) ->
    gen:reply(From, Reply).
