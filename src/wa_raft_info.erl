%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Module to interface with raft metadata

-module(wa_raft_info).
-compile(warn_missing_spec).
-author('gbhaska@fb.com').

-export([
    init_tables/0,
    set_state/3,
    get_state/2,
    delete_state/2,
    set_leader/3,
    get_leader/2,
    set_stale/2,
    get_stale/2
]).

-include("wa_raft.hrl").

%% Raft leader node
-define(RAFT_LEADER_NODE(Table, Partition), {leader, Table, Partition}).
%% Raft server state
-define(RAFT_SERVER_STATE(Table, Partition), {state, Table, Partition}).
%% Raft stale flag
-define(RAFT_STALE_FLAG(Table, Partition), {stale, Table, Partition}).

-spec init_tables() -> ok.
init_tables() ->
    ets:new(?MODULE, [set, public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

-spec set_leader(wa_raft:table(), wa_raft:partition(), node()) -> true.
set_leader(Table, Partition, Value) ->
    ets:update_element(?MODULE, ?RAFT_LEADER_NODE(Table, Partition), {2, Value})
        orelse ets:insert(?MODULE, {?RAFT_LEADER_NODE(Table, Partition), Value}).

-spec get_leader(wa_raft:table(), wa_raft:partition()) -> node() | undefined.
get_leader(Table, Partition) ->
    try
        ets:lookup_element(?MODULE, ?RAFT_LEADER_NODE(Table, Partition), 2)
    catch error:badarg ->
        undefined
    end.

-spec set_state(wa_raft:table(), wa_raft:partition(), wa_raft_server:state()) -> true.
set_state(Table, Partition, State) ->
    ets:update_element(?MODULE, ?RAFT_SERVER_STATE(Table, Partition), {2, State})
        orelse ets:insert(?MODULE, {?RAFT_SERVER_STATE(Table, Partition), State}).

-spec get_state(wa_raft:table(), wa_raft:partition()) -> wa_raft_server:state() | undefined.
get_state(Table, Partition) ->
    try
        ets:lookup_element(?MODULE, ?RAFT_SERVER_STATE(Table, Partition), 2)
    catch error:badarg ->
        undefined
    end.

-spec delete_state(wa_raft:table(), wa_raft:partition()) -> true.
delete_state(Table, Partition) ->
    ets:delete(?MODULE, ?RAFT_SERVER_STATE(Table, Partition)).

%% Set to true if data on current node is stale. Read on this node may return out-of-dated data
-spec set_stale(boolean(), #raft_state{}) -> true.
set_stale(Stale, #raft_state{table = Table, partition = Partition}) ->
    ets:update_element(?MODULE, ?RAFT_STALE_FLAG(Table, Partition), {2, Stale})
        orelse ets:insert(?MODULE, {?RAFT_STALE_FLAG(Table, Partition), Stale}).

-spec get_stale(wa_raft:table(), wa_raft:partition()) -> boolean().
get_stale(Table, Partition) ->
    try
        ets:lookup_element(?MODULE, ?RAFT_STALE_FLAG(Table, Partition), 2)
    catch error:badarg ->
        true
    end.
