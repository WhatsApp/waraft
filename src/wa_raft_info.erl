%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% API for accessing certain useful information about the state of local
%%% RAFT servers without requiring a status request against the RAFT server
%%% itself.

-module(wa_raft_info).
-compile(warn_missing_spec_all).

%% Public API
-export([
    get_current_term/2,
    get_leader/2,
    get_membership/2,
    get_stale/2,
    get_state/2
]).

%% Internal API
-export([
    init_tables/0,
    delete_state/2,
    set_current_term/3,
    set_leader/3,
    set_membership/3,
    set_stale/3,
    set_state/3
]).

%% Local RAFT server's current FSM state
-define(RAFT_SERVER_STATE_KEY(Table, Partition), {state, Table, Partition}).
%% Local RAFT server's most recently known term
-define(RAFT_CURRENT_TERM_KEY(Table, Partition), {term, Table, Partition}).
%% Local RAFT server's most recently known leader node
-define(RAFT_CURRENT_LEADER_KEY(Table, Partition), {leader, Table, Partition}).
%% Local RAFT server's current stale flag - indicates if the server thinks its data is stale
-define(RAFT_STALE_KEY(Table, Partition), {stale, Table, Partition}).
%% Local RAFT server's most recently known membership
-define(RAFT_MEMBERSHIP_KEY(Table, Partition), {membership, Table, Partition}).

%%-------------------------------------------------------------------
%% RAFT Info - Public API
%%-------------------------------------------------------------------

-spec get(term(), Default) -> Default.
get(Key, Default) ->
    try
        ets:lookup_element(?MODULE, Key, 2)
    catch error:badarg ->
        Default
    end.

-spec get_leader(wa_raft:table(), wa_raft:partition()) -> node() | undefined.
get_leader(Table, Partition) ->
    get(?RAFT_CURRENT_LEADER_KEY(Table, Partition), undefined).

-spec get_current_term(wa_raft:table(), wa_raft:partition()) -> wa_raft_log:log_term() | undefined.
get_current_term(Table, Partition) ->
    get(?RAFT_CURRENT_TERM_KEY(Table, Partition), undefined).

-spec get_state(wa_raft:table(), wa_raft:partition()) -> wa_raft_server:state() | undefined.
get_state(Table, Partition) ->
    get(?RAFT_SERVER_STATE_KEY(Table, Partition), undefined).

-spec get_stale(wa_raft:table(), wa_raft:partition()) -> boolean().
get_stale(Table, Partition) ->
    get(?RAFT_STALE_KEY(Table, Partition), true).

-spec get_membership(wa_raft:table(), wa_raft:partition()) -> wa_raft_server:membership() | undefined.
get_membership(Table, Partition) ->
    get(?RAFT_MEMBERSHIP_KEY(Table, Partition), undefined).

%%-------------------------------------------------------------------
%% RAFT Info - Internal API
%%-------------------------------------------------------------------

-spec init_tables() -> ok.
init_tables() ->
    ets:new(?MODULE, [set, public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

-spec set(term(), term()) -> true.
set(Key, Value) ->
    ets:update_element(?MODULE, Key, {2, Value}) orelse ets:insert(?MODULE, {Key, Value}).

-spec delete(term()) -> true.
delete(Key) ->
    ets:delete(?MODULE, Key).

-spec set_leader(wa_raft:table(), wa_raft:partition(), node()) -> true.
set_leader(Table, Partition, Value) ->
    set(?RAFT_CURRENT_LEADER_KEY(Table, Partition), Value).

-spec set_current_term(wa_raft:table(), wa_raft:partition(), wa_raft_log:log_term()) -> true.
set_current_term(Table, Partition, Term) ->
    set(?RAFT_CURRENT_TERM_KEY(Table, Partition), Term).

-spec set_state(wa_raft:table(), wa_raft:partition(), wa_raft_server:state()) -> true.
set_state(Table, Partition, State) ->
    set(?RAFT_SERVER_STATE_KEY(Table, Partition), State).

-spec delete_state(wa_raft:table(), wa_raft:partition()) -> true.
delete_state(Table, Partition) ->
    delete(?RAFT_SERVER_STATE_KEY(Table, Partition)).

-spec set_stale(wa_raft:table(), wa_raft:partition(), boolean()) -> true.
set_stale(Table, Partition, Stale) ->
    set(?RAFT_STALE_KEY(Table, Partition), Stale).

-spec set_membership(wa_raft:table(), wa_raft:partition(), wa_raft_server:membership()) -> true.
set_membership(Table, Partition, Membership) ->
    set(?RAFT_MEMBERSHIP_KEY(Table, Partition), Membership).
