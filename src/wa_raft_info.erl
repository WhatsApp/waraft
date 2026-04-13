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
    get_current_term_and_leader/2,
    get_current_term/2,
    get_leader/2,
    get_status/2,
    get_state/2,
    get_live/2,
    get_lagging/2,
    get_stale/2,
    get_message_queue_length/1
]).

%% Internal API
-export([
    init_tables/0
]).

%% Internal API
-export([
    set_current_term_and_leader/4,
    set_status/6,
    set_message_queue_length/2,
    refresh_message_queue_length/1,
    clear/3
]).

%% Local RAFT server's current status flags (state, liveness, staleness and read readiness)
-define(STATUS_KEY(Table, Partition), {status, Table, Partition}).
%% Local RAFT server's most recently known term and leader
-define(CURRENT_TERM_AND_LEADER_KEY(Table, Partition), {term, Table, Partition}).
%% Local RAFT server's message queue length
-define(MESSAGE_QUEUE_LENGTH_KEY(Name), {message_queue_length, Name}).

%%-------------------------------------------------------------------
%% RAFT Info - Public API
%%-------------------------------------------------------------------

%% The RAFT server always sets both the known term and leader together, so that
%% the atomic read performed by this method will not return a known leader for
%% a different term.
-spec get_current_term_and_leader(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Info | undefined when
    Info :: {Term :: wa_raft_log:log_term(), Leader :: node() | undefined}.
get_current_term_and_leader(Table, Partition) ->
    case ets:lookup(?MODULE, ?CURRENT_TERM_AND_LEADER_KEY(Table, Partition)) of
        [] -> undefined;
        [{_, Term, Leader}] -> {Term, Leader}
    end.

-spec get_current_term(Table :: wa_raft:table(), Partition :: wa_raft:partition()) ->
    wa_raft_log:log_term() | undefined.
get_current_term(Table, Partition) ->
    ets:lookup_element(?MODULE, ?CURRENT_TERM_AND_LEADER_KEY(Table, Partition), 2, undefined).

-spec get_leader(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> node() | undefined.
get_leader(Table, Partition) ->
    ets:lookup_element(?MODULE, ?CURRENT_TERM_AND_LEADER_KEY(Table, Partition), 3, undefined).

-spec get_status(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Status | undefined when
    Status ::
        {
            State :: wa_raft_server:state(),
            Live :: boolean(),
            Lagging :: boolean(),
            Stale :: boolean()
        }.
get_status(Table, Partition) ->
    case ets:lookup(?MODULE, ?STATUS_KEY(Table, Partition)) of
        [] -> undefined;
        [{_, State, Live, Lagging, Stale}] -> {State, Live, Lagging, Stale}
    end.

-spec get_state(Table :: wa_raft:table(), Partition :: wa_raft:partition()) ->
    wa_raft_server:state() | undefined.
get_state(Table, Partition) ->
    ets:lookup_element(?MODULE, ?STATUS_KEY(Table, Partition), 2, undefined).

-spec get_live(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> boolean().
get_live(Table, Partition) ->
    ets:lookup_element(?MODULE, ?STATUS_KEY(Table, Partition), 3, false).

-spec get_lagging(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> boolean().
get_lagging(Table, Partition) ->
    ets:lookup_element(?MODULE, ?STATUS_KEY(Table, Partition), 4, true).

-spec get_stale(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> boolean().
get_stale(Table, Partition) ->
    ets:lookup_element(?MODULE, ?STATUS_KEY(Table, Partition), 5, true).

-spec get_message_queue_length(Name :: atom()) -> non_neg_integer() | undefined.
get_message_queue_length(Name) ->
    ets:lookup_element(?MODULE, ?MESSAGE_QUEUE_LENGTH_KEY(Name), 2, undefined).

%%-------------------------------------------------------------------
%% RAFT Info - Internal API
%%-------------------------------------------------------------------

-spec init_tables() -> ok.
init_tables() ->
    ets:new(?MODULE, [set, public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ok.

-spec set_current_term_and_leader(
    Table :: wa_raft:table(),
    Partition :: wa_raft:partition(),
    Term :: wa_raft_log:log_term(),
    Leader :: undefined | node()
) -> boolean().
set_current_term_and_leader(Table, Partition, Term, Leader) ->
    ets:update_element(
        ?MODULE,
        ?CURRENT_TERM_AND_LEADER_KEY(Table, Partition),
        [{2, Term}, {3, Leader}],
        {?CURRENT_TERM_AND_LEADER_KEY(Table, Partition), Term, Leader}
    ).

-spec set_status(
    Table :: wa_raft:table(),
    Partition :: wa_raft:partition(),
    State :: wa_raft_server:state(),
    Live :: boolean(),
    Lagging :: boolean(),
    Stale :: boolean()
) -> boolean().
set_status(Table, Partition, State, Live, Lagging, Stale) ->
    ets:update_element(
        ?MODULE,
        ?STATUS_KEY(Table, Partition),
        [{2, State}, {3, Live}, {4, Lagging}, {5, Stale}],
        {?STATUS_KEY(Table, Partition), State, Live, Lagging, Stale}
    ).

-spec set_message_queue_length(Name :: atom(), Length :: non_neg_integer()) -> boolean().
set_message_queue_length(Name, Length) ->
    ets:update_element(
        ?MODULE,
        ?MESSAGE_QUEUE_LENGTH_KEY(Name),
        {2, Length},
        {?MESSAGE_QUEUE_LENGTH_KEY(Name), Length}
    ).

-spec refresh_message_queue_length(Name :: atom()) -> boolean().
refresh_message_queue_length(Name) ->
    {message_queue_len, Length} = process_info(self(), message_queue_len),
    set_message_queue_length(Name, Length).

-spec clear(Table :: wa_raft:table(), Partition :: wa_raft:partition(), Name :: atom()) -> true.
clear(Table, Partition, Name) ->
    ets:delete(?MODULE, ?STATUS_KEY(Table, Partition)),
    ets:delete(?MODULE, ?MESSAGE_QUEUE_LENGTH_KEY(Name)),
    true.
