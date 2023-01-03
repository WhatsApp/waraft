%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements tracking of pending requests and queue limits.

-module(wa_raft_queue).
-compile(warn_missing_spec).
-behaviour(gen_server).

%% INTERNAL API
-export([
    name/2
]).

%% PENDING COMMIT QUEUE API
-export([
    commit/4,
    commit_queue_size/2,
    commit_queue_full/2,
    fulfill_commit/4,
    fulfill_all_commits/3
]).

%% PENDING READ API
-export([
    reserve_read/2,
    submit_read/5,
    query_reads/3,
    fulfill_read/4,
    fulfill_read_early/4,
    fulfill_all_reads/3
]).

%% APPLY QUEUE API
-export([
    apply_queue_size/2,
    apply_queue_full/2,
    reserve_apply/2,
    fulfill_apply/2
]).

%% OTP SUPERVISION
-export([
    child_spec/1,
    start_link/1
]).

%% QUEUE SERVER CALLBACKS
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl"). % used by ets:fun2ms
-include("wa_raft.hrl").

%% ETS table creation options shared by all queue tables
-define(RAFT_QUEUE_TABLE_OPTIONS, [named_table, public, {read_concurrency, true}, {write_concurrency, true}]).

%% Name and ETS table options for the commit queue
-define(RAFT_COMMIT_QUEUE_TABLE(Table, Partition), ?TO_ATOM("raft_commit_queue_", Table, Partition)).
-define(RAFT_COMMIT_QUEUE_TABLE_OPTIONS, [set | ?RAFT_QUEUE_TABLE_OPTIONS]).
%% Name and ETS table optiosn for the read queue
-define(RAFT_READ_QUEUE_TABLE(Table, Partition), ?TO_ATOM("raft_read_queue_", Table, Partition)).
-define(RAFT_READ_QUEUE_TABLE_OPTIONS, [ordered_set | ?RAFT_QUEUE_TABLE_OPTIONS]).

%% Total number of counters for RAFT partition specfic counters
-define(RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS, 3).
%% Index into counter reference for counter tracking apply queue size
-define(RAFT_APPLY_QUEUE_SIZE_COUNTER, 1).
%% Index into counter reference for counter tracking commit queue size
-define(RAFT_COMMIT_QUEUE_SIZE_COUNTER, 2).
%% Index into counter reference for counter tracking read queue size
-define(RAFT_READ_QUEUE_SIZE_COUNTER, 3).

%% Maximum number of pending applies for any single RAFT partition
-define(RAFT_MAX_PENDING_APPLIES(), ?RAFT_CONFIG(raft_apply_queue_max_size, 1000)).
%% Maximum number of pending commits for any single RAFT partition
-define(RAFT_MAX_PENDING_COMMITS(), ?RAFT_CONFIG(raft_max_pending_commits, 1500)).
%% Maximum number of pending reads for any single RAFT partition
-define(RAFT_MAX_PENDING_READS(), ?RAFT_CONFIG(raft_max_pending_reads, 5000)).

%%-------------------------------------------------------------------
%% INTERNAL TYPES
%%-------------------------------------------------------------------

-record(state, {
    name :: atom()
}).

%%-------------------------------------------------------------------
%% INTERNAL API
%%-------------------------------------------------------------------

-spec name(wa_raft:table(), wa_raft:partition()) -> atom().
name(Table, Partition) ->
    ?TO_ATOM("raft_queue_", Table, Partition).

%%-------------------------------------------------------------------
%% PENDING COMMIT QUEUE API
%%-------------------------------------------------------------------

-spec commit(wa_raft:table(), wa_raft:partition(), term(), gen_server:from()) -> ok | apply_queue_full | commit_queue_full | duplicate.
commit(Table, Partition, Reference, From) ->
    Counters = require_counters(Table, Partition),
    PendingCommits = counters:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER),
    case PendingCommits >= ?RAFT_MAX_PENDING_COMMITS() of
        true -> commit_queue_full;
        false ->
            case counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES() of
                true -> apply_queue_full;
                false ->
                    case ets:insert_new(?RAFT_COMMIT_QUEUE_TABLE(Table, Partition), {Reference, From}) of
                        true ->
                            ?RAFT_GATHER('raft.acceptor.commit.request.pending', PendingCommits + 1),
                            counters:add(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
                            ok;
                        false ->
                            duplicate
                    end
            end
    end.

-spec commit_queue_size(wa_raft:table(), wa_raft:partition()) -> non_neg_integer().
commit_queue_size(Table, Partition) ->
    case get_counters(Table, Partition) of
        undefined -> 0;
        Counters  -> counters:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER)
    end.

-spec commit_queue_full(wa_raft:table(), wa_raft:partition()) -> boolean().
commit_queue_full(Table, Partition) ->
    commit_queue_size(Table, Partition) >= ?RAFT_MAX_PENDING_COMMITS().

-spec fulfill_commit(wa_raft:table(), wa_raft:partition(), term(), term()) -> ok | not_found.
fulfill_commit(Table, Partition, Reference, Reply) ->
    Counters = require_counters(Table, Partition),
    case ets:take(?RAFT_COMMIT_QUEUE_TABLE(Table, Partition), Reference) of
        [{Reference, From}] ->
            counters:sub(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
            gen_server:reply(From, Reply);
        [] ->
            not_found
    end.

-spec fulfill_all_commits(wa_raft:table(), wa_raft:partition(), term()) -> ok.
fulfill_all_commits(Table, Partition, Reply) ->
    Counters = require_counters(Table, Partition),
    CommitsTable = ?RAFT_COMMIT_QUEUE_TABLE(Table, Partition),
    lists:foreach(
        fun ({Reference, _}) ->
            case ets:take(CommitsTable, Reference) of
                [{Reference, From}] ->
                    counters:sub(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
                    gen_server:reply(From, Reply);
                [] ->
                    ok
            end
        end, ets:tab2list(CommitsTable)).

%%-------------------------------------------------------------------
%% PENDING READ QUEUE API
%%-------------------------------------------------------------------

% Inspects the read and apply queues to check if a strong read is allowed
% to be submitted to the RAFT server currently. If so, then returns 'ok'
% and increments the read counter. Inspecting the queues and actually
% adding the read request to the table are done in two stages for reads
% because the acceptor does not have enough information to add the read
% to the ETS table directly.
-spec reserve_read(wa_raft:table(), wa_raft:partition()) -> ok | read_queue_full | apply_queue_full.
reserve_read(Table, Partition) ->
    Counters = require_counters(Table, Partition),
    PendingReads = counters:get(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER),
    case PendingReads >= ?RAFT_MAX_PENDING_READS() of
        true -> read_queue_full;
        false ->
            case counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES() of
                true -> apply_queue_full;
                false ->
                    ?RAFT_GATHER('raft.acceptor.strong_read.request.pending', PendingReads + 1),
                    counters:add(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
                    ok
            end
    end.

% Called from the RAFT server once it knows the proper ReadIndex for the
% read request to add the read request to the reads table for storage
% to handle upon applying.
-spec submit_read(wa_raft:table(), wa_raft:partition(), wa_raft_log:log_index(), term(), term()) -> ok | read_queue_full.
submit_read(Table, Partition, ReadIndex, From, Command) ->
    ets:insert(?RAFT_READ_QUEUE_TABLE(Table, Partition), {{ReadIndex, make_ref()}, From, Command}),
    ok.

-spec query_reads(wa_raft:table(), wa_raft:partition(), wa_raft_log:log_index() | infinity) -> [{{wa_raft_log:log_index(), reference()}, term()}].
query_reads(Table, Partition, MaxLogIndex) ->
    MatchSpec = ets:fun2ms(
        fun({{LogIndex, Reference}, _, Command}) when LogIndex =< MaxLogIndex ->
            {{LogIndex, Reference}, Command}
        end
    ),
    ets:select(?RAFT_READ_QUEUE_TABLE(Table, Partition), MatchSpec).

-spec fulfill_read(wa_raft:table(), wa_raft:partition(), term(), term()) -> ok | not_found.
fulfill_read(Table, Partition, Reference, Reply) ->
    Counters = require_counters(Table, Partition),
    case ets:take(?RAFT_READ_QUEUE_TABLE(Table, Partition), Reference) of
        [{Reference, From, _}] ->
            counters:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
            gen_server:reply(From, Reply);
        [] ->
            not_found
    end.

% Complete a read that was reserved by the RAFT acceptor but was rejected
% before it could be added to the read queue and so has no reference.
-spec fulfill_read_early(wa_raft:table(), wa_raft:partition(), gen_server:from(), term()) -> ok.
fulfill_read_early(Table, Partition, From, Reply) ->
    Counters = require_counters(Table, Partition),
    counters:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
    gen_server:reply(From, Reply).

-spec fulfill_all_reads(wa_raft:table(), wa_raft:partition(), term()) -> ok.
fulfill_all_reads(Table, Partition, Reply) ->
    Counters = require_counters(Table, Partition),
    ReadsTable = ?RAFT_READ_QUEUE_TABLE(Table, Partition),
    lists:foreach(
        fun ({Reference, _, _}) ->
            case ets:take(ReadsTable, Reference) of
                [{Reference, From, _}] ->
                    counters:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
                    gen_server:reply(From, Reply);
                [] ->
                    ok
            end
        end, ets:tab2list(ReadsTable)).

%%-------------------------------------------------------------------
%% APPLY QUEUE API
%%-------------------------------------------------------------------

-spec apply_queue_size(wa_raft:table(), wa_raft:partition()) -> non_neg_integer().
apply_queue_size(Table, Partition) ->
    case get_counters(Table, Partition) of
        undefined -> 0;
        Counters  -> counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER)
    end.

-spec apply_queue_full(wa_raft:table(), wa_raft:partition()) -> boolean().
apply_queue_full(Table, Partition) ->
    apply_queue_size(Table, Partition) >= ?RAFT_MAX_PENDING_APPLIES().

-spec reserve_apply(wa_raft:table(), wa_raft:partition()) -> ok.
reserve_apply(Table, Partition) ->
    Counters = require_counters(Table, Partition),
    counters:add(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER, 1).

-spec fulfill_apply(wa_raft:table(), wa_raft:partition()) -> ok.
fulfill_apply(Table, Partition) ->
    Counters = require_counters(Table, Partition),
    counters:sub(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER, 1).

%%-------------------------------------------------------------------
%% OTP SUPERVISION
%%-------------------------------------------------------------------

-spec child_spec(Options :: wa_raft:options()) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 1000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: wa_raft:options()) -> {ok, Pid :: pid()} | ignore | wa_raft:error().
start_link(#{table := Table, partition := Partition} = Options) ->
    gen_server:start_link({local, name(Table, Partition)}, ?MODULE, Options, []).

%%-------------------------------------------------------------------
%% QUEUE SERVER CALLBACKS
%%-------------------------------------------------------------------

-spec init(Options :: wa_raft:options()) -> {ok, #state{}}.
init(#{table := Table, partition := Partition}) ->
    ?LOG_NOTICE("Queue[~p] starting", [name(Table, Partition)], #{domain => [whatsapp, wa_raft]}),

    process_flag(trap_exit, true),
    init_counters(Table, Partition),

    % Create ETS tables for pending commits and reads.
    ets:new(?RAFT_COMMIT_QUEUE_TABLE(Table, Partition), ?RAFT_COMMIT_QUEUE_TABLE_OPTIONS),
    ets:new(?RAFT_READ_QUEUE_TABLE(Table, Partition), ?RAFT_READ_QUEUE_TABLE_OPTIONS),

    {ok, #state{name = name(Table, Partition)}}.

-spec handle_call(Request :: term(), From :: gen_server:from(), State :: #state{}) -> {noreply, #state{}}.
handle_call(Request, From, #state{name = Name} = State) ->
    ?LOG_NOTICE("Queue[~p] got unexpected request ~0P from ~0p",
        [Name, Request, 100, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, #state{}}.
handle_cast(Request, #state{name = Name} = State) ->
    ?LOG_NOTICE("Queue[~p] got unexpected call ~0P",
        [Name, Request, 100], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> term().
terminate(Reason, #state{name = Name}) ->
    ?LOG_NOTICE("Queue[~p] terminating due to ~0P",
        [Name, Reason, 100], #{domain => [whatsapp, wa_raft]}).

%%-------------------------------------------------------------------
%% PRIVATE METHODS
%%-------------------------------------------------------------------

%% Get the counters reference for a particular partition or create and save one
%% if one does not already exist. Reset all counter fields to zero.
-spec init_counters(wa_raft:table(), wa_raft:partition()) -> counters:counters_ref().
init_counters(Table, Partition) ->
    % For the purposes of caching, we want there to be only one counters
    % reference associated with any RAFT partition over the duration of the
    % application so we store them into persistent_term to preserve them. We
    % only expect each partition to create and add a single counter into the
    % term so there will be minimal GC impact and that only one queue process
    % will be active per partition, preventing races on this entry.
    Key = {?MODULE, Table, Partition},
    Counters = try persistent_term:get(Key) catch
        error:badarg ->
            NewCounters = counters:new(?RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS, [atomics]),
            persistent_term:put(Key, NewCounters),
            NewCounters
    end,

    % The queue process is the first process in the supervision for a single
    % RAFT partition. The supervisor is configured to restart all processes if
    % even a single process fails. Since the queue process is starting up, all
    % queues tracked should be empty so reset all counters.
    lists:foreach(
        fun (I) -> counters:put(Counters, I, 0) end,
        lists:seq(1, ?RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS)),

    Counters.

% Save the counters reference in the process dictionary if possible.
% The counters reference for any particular partition is not intended to change
% through the lifetime of the application at least.
-spec get_counters(wa_raft:table(), wa_raft:partition()) -> counters:counters_ref() | undefined.
get_counters(Table, Partition) ->
    Key = {?MODULE, Table, Partition},
    case get(Key) of
        undefined ->
            put(Key, persistent_term:get(Key, undefined)),
            get(Key);
        Counters ->
            Counters
    end.

-spec require_counters(wa_raft:table(), wa_raft:partition()) -> counters:counters_ref().
require_counters(Table, Partition) ->
    case get_counters(Table, Partition) of
        undefined -> error(badarg);
        Counters  -> Counters
    end.
