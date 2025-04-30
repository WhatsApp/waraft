%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements tracking of pending requests and queue limits.

-module(wa_raft_queue).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

%% INTERNAL API
-export([
    default_name/2,
    default_counters/0,
    default_commit_queue_name/2,
    default_read_queue_name/2,
    registered_name/2,
    registered_info/2
]).

%% PENDING COMMIT QUEUE API
-export([
    commit/4,
    commit_queue_size/2,
    commit_queue_full/2,
    fulfill_commit/4,
    fulfill_incomplete_commit/4,
    fulfill_all_commits/3
]).

%% PENDING READ API
-export([
    reserve_read/2,
    submit_read/5,
    query_reads/3,
    fulfill_read/4,
    fulfill_incomplete_read/4,
    fulfill_all_reads/3
]).

%% APPLY QUEUE API
-export([
    apply_queue_size/2,
    apply_queue_byte_size/2,
    apply_queue_full/2,
    reserve_apply/3,
    fulfill_apply/3
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

%% Total number of counters for RAFT partition specfic counters
-define(RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS, 4).
%% Index into counter reference for counter tracking apply queue size
-define(RAFT_APPLY_QUEUE_SIZE_COUNTER, 1).
%% Index into counter reference for counter tracking apply total byte size
-define(RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER, 2).
%% Index into counter reference for counter tracking commit queue size
-define(RAFT_COMMIT_QUEUE_SIZE_COUNTER, 3).
%% Index into counter reference for counter tracking read queue size
-define(RAFT_READ_QUEUE_SIZE_COUNTER, 4).

%%-------------------------------------------------------------------
%% INTERNAL TYPES
%%-------------------------------------------------------------------

-record(state, {
    name :: atom()
}).

%%-------------------------------------------------------------------
%% INTERNAL API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT queue server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_queue_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Create a properly-sized counters object for use by a RAFT queue.
-spec default_counters() -> Counters :: counters:counters_ref().
default_counters() ->
    counters:new(?RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS, [atomics]).

%% Get the default name for the RAFT commit queue ETS table associated with the
%% provided RAFT partition.
-spec default_commit_queue_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_commit_queue_name(Table, Partition) ->
    list_to_atom("raft_commit_queue_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the default name for the RAFT read queue ETS table associated with the
%% provided RAFT partition.
-spec default_read_queue_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_read_queue_name(Table, Partition) ->
    list_to_atom("raft_read_queue_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT queue server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.queue_name
    end.

%% Get the registered counter and queue names for the RAFT queue associated
%% with the provided RAFT partition or 'undefined' if no registration exists.
-spec registered_info(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> {App :: atom(), Counters :: counters:counters_ref(), CommitQueue :: atom(), ReadQueue :: atom()} | undefined.
registered_info(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> undefined;
        Options   -> {Options#raft_options.application, Options#raft_options.queue_counters, Options#raft_options.queue_commits, Options#raft_options.queue_reads}
    end.

%%-------------------------------------------------------------------
%% PENDING COMMIT QUEUE API
%%-------------------------------------------------------------------

-spec commit(wa_raft:table(), wa_raft:partition(), term(), gen_server:from()) -> ok | apply_queue_full | commit_queue_full | duplicate.
commit(Table, Partition, Reference, From) ->
    {App, Counters, CommitQueue, _} = require_info(Table, Partition),
    PendingCommits = counters:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER),
    case PendingCommits >= ?RAFT_MAX_PENDING_COMMITS(App) of
        true -> commit_queue_full;
        false ->
            case counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES(App) of
                true -> apply_queue_full;
                false ->
                    case ets:insert_new(CommitQueue, {Reference, From}) of
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
    case registered_info(Table, Partition) of
        undefined           -> 0;
        {_, Counters, _, _} -> counters:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER)
    end.

-spec commit_queue_full(wa_raft:table(), wa_raft:partition()) -> boolean().
commit_queue_full(Table, Partition) ->
    case registered_info(Table, Partition) of
        undefined             -> false;
        {App, Counters, _, _} -> counters:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_COMMITS(App)
    end.

% Fulfill a pending commit with the result of the application of the command contained
% within the commit.
-spec fulfill_commit(wa_raft:table(), wa_raft:partition(), term(), dynamic()) -> ok | not_found.
fulfill_commit(Table, Partition, Reference, Reply) ->
    {_, Counters, CommitQueue, _} = require_info(Table, Partition),
    case ets:take(CommitQueue, Reference) of
        [{Reference, From}] ->
            counters:sub(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
            gen_server:reply(From, Reply);
        [] ->
            not_found
    end.

% Fulfill a pending commit with an error that indicates that the commit was not completed.
-spec fulfill_incomplete_commit(wa_raft:table(), wa_raft:partition(), term(), wa_raft_acceptor:commit_error()) -> ok | not_found.
fulfill_incomplete_commit(Table, Partition, Reference, Error) ->
    fulfill_commit(Table, Partition, Reference, Error).

% Fulfill a pending commit with an error that indicates that the commit was not completed.
-spec fulfill_all_commits(wa_raft:table(), wa_raft:partition(), wa_raft_acceptor:commit_error()) -> ok.
fulfill_all_commits(Table, Partition, Reply) ->
    {_, Counters, CommitQueue, _} = require_info(Table, Partition),
    CommitsTable = CommitQueue,
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
    {App, Counters, _, _} = require_info(Table, Partition),
    PendingReads = counters:get(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER),
    case PendingReads >= ?RAFT_MAX_PENDING_READS(App) of
        true -> read_queue_full;
        false ->
            case counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES(App) of
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
    {_, _, _, ReadQueue} = require_info(Table, Partition),
    ets:insert(ReadQueue, {{ReadIndex, make_ref()}, From, Command}),
    ok.

-spec query_reads(wa_raft:table(), wa_raft:partition(), wa_raft_log:log_index() | infinity) -> [{{wa_raft_log:log_index(), reference()}, term()}].
query_reads(Table, Partition, MaxLogIndex) ->
    {_, _, _, ReadQueue} = require_info(Table, Partition),
    MatchSpec = ets:fun2ms(
        fun({{LogIndex, Reference}, _, Command}) when LogIndex =< MaxLogIndex ->
            {{LogIndex, Reference}, Command}
        end
    ),
    ets:select(ReadQueue, MatchSpec).

-spec fulfill_read(wa_raft:table(), wa_raft:partition(), term(), dynamic()) -> ok | not_found.
fulfill_read(Table, Partition, Reference, Reply) ->
    {_, Counters, _, ReadQueue} = require_info(Table, Partition),
    case ets:take(ReadQueue, Reference) of
        [{Reference, From, _}] ->
            counters:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
            gen_server:reply(From, Reply);
        [] ->
            not_found
    end.

% Complete a read that was reserved by the RAFT acceptor but was rejected
% before it could be added to the read queue and so has no reference.
-spec fulfill_incomplete_read(wa_raft:table(), wa_raft:partition(), gen_server:from(), wa_raft_acceptor:read_error()) -> ok.
fulfill_incomplete_read(Table, Partition, From, Reply) ->
    {_, Counters, _, _} = require_info(Table, Partition),
    counters:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
    gen_server:reply(From, Reply).

% Fulfill a pending reads with an error that indicates that the read was not completed.
-spec fulfill_all_reads(wa_raft:table(), wa_raft:partition(), wa_raft_acceptor:read_error()) -> ok.
fulfill_all_reads(Table, Partition, Reply) ->
    {_, Counters, _, ReadQueue} = require_info(Table, Partition),
    ReadsTable = ReadQueue,
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
    case registered_info(Table, Partition) of
        undefined           -> 0;
        {_, Counters, _, _} -> counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER)
    end.

-spec apply_queue_byte_size(wa_raft:table(), wa_raft:partition()) -> non_neg_integer().
apply_queue_byte_size(Table, Partition) ->
    case registered_info(Table, Partition) of
        undefined           -> 0;
        {_, Counters, _, _} -> counters:get(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER)
    end.

-spec apply_queue_full(wa_raft:table(), wa_raft:partition()) -> boolean().
apply_queue_full(Table, Partition) ->
    case registered_info(Table, Partition) of
        undefined ->
            false;
        {App, Counters, _, _} ->
            counters:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES(App) orelse
                counters:get(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLY_BYTES(App)
    end.

-spec reserve_apply(wa_raft:table(), wa_raft:partition(), non_neg_integer()) -> ok.
reserve_apply(Table, Partition, Size) ->
    {_, Counters, _, _} = require_info(Table, Partition),
    counters:add(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER, 1),
    counters:add(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER, Size).

-spec fulfill_apply(wa_raft:table(), wa_raft:partition(), non_neg_integer()) -> ok.
fulfill_apply(Table, Partition, Size) ->
    {_, Counters, _, _} = require_info(Table, Partition),
    counters:sub(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER, 1),
    counters:sub(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER, Size).

%%-------------------------------------------------------------------
%% OTP SUPERVISION
%%-------------------------------------------------------------------

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 1000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) -> {ok, Pid :: pid()} | ignore | wa_raft:error().
start_link(#raft_options{queue_name = Name} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%%-------------------------------------------------------------------
%% QUEUE SERVER CALLBACKS
%%-------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> {ok, #state{}}.
init(#raft_options{table = Table, partition = Partition, queue_name = Name, queue_counters = Counters, queue_commits = CommitQueueName, queue_reads = ReadQueueName}) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Queue[~p] starting for partition ~0p/~0p with read queue ~0p and commit queue ~0p",
        [Name, Table, Partition, ReadQueueName, CommitQueueName], #{domain => [whatsapp, wa_raft]}),

    % The queue process is the first process in the supervision for a single
    % RAFT partition. The supervisor is configured to restart all processes if
    % even a single process fails. Since the queue process is starting up, all
    % queues tracked should be empty so reset all counters.
    lists:foreach(
        fun (I) -> counters:put(Counters, I, 0) end,
        lists:seq(1, ?RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS)),

    % Create ETS tables for pending commits and reads.
    CommitQueueName = ets:new(CommitQueueName, [set | ?RAFT_QUEUE_TABLE_OPTIONS]),
    ReadQueueName = ets:new(ReadQueueName, [ordered_set | ?RAFT_QUEUE_TABLE_OPTIONS]),

    {ok, #state{name = Name}}.

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

-spec require_info(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> {Application :: atom(), Counters :: counters:counters_ref(), CommitQueue :: atom(), ReadQueue :: atom()}.
require_info(Table, Partition) ->
    case registered_info(Table, Partition) of
        undefined -> error(partition_not_registered);
        Info      -> Info
    end.
