%% @format
%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements tracking of pending requests and queue limits.

-module(wa_raft_queue).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

%% PUBLIC API
-export([
    queues/1,
    queues/2,
    commit_queue_size/1,
    commit_queue_size/2,
    commit_queue_full/1,
    commit_queue_full/2,
    apply_queue_size/1,
    apply_queue_size/2,
    apply_queue_byte_size/1,
    apply_queue_byte_size/2,
    apply_queue_full/1,
    apply_queue_full/2
]).

%% INTERNAL API
-export([
    default_name/2,
    default_counters/0,
    default_commit_queue_name/2,
    default_read_queue_name/2,
    registered_name/2
]).

%% PENDING COMMIT QUEUE API
-export([
    commit/3,
    fulfill_commit/3,
    fulfill_incomplete_commit/3,
    fulfill_all_commits/2
]).

%% PENDING READ API
-export([
    reserve_read/1,
    submit_read/4,
    query_reads/2,
    fulfill_read/3,
    fulfill_incomplete_read/3,
    fulfill_all_reads/2
]).

%% APPLY QUEUE API
-export([
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

%% TYPES
-export_type([
    queues/0
]).

-include_lib("kernel/include/logger.hrl").
% used by ets:fun2ms
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

%%-------------------------------------------------------------------

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

-record(queues, {
    application :: atom(),
    counters :: atomics:atomics_ref(),
    commits :: atom(),
    reads :: atom()
}).
-opaque queues() :: #queues{}.

%%-------------------------------------------------------------------
%% PUBLIC API
%%-------------------------------------------------------------------

-spec queues(Options :: #raft_options{}) -> Queues :: queues().
queues(Options) ->
    #queues{
        application = Options#raft_options.application,
        counters = Options#raft_options.queue_counters,
        commits = Options#raft_options.queue_commits,
        reads = Options#raft_options.queue_reads
    }.

-spec queues(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Queues :: queues() | undefined.
queues(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> undefined;
        Options -> queues(Options)
    end.

-spec commit_queue_size(Queues :: queues()) -> non_neg_integer().
commit_queue_size(#queues{counters = Counters}) ->
    atomics:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER).

-spec commit_queue_size(wa_raft:table(), wa_raft:partition()) -> non_neg_integer().
commit_queue_size(Table, Partition) ->
    case queues(Table, Partition) of
        undefined -> 0;
        Queue -> commit_queue_size(Queue)
    end.

-spec commit_queue_full(Queues :: queues()) -> boolean().
commit_queue_full(#queues{application = App, counters = Counters}) ->
    atomics:get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_COMMITS(App).

-spec commit_queue_full(wa_raft:table(), wa_raft:partition()) -> boolean().
commit_queue_full(Table, Partition) ->
    case queues(Table, Partition) of
        undefined -> false;
        Queues -> commit_queue_full(Queues)
    end.

-spec apply_queue_size(Queues :: queues()) -> non_neg_integer().
apply_queue_size(#queues{counters = Counters}) ->
    atomics:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER).

-spec apply_queue_size(wa_raft:table(), wa_raft:partition()) -> non_neg_integer().
apply_queue_size(Table, Partition) ->
    case queues(Table, Partition) of
        undefined -> 0;
        Queues -> apply_queue_size(Queues)
    end.

-spec apply_queue_byte_size(Queues :: queues()) -> non_neg_integer().
apply_queue_byte_size(#queues{counters = Counters}) ->
    atomics:get(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER).

-spec apply_queue_byte_size(wa_raft:table(), wa_raft:partition()) -> non_neg_integer().
apply_queue_byte_size(Table, Partition) ->
    case queues(Table, Partition) of
        undefined -> 0;
        Queues -> apply_queue_byte_size(Queues)
    end.

-spec apply_queue_full(Queues :: queues()) -> boolean().
apply_queue_full(#queues{application = App, counters = Counters}) ->
    atomics:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES(App) orelse
        atomics:get(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLY_BYTES(App).

-spec apply_queue_full(wa_raft:table(), wa_raft:partition()) -> boolean().
apply_queue_full(Table, Partition) ->
    case queues(Table, Partition) of
        undefined -> false;
        Queues -> apply_queue_full(Queues)
    end.

%%-------------------------------------------------------------------
%% INTERNAL API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT queue server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    binary_to_atom(<<"raft_queue_", (atom_to_binary(Table))/bytes, "_", (integer_to_binary(Partition))/bytes>>).

%% Create a properly-sized atomics array for use by a RAFT queue
-spec default_counters() -> Counters :: atomics:atomics_ref().
default_counters() ->
    atomics:new(?RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS, []).

%% Get the default name for the RAFT commit queue ETS table associated with the
%% provided RAFT partition.
-spec default_commit_queue_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_commit_queue_name(Table, Partition) ->
    binary_to_atom(<<"raft_commit_queue_", (atom_to_binary(Table))/bytes, "_", (integer_to_binary(Partition))/bytes>>).

%% Get the default name for the RAFT read queue ETS table associated with the
%% provided RAFT partition.
-spec default_read_queue_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_read_queue_name(Table, Partition) ->
    binary_to_atom(<<"raft_read_queue_", (atom_to_binary(Table))/bytes, "_", (integer_to_binary(Partition))/bytes>>).

%% Get the registered name for the RAFT queue server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options -> Options#raft_options.queue_name
    end.

%%-------------------------------------------------------------------
%% PENDING COMMIT QUEUE API
%%-------------------------------------------------------------------

-spec commit(Queues :: queues(), Key :: wa_raft_acceptor:key(), From :: gen_server:from()) ->
    ok | apply_queue_full | commit_queue_full | duplicate.
commit(#queues{counters = Counters, commits = Commits} = Queues, Reference, From) ->
    case commit_queue_full(Queues) of
        true ->
            commit_queue_full;
        false ->
            case apply_queue_full(Queues) of
                true ->
                    apply_queue_full;
                false ->
                    case ets:insert_new(Commits, {Reference, From}) of
                        true ->
                            PendingCommits = atomics:add_get(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
                            ?RAFT_GATHER('raft.acceptor.commit.request.pending', PendingCommits),
                            ok;
                        false ->
                            duplicate
                    end
            end
    end.

% Fulfill a pending commit with the result of the application of the command contained
% within the commit.
-spec fulfill_commit(Queues :: queues(), term(), dynamic()) -> ok | not_found.
fulfill_commit(#queues{counters = Counters, commits = Commits}, Reference, Reply) ->
    case ets:take(Commits, Reference) of
        [{Reference, From}] ->
            atomics:sub(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
            gen_server:reply(From, Reply);
        [] ->
            not_found
    end.

% Fulfill a pending commit with an error that indicates that the commit was not completed.
-spec fulfill_incomplete_commit(Queues :: queues(), term(), wa_raft_acceptor:commit_error()) -> ok | not_found.
fulfill_incomplete_commit(Queues, Reference, Error) ->
    fulfill_commit(Queues, Reference, Error).

% Fulfill a pending commit with an error that indicates that the commit was not completed.
-spec fulfill_all_commits(Queues :: queues(), wa_raft_acceptor:commit_error()) -> ok.
fulfill_all_commits(#queues{counters = Counters, commits = Commits}, Reply) ->
    lists:foreach(
        fun({Reference, _}) ->
            case ets:take(Commits, Reference) of
                [{Reference, From}] ->
                    atomics:sub(Counters, ?RAFT_COMMIT_QUEUE_SIZE_COUNTER, 1),
                    gen_server:reply(From, Reply);
                [] ->
                    ok
            end
        end,
        ets:tab2list(Commits)
    ).

%%-------------------------------------------------------------------
%% PENDING READ QUEUE API
%%-------------------------------------------------------------------

% Inspects the read and apply queues to check if a strong read is allowed
% to be submitted to the RAFT server currently. If so, then returns 'ok'
% and increments the read counter. Inspecting the queues and actually
% adding the read request to the table are done in two stages for reads
% because the acceptor does not have enough information to add the read
% to the ETS table directly.
-spec reserve_read(Queues :: queues()) -> ok | read_queue_full | apply_queue_full.
reserve_read(#queues{application = App, counters = Counters}) ->
    PendingReads = atomics:get(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER),
    case PendingReads >= ?RAFT_MAX_PENDING_READS(App) of
        true ->
            read_queue_full;
        false ->
            case atomics:get(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER) >= ?RAFT_MAX_PENDING_APPLIES(App) of
                true ->
                    apply_queue_full;
                false ->
                    ?RAFT_GATHER('raft.acceptor.strong_read.request.pending', PendingReads + 1),
                    atomics:add(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
                    ok
            end
    end.

% Called from the RAFT server once it knows the proper ReadIndex for the
% read request to add the read request to the reads table for storage
% to handle upon applying.
-spec submit_read(Queues :: queues(), wa_raft_log:log_index(), term(), term()) -> ok.
submit_read(#queues{reads = Reads}, ReadIndex, From, Command) ->
    ets:insert(Reads, {{ReadIndex, make_ref()}, From, Command}),
    ok.

-spec query_reads(Queues :: queues(), wa_raft_log:log_index() | infinity) ->
    [{{wa_raft_log:log_index(), reference()}, term()}].
query_reads(#queues{reads = Reads}, MaxLogIndex) ->
    MatchSpec = ets:fun2ms(
        fun({{LogIndex, Reference}, _, Command}) when LogIndex =< MaxLogIndex ->
            {{LogIndex, Reference}, Command}
        end
    ),
    ets:select(Reads, MatchSpec).

-spec fulfill_read(Queues :: queues(), term(), dynamic()) -> ok | not_found.
fulfill_read(#queues{counters = Counters, reads = Reads}, Reference, Reply) ->
    case ets:take(Reads, Reference) of
        [{Reference, From, _}] ->
            atomics:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
            gen_server:reply(From, Reply);
        [] ->
            not_found
    end.

% Complete a read that was reserved by the RAFT acceptor but was rejected
% before it could be added to the read queue and so has no reference.
-spec fulfill_incomplete_read(Queues :: queues(), gen_server:from(), wa_raft_acceptor:read_error()) -> ok.
fulfill_incomplete_read(#queues{counters = Counters}, From, Reply) ->
    atomics:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
    gen_server:reply(From, Reply).

% Fulfill a pending reads with an error that indicates that the read was not completed.
-spec fulfill_all_reads(Queues :: queues(), wa_raft_acceptor:read_error()) -> ok.
fulfill_all_reads(#queues{counters = Counters, reads = Reads}, Reply) ->
    lists:foreach(
        fun({Reference, _, _}) ->
            case ets:take(Reads, Reference) of
                [{Reference, From, _}] ->
                    atomics:sub(Counters, ?RAFT_READ_QUEUE_SIZE_COUNTER, 1),
                    gen_server:reply(From, Reply);
                [] ->
                    ok
            end
        end,
        ets:tab2list(Reads)
    ).

%%-------------------------------------------------------------------
%% APPLY QUEUE API
%%-------------------------------------------------------------------

-spec reserve_apply(Queues :: queues(), non_neg_integer()) -> ok.
reserve_apply(#queues{counters = Counters}, Size) ->
    atomics:add(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER, 1),
    atomics:add(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER, Size).

-spec fulfill_apply(Queues :: queues(), non_neg_integer()) -> ok.
fulfill_apply(#queues{counters = Counters}, Size) ->
    atomics:sub(Counters, ?RAFT_APPLY_QUEUE_SIZE_COUNTER, 1),
    atomics:sub(Counters, ?RAFT_APPLY_QUEUE_BYTE_SIZE_COUNTER, Size).

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
init(
    #raft_options{
        table = Table,
        partition = Partition,
        queue_name = Name,
        queue_counters = Counters,
        queue_commits = CommitsName,
        queue_reads = ReadsName
    }
) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE(
        "Queue[~p] starting for partition ~0p/~0p with read queue ~0p and commit queue ~0p",
        [Name, Table, Partition, ReadsName, CommitsName],
        #{domain => [whatsapp, wa_raft]}
    ),

    % The queue process is the first process in the supervision for a single
    % RAFT partition. The supervisor is configured to restart all processes if
    % even a single process fails. Since the queue process is starting up, all
    % queues tracked should be empty so reset all counters.
    [atomics:put(Counters, Index, 0) || Index <- lists:seq(1, ?RAFT_NUMBER_OF_QUEUE_SIZE_COUNTERS)],

    % Create ETS tables for pending commits and reads.
    CommitsName = ets:new(CommitsName, [set | ?RAFT_QUEUE_TABLE_OPTIONS]),
    ReadsName = ets:new(ReadsName, [ordered_set | ?RAFT_QUEUE_TABLE_OPTIONS]),

    {ok, #state{name = Name}}.

-spec handle_call(Request :: term(), From :: gen_server:from(), State :: #state{}) -> {noreply, #state{}}.
handle_call(Request, From, #state{name = Name} = State) ->
    ?LOG_NOTICE(
        "Queue[~p] got unexpected request ~0P from ~0p",
        [Name, Request, 100, From],
        #{domain => [whatsapp, wa_raft]}
    ),
    {noreply, State}.

-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, #state{}}.
handle_cast(Request, #state{name = Name} = State) ->
    ?LOG_NOTICE(
        "Queue[~p] got unexpected call ~0P",
        [Name, Request, 100],
        #{domain => [whatsapp, wa_raft]}
    ),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> term().
terminate(Reason, #state{name = Name}) ->
    ?LOG_NOTICE(
        "Queue[~p] terminating due to ~0P",
        [Name, Reason, 100],
        #{domain => [whatsapp, wa_raft]}
    ).
