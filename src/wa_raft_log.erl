%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module is the interface for raft log. It defines the callbacks
%%% required by the specific log implementations.

-module(wa_raft_log).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

%% OTP supervision
-export([
    child_spec/1,
    start_link/1
]).

%% APIs for writing new log data
-export([
    append/2,
    append/3
]).

%% APIs for accessing log data
-export([
    first_index/1,
    last_index/1,

    fold/5,
    fold/6,

    fold_terms/5,

    term/2,
    get/2,
    get/3,
    get/4,
    get_terms/3,

    config/1
]).

%% APIs for batching new entries and committing
-export([
    submit/2,
    pending/1,
    sync/1,
    cancel/1
]).

%% APIs for managing logs and log data
-export([
    open/2,
    reset/2,
    truncate/2,
    trim/2,
    rotate/2, rotate/4,
    flush/1
]).

%% Internal API
-export([
    default_name/2,
    registered_name/2
]).

%% Internal API
-export([
    log/1,
    log_name/1,
    provider/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-export_type([
    log/0,
    log_name/0,
    log_pos/0,
    log_op/0,
    log_index/0,
    log_term/0,
    log_entry/0,
    log_record/0,
    view/0,
    error/0
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% Atom indicating that the provider has not been opened yet.
-define(PROVIDER_NOT_OPENED, '$not_opened').

%% A view of a RAFT log that is backed by a particular
%% log provider. This view keeps track of its own logical
%% start and end indices as well as a batch of pending
%% log entries so that the RAFT server is always able to
%% access a consistent view of the RAFT log given simple
%% RAFT log provider implementations.
-record(log_view, {
    log :: log(),
    first = 0 :: log_index(),
    last = 0 :: log_index(),
    pending = [] :: [log_entry()],
    config :: undefined | {log_index(), wa_raft_server:config()}
}).

%% The state stored by the RAFT log server which is
%% responsible for synchronizing destructive operations
%% on the RAFT log with operations that are performed
%% asynchronously to the RAFT server.
-record(log_state, {
    log :: log(),
    state = ?PROVIDER_NOT_OPENED :: term()
}).

%% Name of a raft log.
-type log() :: #raft_log{}.
-type log_name() :: atom().
-type log_index() :: non_neg_integer().
-type log_term() :: non_neg_integer().
-type log_pos() :: #raft_log_pos{}.
-type log_op() ::
    undefined
    | {wa_raft_acceptor:key(), wa_raft_acceptor:command()}
    | {wa_raft_acceptor:key(), wa_raft_label:label(), wa_raft_acceptor:command()}.
-type log_entry() :: {log_term(), log_op()}.
-type log_record() :: {log_index(), log_entry()}.

%% A view of a RAFT log.
-opaque view() :: #log_view{}.

%% The recoverable error types that can be returned by log providers.
%% If there is an error condition that causes a read or write operation
%% to the log to be unserviceable, then log providers should raise an
%% error or exit.
-type error() :: {error, error_reason()}.
-type error_reason() :: corruption | invalid_start_index | invalid_end_index.

%%-------------------------------------------------------------------
%% RAFT log provider interface for accessing log data
%%-------------------------------------------------------------------

%% Gets the first index of the RAFT log. If there are no log entries,
%% then return 'undefined'.
-callback first_index(Log :: log()) -> undefined | log_index() | error().

%% Gets the last index of the RAFT log. If there are no log entries,
%% then return 'undefined'.
-callback last_index(Log :: log()) -> undefined | log_index() | error().

%% Fold over a range (inclusive) of log entries from the RAFT log by
%% calling the provided accumulator function on successive log entries.
%% The size of each entry is calculated as the external term size of
%% {Term, Op} in bytes. The fold will stop once the provided size limit
%% is reached (if one is given, otherwise will complete the range of entries).
%% There is no expectation that the log entries folded over are complete,
%% only that the accumulator is called on log entries with indices that
%% are strictly increasing however implementations should try to call the
%% accumulator on all available log entries within the range. Callers of
%% this function are responsible for performing any necessary validation
%% of log indices.
-callback fold(Log :: log(),
               Start :: log_index(),
               End :: log_index(),
               SizeLimit :: non_neg_integer() | infinity,
               Func :: fun((Index :: log_index(), Size :: non_neg_integer(), Entry :: log_entry(), Acc) -> Acc),
               Acc) ->
    {ok, Acc} | wa_raft:error().

%% Fold over a range (inclusive) of log terms from the RAFT log by
%% calling the provided accumulator function on successive log terms.
%% There is no expectation that the log terms folded over are complete,
%% only that the accumulator is called on log terms with indices that
%% are strictly increasing however implementations should try to call the
%% accumulator on all available log terms within the range. Callers of
%% this function are responsible for performing any necessary validation
%% of log indices.
-callback fold_terms(Log :: log(),
                     Start :: log_index(),
                     End :: log_index(),
                     Func :: fun((Index :: log_index(), Entry :: log_entry(), Acc) -> Acc),
                     Acc) ->
    {ok, Acc} | wa_raft:error().

%% Get a single log entry at the specified index. This API is specified
%% separately because some implementations may have more efficient ways to
%% get log entries when only one log entry is required. If the log entry
%% does not exist, then return 'not_found'.
-callback get(Log :: log(), Index :: log_index()) -> {ok, Entry :: log_entry()} | not_found | error().

%% Get only the term of a specific log entry. This API is specified
%% seperately because some implementations may have more efficient ways to
%% get just the term of a particular log entry. If the log entry does not
%% exist, then return 'not_found'.
-callback term(Log :: log(), Index :: log_index()) -> {ok, Term :: log_term()} | not_found | error().

%% Get the most recent configuration stored in the log. Log providers
%% should ensure that configuration operations are indexed so that this
%% call does not require a scan of the log.
-callback config(Log :: log()) -> {ok, Index :: log_index(), Config :: wa_raft_server:config()} | not_found | error().

%%-------------------------------------------------------------------
%% RAFT log provider interface for writing new log data
%%-------------------------------------------------------------------

%% Write log entries to the specified position in the RAFT log.
%%  - Implementations should not write any log entries before
%%    the start of the current log even if they may be included
%%    in the provided list of log entries.
%%  - When handling log entries that already exist in the RAFT log,
%%    implementations should verify that the terms in the provided
%%    log entries match the terms of the existing log entries.
%%    If a term does not match, then {mismatch, Index} should be
%%    returned with the index of the first log entry whose term does
%%    not match.
%%  - This function should never overwrite existing log entries.
%%  - If appending the provided log entries at the provided starting
%%    position would produce a gap, then return
%%    `{error, invalid_start_index}`.
%%  - Otherwise, if the provided log entries were written
%%    successfully or otherwise already existed, return `ok`.
%% In 'strict' mode, the append should always succeed or otherwise
%% return an error. In 'relaxed' mode, if there are conditions that
%% would make it impossible to quickly append to the log, it is
%% acceptable to skip this append and return 'skipped'.
-callback append(View :: view(), Start :: log_index(), Entries :: [log_entry()], Mode :: strict | relaxed) ->
    ok | {mismatch, Index :: log_index()} | skipped | wa_raft:error().

%%-------------------------------------------------------------------
%% RAFT log provider interface for managing underlying RAFT log
%%-------------------------------------------------------------------

%% Perform any first time setup operations before opening the RAFT log.
%% This function is called from the RAFT log server and is only called
%% once per incarnation of a RAFT partition.
%% If this setup fails such that the log is not usable, implementations
%% should raise an error or exit to interrupt the startup process.
-callback init(Log :: wa_raft_log:log()) -> ok.

%% Open the RAFT log and return a state term that will be provided to
%% subsequent calls made from the RAFT log server. During the opening
%% process, the log will be inspected to see if it contains a record
%% corresponding to the last applied index of the storage backing this
%% RAFT partition and whether or not the term of this entry matches
%% that reported by the storage. If so, then opening proceeeds normally.
%% If there is a mismatch, then the log will be reinitialized using
%% `reset/3`.
%% If this setup fails such that the log is not usable, implementations
%% should raise an error or exit to interrupt the opening process.
-callback open(Log :: wa_raft_log:log()) -> {ok, State :: term()} | error().

%% Close the RAFT log and release any resources used by it. This
%% is called when the RAFT log server is terminating.
-callback close(Log :: log(), State :: term()) -> term().

%% Completely clear the RAFT log and setup a new log with an initial entry
%% at the provided index with the provided term and an undefined op.
-callback reset(Log :: log(), Position :: log_pos(), State :: term()) -> {ok, NewState :: term()} | error().

%% Truncate the RAFT log to the given position so that all log entries
%% including and after the provided index are completely deleted from
%% the RAFT log.
-callback truncate(Log :: log(), Index :: log_index(), State :: term()) -> {ok, NewState :: term()} | error().

%% Optionally, trim the RAFT log up to the given index.
%%  - This means that all log entries before the given index can be
%%    deleted (both term and op information can be removed) and the
%%    log entry at the given index can have its op removed (keeping
%%    only the term information).
%%  - Implementations are not required to always trim the log to exactly
%%    the provided index but must not trim past the provided index and
%%    must always ensure that if the log were to be reloaded from disk
%%    at any time that the log always remains contiguous, meaning that
%%    only the first entry in the log can be missing op information and
%%    that the indices of all log entries in the log are contiguous.
-callback trim(Log :: log(), Index :: log_index(), State :: term()) -> {ok, NewState :: term()} | error().

%% Flush log to disk on a best-effort basis.
-callback flush(Log :: log()) -> term().

%%-------------------------------------------------------------------
%% RAFT log provider interface for writing new log data
%%-------------------------------------------------------------------

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => permanent,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(#raft_options{log_name = Name} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%%-------------------------------------------------------------------
%% APIs for writing new log data
%%-------------------------------------------------------------------

%% Append the provided log entries to the end of the log.
%% See `append/3` for more detailed information.
-spec append(View :: view(), Entries :: [log_entry()]) -> {ok, LastIndex :: log_index(), NewView :: view()} | wa_raft:error().
append(#log_view{last = Last} = View, Entries) ->
    append(View, Last + 1, Entries).

%% Append the provided log entries to the log at the specified starting position.
%% If this provided starting position is past the end of the log and appending
%% would produce a gap, then fail. If the provided start position is located
%% in the middle of the log, then the terms of the provided log entries will be
%% compared with the already existing log entries. If there is any mismatch, then
%% all log entries after the mismatching log will be replaced with the new log
%% entries provided.
-spec append(View :: view(), Start :: log_index(), Entries :: [log_entry()]) ->
    {ok, MatchIndex :: log_index(), NewView :: view()} | wa_raft:error().
append(View, Start, _Entries) when Start =< 0 ->
    ?LOG_ERROR("[~p] rejecting append starting at invalid start index ~p", [log_name(View), Start], #{domain => [whatsapp, wa_raft]}),
    {error, invalid_start_index};
append(#log_view{log = Log, last = Last} = View0, Start, Entries) ->
    ?RAFT_COUNT('raft.log.append'),
    Provider = provider(Log),
    case Provider:append(View0, Start, Entries, strict) of
        ok ->
            ?RAFT_COUNT('raft.log.append.ok'),
            NewMatch = Start + length(Entries) - 1,
            NewLast = max(Last, NewMatch),
            {ok, NewMatch, refresh_config(View0#log_view{last = NewLast})};
        {mismatch, Index} ->
            ?RAFT_COUNT('raft.log.append.mismatch'),
            case truncate(View0, Index) of
                {ok, View1} ->
                    NewEntries = lists:nthtail(Index - Start, Entries),
                    case Provider:append(View1, Index, NewEntries, strict) of
                        ok ->
                            ?RAFT_COUNT('raft.log.append.ok'),
                            NewMatch = Start + length(Entries) - 1,
                            NewLast = max(Index - 1, NewMatch),
                            {ok, NewMatch, refresh_config(View1#log_view{last = NewLast})};
                        {error, Reason} ->
                            ?RAFT_COUNT('raft.log.append.error'),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ?RAFT_COUNT('raft.log.append.mismatch.error'),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.append.error'),
            {error, Reason}
    end.

%%-------------------------------------------------------------------
%% APIs for accessing log data
%%-------------------------------------------------------------------

%% Gets the first index of the log view or as reported by the log provider.
-spec first_index(LogOrView :: log() | view()) -> FirstIndex :: log_index().
first_index(#log_view{first = First}) ->
    First;
first_index(Log) ->
    Provider = provider(Log),
    Provider:first_index(Log).

%% Gets the last index of the log view or as reported by the log provider.
-spec last_index(LogOrView :: log() | view()) -> LastIndex :: log_index().
last_index(#log_view{last = Last}) ->
    Last;
last_index(Log) ->
    Provider = provider(Log),
    Provider:last_index(Log).

-spec fold(LogOrView :: log() | view(),
           First :: log_index(),
           Last :: log_index() | infinity,
           Func :: fun((Index :: log_index(), Entry :: log_entry(), Acc) -> Acc),
           Acc) ->
    {ok, Acc} | wa_raft:error().
fold(LogOrView, First, Last, Func, Acc) ->
    fold(LogOrView, First, Last, infinity, Func, Acc).

%% Folds over the entries in the log view of raw entries from the log provider
%% between the provided first and last log indices (inclusive) up until the
%% provided accumulator function has been called on a total byte size of log
%% entries that is less than the provided byte size limit.
%% If there exists a log entry between the provided first and last indices then
%% the accumulator function will be called on at least that entry.
%% This API provides no validation of the log indices and entries passed by the
%% provider to the callback function.
-spec fold(LogOrView :: log() | view(),
           First :: log_index(),
           Last :: log_index() | infinity,
           SizeLimit :: non_neg_integer() | infinity,
           Func ::
                fun((Index :: log_index(), Entry :: log_entry(), Acc) -> Acc) |
                fun((Index :: log_index(), Size :: non_neg_integer(), Entry :: log_entry(), Acc) -> Acc),
           Acc) ->
    {ok, Acc} | wa_raft:error().
fold(#log_view{log = Log, first = LogFirst, last = LogLast}, First, Last, SizeLimit, Func, Acc) ->
    fold_impl(Log, max(First, LogFirst), min(Last, LogLast), SizeLimit, Func, Acc);
fold(Log, First, Last, SizeLimit, Func, Acc) ->
    Provider = provider(Log),
    LogFirst = Provider:first_index(Log),
    LogLast = Provider:last_index(Log),
    % eqwalizer:fixme - min [T166261957]
    fold_impl(Log, max(First, LogFirst), min(Last, LogLast), SizeLimit, Func, Acc).

-spec fold_impl(
    Log :: log(),
    First :: log_index(),
    Last :: log_index(),
    SizeLimit :: non_neg_integer() | infinity,
    Func ::
        fun((Index :: log_index(), Entry :: log_entry(), Acc) -> Acc) |
        fun((Index :: log_index(), Size :: non_neg_integer(), Entry :: log_entry(), Acc) -> Acc),
    Acc :: term()
) -> {ok, Acc} | wa_raft:error().
fold_impl(Log, First, Last, SizeLimit, Func, AccIn) ->
    ?RAFT_COUNT('raft.log.fold'),
    ?RAFT_COUNTV('raft.log.fold.total', Last - First + 1),
    AdjFunc = if
        is_function(Func, 3) -> fun (Index, _Size, Entry, InnerAcc) -> Func(Index, Entry, InnerAcc) end;
        is_function(Func, 4) -> Func
    end,
    Provider = provider(Log),
    case Provider:fold(Log, First, Last, SizeLimit, AdjFunc, AccIn) of
        {ok, AccOut} ->
            {ok, AccOut};
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.fold.error'),
            {error, Reason}
    end.

%% Folds over the terms in the log view of raw entries from the log provider
%% between the provided first and last log indices (inclusive).
%% If there exists a log term between the provided first and last indices then
%% the accumulator function will be called on at least that term.
%% This API provides no validation of the log indices and term passed by the
%% provider to the callback function.
-spec fold_terms(LogOrView :: log() | view(),
                 First :: log_index(),
                 Last :: log_index(),
                 Func :: fun((Index :: log_index(), Term :: log_term(), Acc) -> Acc),
                 Acc) ->
    {ok, Acc} | wa_raft:error().
fold_terms(#log_view{log = Log, first = LogFirst, last = LogLast}, First, Last, Func, Acc) ->
    fold_terms_impl(Log, max(First, LogFirst), min(Last, LogLast), Func, Acc);
fold_terms(Log, First, Last, Func, Acc) ->
    Provider = provider(Log),
    LogFirst = Provider:first_index(Log),
    LogLast = Provider:last_index(Log),
    fold_terms_impl(Log, max(First, LogFirst), min(Last, LogLast), Func, Acc).

-spec fold_terms_impl(
    Log :: log(),
    First :: log_index(),
    Last :: log_index(),
    Func :: fun((Index :: log_index(), Term :: log_term(), Acc) -> Acc),
    Acc :: term()
) -> {ok, Acc} | wa_raft:error().
fold_terms_impl(Log, First, Last, Func, AccIn) ->
    ?RAFT_COUNT('raft.log.fold_terms'),
    ?RAFT_COUNTV('raft.log.fold_terms.total', Last - First + 1),
    Provider = provider(Log),
    case Provider:fold_terms(Log, First, Last, Func, AccIn) of
        {ok, AccOut} ->
            {ok, AccOut};
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.fold_terms.error'),
            {error, Reason}
    end.

%% Gets the term of entry at the provided log index. When using a log view
%% this function may return 'not_found' even if the underlying log entry still
%% exists if the entry is outside of the log view.
-spec term(LogOrView :: log() | view(), Index :: log_index()) -> {ok, Term :: log_term()} | not_found | wa_raft:error().
term(#log_view{first = First, last = Last}, Index) when Index < First orelse Last < Index ->
    not_found;
term(#log_view{log = Log}, Index) ->
    Provider = provider(Log),
    Provider:term(Log, Index);
term(Log, Index) ->
    Provider = provider(Log),
    Provider:term(Log, Index).

%% Gets the log entry at the provided log index. When using a log view
%% this function may return 'not_found' even if the underlying log entry still
%% exists if the entry is outside of the log view.
-spec get(LogOrView :: log() | view(), Index :: log_index()) -> {ok, Entry :: log_entry()} | not_found | wa_raft:error().
get(#log_view{first = First, last = Last}, Index) when Index < First orelse Last < Index ->
    not_found;
get(#log_view{log = Log}, Index) ->
    ?RAFT_COUNT('raft.log.get'),
    Provider = provider(Log),
    Provider:get(Log, Index);
get(Log, Index) ->
    ?RAFT_COUNT('raft.log.get'),
    Provider = provider(Log),
    Provider:get(Log, Index).

-spec get(LogOrView :: log() | view(), First :: log_index(), CountLimit :: non_neg_integer()) ->
    {ok, Entries :: [log_entry()]} | wa_raft:error().
get(LogOrView, First, CountLimit) ->
    get(LogOrView, First, CountLimit, infinity).

%% Gets a contiguous range of log entries starting at the provided log index,
%% up to the specified maximum total number of bytes (based on external format).
%% If at least one log entry is requested, then at least one log entry will be
%% returned no matter what total number of bytes is specified.
%% When using a log view this function may not return all physically present
%% log entries if those entries are outside of the log view.
-spec get(LogOrView :: log() | view(), First :: log_index(), CountLimit :: non_neg_integer(), SizeLimit :: non_neg_integer() | infinity) ->
    {ok, Entries :: [log_entry()]} | wa_raft:error().
get(LogOrView, First, CountLimit, SizeLimit) ->
    try
        fold(LogOrView, First, First + CountLimit - 1, SizeLimit,
            fun
                (Index, Entry, {Index, Acc})            -> {Index + 1, [Entry | Acc]};
                (_Index, _Entry, {ExpectedIndex, _Acc}) -> throw({missing, ExpectedIndex})
            end, {First, []})
    of
        {ok, {_, EntriesRev}} -> {ok, lists:reverse(EntriesRev)};
        {error, Reason} -> {error, Reason}
    catch
        throw:{missing, Index} ->
            ?LOG_WARNING("[~p] detected log is missing index ~p during get of ~p ~~ ~p",
                [log_name(LogOrView), Index, First, First + CountLimit - 1], #{domain => [whatsapp, wa_raft]}),
            {error, corruption}
    end.

-spec get_terms(LogOrView :: log() | view(), First :: log_index(), Limit :: non_neg_integer()) ->
    {ok, Terms :: [wa_raft_log:log_term()]} | wa_raft:error().
get_terms(LogOrView, First, Limit) ->
    try
        fold_terms(LogOrView, First, First + Limit - 1,
            fun
                (Index, Term, {Index, Acc})            -> {Index + 1, [Term | Acc]};
                (_Index, _Term, {ExpectedIndex, _Acc}) -> throw({missing, ExpectedIndex})
            end, {First, []})
    of
        {ok, {_, TermsRev}} -> {ok, lists:reverse(TermsRev)};
        {error, Reason} -> {error, Reason}
    catch
        throw:{missing, Index} ->
            ?LOG_WARNING("[~p] detected log is missing index ~p during get of ~p ~~ ~p",
                [log_name(LogOrView), Index, First, First + Limit - 1], #{domain => [whatsapp, wa_raft]}),
            {error, corruption}
    end.

-spec config(LogOrView :: log() | view()) -> {ok, Index :: log_index(), Config :: wa_raft_server:config()} | not_found.
config(#log_view{config = undefined}) ->
    not_found;
config(#log_view{first = First, config = {Index, _}}) when First > Index ->
    % After trims, it is possible that we have a cached config from before the start
    % of the log view. Don't return the cached config in this case.
    not_found;
config(#log_view{config = {Index, Config}}) ->
    {ok, Index, Config};
config(Log) ->
    Provider = provider(Log),
    case Provider:config(Log) of
        {ok, Index, Config} -> {ok, Index, wa_raft_server:normalize_config(Config)};
        Other -> Other
    end.

%%-------------------------------------------------------------------
%% APIs for batching new entries and committing
%%-------------------------------------------------------------------

%% Add a new entry to the list of pending log entries in the current batch.
%% These pending log entries can be commited to the log by using `sync/1` or
%% removed from the batch using `cancel/1`.
%%
-spec submit(View :: view() , Entry :: wa_raft_log:log_entry()) -> {ok, NewView :: view()}.
submit(#log_view{pending = []} = View, Entry) ->
   {ok, View#log_view{pending = [Entry]}};
submit(#log_view{pending = [{_, {?READ_OP, noop}} | Tail]} = View, Entry) ->
   {ok, View#log_view{pending = [Entry | Tail]}};
submit(#log_view{pending = [{_, {?READ_OP, _Label, noop}} | Tail]} = View, Entry) ->
   {ok, View#log_view{pending = [Entry | Tail]}};
submit(#log_view{pending = Pending} = View, Entry) ->
   {ok, View#log_view{pending = [Entry | Pending]}}.

%% Return the number of pending log entries in the current batch.
-spec pending(View :: view()) -> Pending :: non_neg_integer().
pending(#log_view{pending = Pending}) ->
    length(Pending).

%% Append all pending log entries in the current batch to the end of the log.
%% This operation performs a 'relaxed' append which may fail. In the case that
%% the append fails, then this function will return 'skipped' and there will
%% be no change to the log nor to the pending log entries in the current batch.
-spec sync(View :: view()) -> {ok, NewView :: view()} | skipped | wa_raft:error().
sync(#log_view{log = Log, last = Last, pending = Pending} = View) ->
    ?RAFT_COUNT('raft.log.sync'),
    Entries = lists:reverse(Pending),
    Provider = provider(Log),
    case Provider:append(View, Last + 1, Entries, relaxed) of
        ok ->
            {ok, refresh_config(View#log_view{last = Last + length(Pending), pending = []})};
        skipped ->
            skipped;
        {error, Reason} ->
            {error, Reason}
    end.

%% Remove all pending log entries from the current batch, returning those log
%% entries that were removed.
-spec cancel(View :: view()) -> {ok, Cancelled :: [log_entry()], NewView :: view()}.
cancel(#log_view{pending = Pending} = View) ->
    ?RAFT_COUNT('raft.log.cancel'),
    {ok, lists:reverse(Pending), View#log_view{pending = []}}.

%%-------------------------------------------------------------------
%% APIs for managing logs and log data
%%-------------------------------------------------------------------

%% Open the specified log (registered name or pid) at the provided position.
%% If the log does not contain the provided position, then the log is reset
%% to include it. Otherwise, the log is opened as is and may contain entries
%% before and after the provided position.
-spec open(PidOrName :: pid() | log_name(), Position :: log_pos()) -> {ok, View :: view()} | wa_raft:error().
open(PidOrName, Position) ->
    gen_server:call(PidOrName, {open, Position}, infinity).

%% Reset the log backing the provided log view to contain only the provided
%% position. The log entry data at the provided position will be 'undefined'.
-spec reset(View :: view(), Position :: log_pos()) -> {ok, NewView :: view()} | wa_raft:error().
reset(#log_view{log = Log} = View, Position) ->
    gen_server:call(log_name(Log), {reset, Position, View}, infinity).

%% Truncate the log by deleting all log entries in the log at and after the
%% provided log index. This operation is required to delete all data for the
%% affected log indices.
-spec truncate(View :: view(), Index :: log_index()) -> {ok, NewView :: view()} | wa_raft:error().
truncate(#log_view{log = Log} = View, Index) ->
    gen_server:call(log_name(Log), {truncate, Index, View}, infinity).

%% Trim the log by removing log entries before the provided log index.
%% This operation is not required to remove all data before the
%% provided log index immediately and can defer this work to future
%% trimming operations. This operation is asynchronous.
-spec trim(View :: view(), Index :: log_index()) -> {ok, NewView :: view()}.
trim(#log_view{log = Log, first = First} = View, Index) ->
    gen_server:cast(log_name(Log), {trim, Index}),
    {ok, View#log_view{first = max(Index, First)}}.

%% Perform a batched trimming (rotate) of the underlying log according
%% to application environment configuration values.
-spec rotate(View :: view(), Index :: log_index()) -> {ok, NewView :: view()}.
rotate(#log_view{log = #raft_log{application = App}} = View, Index) ->
    % Current rotation configuration is based on two configuration values,
    % 'raft_max_log_records_per_file' which indicates after how many outstanding extra
    % log entries are in the log should we trim and 'raft_max_log_records' which
    % indicates how many additional log entries after the fully replicated index should
    % be considered not extraneous and be kept by rotation.
    Interval = ?RAFT_LOG_ROTATION_INTERVAL(App),
    Keep = ?RAFT_LOG_ROTATION_KEEP(App, Interval),
    rotate(View, Index, Interval, Keep).

%% Perform a batched trimming (rotate) of the underlying log where
%% we keep some number of log entries and only trigger trimming operations
%% every so often.
-spec rotate(View :: view(), Index :: log_index(), Interval :: pos_integer(), Keep :: non_neg_integer()) -> {ok, NewView :: view()}.
rotate(#log_view{first = First} = View, Index, Interval, Keep) when Index - Keep - First >= Interval ->
    ?RAFT_COUNT('raft.log.rotate'),
    trim(View, Index - Keep);
rotate(View, _Index, _Interval, _Keep) ->
    ?RAFT_COUNT('raft.log.rotate'),
    {ok, View}.

%% Try to flush any underlying log data that is not yet on disk to disk.
-spec flush(LogOrView :: log() | view()) -> ok.
flush(#log_view{log = Log}) ->
    gen_server:cast(log_name(Log), flush);
flush(Log) ->
    gen_server:cast(log_name(Log), flush).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT log server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_log_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT log server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.log_name
    end.

-spec log(View :: view()) -> Log :: log().
log(#log_view{log = Log}) ->
    Log.

-spec log_name(LogOrView :: log() | view()) -> Name :: log_name().
log_name(#log_view{log = #raft_log{name = Name}}) ->
    Name;
log_name(#raft_log{name = Name}) ->
    Name.

-spec provider(LogOrView :: log() | view()) -> Provider :: module().
provider(#log_view{log = #raft_log{provider = Provider}}) ->
    Provider;
provider(#raft_log{provider = Provider}) ->
    Provider.

-spec refresh_config(View :: view()) -> NewView :: view().
refresh_config(#log_view{log = Log} = View) ->
    Provider = provider(Log),
    case Provider:config(Log) of
        {ok, Index, Config} ->
            View#log_view{config = {Index, wa_raft_server:normalize_config(Config)}};
        not_found ->
            View#log_view{config = undefined}
    end.

%%-------------------------------------------------------------------
%% gen_server Callbacks
%%-------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> {ok, State :: #log_state{}}.
init(#raft_options{application = Application, table = Table, partition = Partition, log_name = Name, log_module = Provider}) ->
    process_flag(trap_exit, true),

    Log = #raft_log{
       name = Name,
       application = Application,
       table = Table,
       partition = Partition,
       provider = Provider
    },
    ok = Provider:init(Log),

    {ok, #log_state{log = Log}}.

-spec handle_call(Request, From :: term(), State :: #log_state{}) ->
    {reply, Reply :: term(), NewState :: #log_state{}} |
    {noreply, NewState :: #log_state{}}
    when Request ::
        {open, Position :: log_pos()} |
        {reset, Position :: log_pos(), View :: view()} |
        {truncate, Index :: log_index(), View :: view()}.
handle_call({open, Position}, _From, State) ->
    {Reply, NewState} = handle_open(Position, State),
    {reply, Reply, NewState};
handle_call({reset, Position, View}, _From, State) ->
    case handle_reset(Position, View, State) of
        {ok, NewView, NewState} ->
            {reply, {ok, NewView}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({truncate, Index, View}, _From, State) ->
    case handle_truncate(Index, View, State) of
        {ok, NewView, NewState} ->
            {reply, {ok, NewView}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(Request, From, #log_state{log = Log} = State) ->
    ?LOG_NOTICE("[~p] got unrecognized call ~p from ~p",
        [log_name(Log), Request, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request, State :: #log_state{}) -> {noreply, NewState :: #log_state{}}
    when Request :: flush | {trim, Index :: log_index()}.
handle_cast(flush, #log_state{log = Log} = State) ->
    Provider = provider(Log),
    Provider:flush(Log),
    {noreply, State};
handle_cast({trim, Index}, #log_state{log = Log} = State) ->
    case handle_trim(Index, State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            ?LOG_WARNING("[~p] failed to trim log due to ~p",
                [log_name(Log), Reason], #{domain => [whatsapp, wa_raft]}),
            {noreply, State}
    end;
handle_cast(Request, #log_state{log = Log} = State) ->
    ?LOG_NOTICE("[~p] got unrecognized cast ~p",
        [log_name(Log), Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #log_state{}) -> term().
terminate(Reason, #log_state{log = Log, state = State}) ->
    Provider = provider(Log),
    ?LOG_NOTICE("[~p] terminating due to ~p",
        [Log, Reason], #{domain => [whatsapp, wa_raft]}),
    State =/= ?PROVIDER_NOT_OPENED andalso Provider:close(Log, State).

%%-------------------------------------------------------------------
%% RAFT Log Server Logic
%%-------------------------------------------------------------------

-spec handle_open(Position :: log_pos(), State :: #log_state{}) ->
    {{ok, NewView :: view()} | wa_raft:error(), NewState :: #log_state{}}.
handle_open(#raft_log_pos{index = Index, term = Term} = Position,
            #log_state{log = #raft_log{name = Name, provider = Provider} = Log} = State0) ->
    ?RAFT_COUNT('raft.log.open'),
    ?LOG_NOTICE("[~p] opening log at position ~p:~p", [Name, Index, Term], #{domain => [whatsapp, wa_raft]}),
    case Provider:open(Log) of
        {ok, ProviderState} ->
            Action = case Provider:get(Log, Index) of
                {ok, {Term, _Op}} ->
                    none;
                {ok, {MismatchTerm, _Op}} ->
                    ?LOG_WARNING("[~p] resetting log due to expecting term ~p at ~p but log contains term ~p",
                        [Name, Term, Index, MismatchTerm], #{domain => [whatsapp, wa_raft]}),
                    reset;
                not_found ->
                    reset;
                Other ->
                    {failed, Other}
            end,

            State1 = State0#log_state{state = ProviderState},
            View0 = #log_view{log = Log},
            case Action of
                none ->
                    ?RAFT_COUNT('raft.log.open.normal'),
                    View1 = case Provider:first_index(Log) of
                        undefined ->
                            ?LOG_WARNING("[~p] opened log normally but the first index was not set",
                                [Name], #{domain => [whatsapp, wa_raft]}),
                            View0;
                        FirstIndex ->
                            View0#log_view{first = FirstIndex}
                    end,
                    View2 = case Provider:last_index(Log) of
                        undefined ->
                            ?LOG_WARNING("[~p] opened log normally but the last index was not set",
                                [Name], #{domain => [whatsapp, wa_raft]}),
                            View1;
                        LastIndex ->
                            View1#log_view{last = LastIndex}
                    end,
                    View3 = refresh_config(View2),
                    {{ok, View3}, State1};
                reset ->
                    ?RAFT_COUNT('raft.log.open.reset'),
                    case handle_reset(Position, View0, State1) of
                        {ok, View1, State2} ->
                            {{ok, View1}, State2};
                        {error, Reason} ->
                            ?RAFT_COUNT('raft.log.open.reset.error'),
                            {{error, Reason}, State1}
                    end;
                {failed, Return} ->
                    ?RAFT_COUNT('raft.log.open.error'),
                    {Return, State1}
            end;
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.open.error'),
            {{error, Reason}, State0#log_state{state = ?PROVIDER_NOT_OPENED}}
    end.

-spec handle_reset(Position :: log_pos(), View :: view(), State :: #log_state{}) ->
    {ok, NewView :: view(), NewState :: #log_state{}} | wa_raft:error().
handle_reset(_Position, _View, #log_state{state = ?PROVIDER_NOT_OPENED}) ->
    {error, not_open};
handle_reset(#raft_log_pos{index = 0, term = Term}, _View, #log_state{log = Log}) when Term =/= 0 ->
    ?LOG_ERROR("[~p] rejects reset to index 0 with non-zero term ~p", [log_name(Log), Term], #{domain => [whatsapp, wa_raft]}),
    {error, invalid_position};
handle_reset(#raft_log_pos{index = Index, term = Term} = Position, View0,
             #log_state{log = Log, state = ProviderState} = State0) ->
    ?RAFT_COUNT('raft.log.reset'),
    ?LOG_NOTICE("[~p] resetting log to position ~p:~p", [log_name(Log), Index, Term], #{domain => [whatsapp, wa_raft]}),
    Provider = provider(Log),
    case Provider:reset(Log, Position, ProviderState) of
        {ok, NewProviderState} ->
            View1 = View0#log_view{first = Index, last = Index, config = undefined},
            State1 = State0#log_state{state = NewProviderState},
            {ok, View1, State1};
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.reset.error'),
            {error, Reason}
    end.

-spec handle_truncate(Index :: log_index(), View :: view(), State :: #log_state{}) ->
    {ok, NewView :: view(), NewState :: #log_state{}} | wa_raft:error().
handle_truncate(_Index, _View, #log_state{state = ?PROVIDER_NOT_OPENED}) ->
    {error, not_open};
handle_truncate(Index, #log_view{first = First}, #log_state{log = Log}) when Index =< First ->
    ?LOG_ERROR("[~p] rejects log deletion by truncation to ~p for log starting at ~p", [log_name(Log), Index, First], #{domain => [whatsapp, wa_raft]}),
    {error, invalid_position};
handle_truncate(Index, #log_view{last = Last} = View0, #log_state{log = Log, state = ProviderState} = State0) ->
    ?RAFT_COUNT('raft.log.truncate'),
    ?LOG_NOTICE("[~p] truncating log from ~p to past ~p", [log_name(Log), Last, Index], #{domain => [whatsapp, wa_raft]}),
    Provider = provider(Log),
    case Provider:truncate(Log, Index, ProviderState) of
        {ok, NewProviderState} ->
            View1 = View0#log_view{last = min(Last, Index - 1)},
            View2 = refresh_config(View1),
            State1 = State0#log_state{state = NewProviderState},
            {ok, View2, State1};
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.truncate.error'),
            {error, Reason}
    end.

%% Trim is an asychronous operation so we do not use the view here.
%% Rather, the wa_raft_log:trim/2 API will assume that the trim succeeded and
%% optimistically update the view to advance the start of the log to the provided index.
-spec handle_trim(Index :: log_index(), State :: #log_state{}) ->
    {ok, NewState :: #log_state{}} | wa_raft:error().
handle_trim(_Index, #log_state{state = ?PROVIDER_NOT_OPENED}) ->
    {error, not_open};
handle_trim(Index, #log_state{log = Log, state = ProviderState} = State) ->
    ?RAFT_COUNT('raft.log.trim'),
    ?LOG_DEBUG("[~p] trimming log to ~p", [log_name(Log), Index], #{domain => [whatsapp, wa_raft]}),
    Provider = provider(Log),
    case Provider:trim(Log, Index, ProviderState) of
        {ok, NewProviderState} ->
            {ok, State#log_state{state = NewProviderState}};
        {error, Reason} ->
            ?RAFT_COUNT('raft.log.trim.error'),
            {error, Reason}
    end.
