%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module is an implementation of a completely in-memory RAFT
%%% log provider that uses ETS as a backing store for the log data.
%%% This module is only suitable as a log provider for an fully
%%% in-memory RAFT cluster and should not be used when any durability
%%% guarantees are required against node shutdown.

-module(wa_raft_log_ets).
-compile(warn_missing_spec).
-author('hsun324@fb.com').

-behaviour(wa_raft_log).

%% RAFT log provider interface for accessing log data
-export([
    first_index/1,
    last_index/1,
    fold/5,
    get/2,
    term/2,
    config/1
]).

%% RAFT log provider interface for writing new log data
-export([
    append/4
]).

%% RAFT log provider interface for managing underlying RAFT log
-export([
    init/2,
    open/2,
    close/2,
    reset/3,
    truncate/3,
    trim/3,
    flush/1
]).

-include("wa_raft.hrl").

-type state() :: undefined.

%%-------------------------------------------------------------------
%% RAFT log provider interface for accessing log data
%%-------------------------------------------------------------------

-spec first_index(Log :: wa_raft_log:log()) -> undefined | wa_raft_log:log_index().
first_index(Log) ->
    case ets:first(Log) of
        '$end_of_table' -> undefined;
        Key             -> Key
    end.

-spec last_index(Log :: wa_raft_log:log()) -> undefined | wa_raft_log:log_index().
last_index(Log) ->
    case ets:last(Log) of
        '$end_of_table' -> undefined;
        Key             -> Key
    end.

-spec fold(Log :: wa_raft_log:log(),
           Start :: wa_raft_log:log_index() | '$end_of_table',
           End :: wa_raft_log:log_index(),
           Func :: fun((Index :: wa_raft_log:log_index(), Entry :: wa_raft_log:log_entry(), Size :: non_neg_integer(), AccIn :: term()) -> AccOut :: term()),
           Acc0 :: term()) -> {ok, AccN :: term()}.
fold(_Log, Start, End, _Func, Acc) when End < Start ->
    {ok, Acc};
fold(Log, Start, End, Func, Acc) ->
    case ets:lookup(Log, Start) of
        [{Start, Entry}] -> fold(Log, ets:next(Log, Start), End, Func, Func(Start, Entry, erlang:external_size(Entry), Acc));
        []               -> fold(Log, ets:next(Log, Start), End, Func, Acc)
    end.

-spec get(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index()) -> {ok, Entry :: wa_raft_log:log_entry()} | not_found.
get(Log, Index) ->
    case ets:lookup(Log, Index) of
        [{Index, Entry}] -> {ok, Entry};
        []               -> not_found
    end.

-spec term(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index()) -> {ok, Term :: wa_raft_log:log_term()} | not_found.
term(Log, Index) ->
    case get(Log, Index) of
        {ok, {Term, _Op}} -> {ok, Term};
        not_found         -> not_found
    end.

-spec config(Log :: wa_raft_log:log()) -> {ok, Index :: wa_raft_log:log_index(), Entry :: wa_raft_server:config()} | not_found.
config(Log) ->
    case ets:select_reverse(Log, [{{'$1', {'_', {'_', {config, '$2'}}}}, [], [{{'$1', '$2'}}]}], 1) of
        {[{Index, Config}], _Cont} -> {ok, Index, Config};
        _                          -> not_found
    end.

%%-------------------------------------------------------------------
%% RAFT log provider interface for writing new log data
%%-------------------------------------------------------------------

-spec append(View :: wa_raft_log:view(), Start :: wa_raft_log:log_index(), Entries :: [wa_raft_log:log_entry()], Mode :: strict | relaxed) ->
    ok | {mismatch, Index :: wa_raft_log:log_index()} | skipped | wa_raft_log:error().
append(View, Start, Entries, _Mode) ->
    Log = wa_raft_log:log(View),
    append_impl(Log, Start, Entries, first_index(Log), last_index(Log)).

append_impl(_Log, _Start, [], _First, _Last) ->
    ok;
append_impl(Log, Start, [_ | Entries], First, Last) when Start < First ->
    append_impl(Log, Start + 1, Entries, First, Last);
append_impl(Log, Start, [{Term, _Entry} | Entries], First, Last) when Start =< Last ->
    case term(Log, Start) of
        {ok, Term} -> append_impl(Log, Start + 1, Entries, First, Last);
        _          -> {mismatch, Start}
    end;
append_impl(Log, Start, [_|_] = Entries, _First, Last) when Start =:= Last + 1 ->
    true = ets:insert(Log,
        lists:zip(
            lists:seq(Start, Start + length(Entries) - 1),
            Entries)),
    ok;
append_impl(_Log, _Start, [_|_], _First, _Last) ->
    {error, invalid_start_index}.

%%-------------------------------------------------------------------
%% RAFT log provider interface for managing underlying RAFT log
%%-------------------------------------------------------------------

-spec init(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok.
init(Table, Partition) ->
    Log = ?RAFT_LOG_NAME(Table, Partition),
    Log = ets:new(Log, [ordered_set, public, named_table]),
    ok.

-spec open(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> {ok, State :: state()}.
open(_Table, _Partition) ->
    {ok, undefined}.

-spec close(Log :: wa_raft_log:log(), State :: state()) -> any().
close(_Log, _State) ->
    ok.

-spec reset(Log :: wa_raft_log:log(), Position :: wa_raft_log:log_pos(), State :: state()) ->
    {ok, NewState :: state()}.
reset(Log, #raft_log_pos{index = Index, term = Term}, State) ->
    true = ets:delete_all_objects(Log),
    true = ets:insert(Log, {Index, {Term, undefined}}),
    {ok, State}.

-spec truncate(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index() | '$end_of_table', State :: state()) ->
    {ok, NewState :: state()}.
truncate(_Log, '$end_of_table', State) ->
    {ok, State};
truncate(Log, Index, State) ->
    true = ets:delete(Log, Index),
    truncate(Log, ets:next(Log, Index), State).

-spec trim(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index(), State :: state()) ->
    {ok, NewState :: state()}.
trim(Log, Index, State) ->
    trim_impl(Log, Index - 1),
    {ok, State}.

trim_impl(_Log, '$end_of_table') ->
    ok;
trim_impl(Log, Index) ->
    true = ets:delete(Log, Index),
    trim_impl(Log, ets:prev(Log, Index)).

-spec flush(Log :: wa_raft_log:log()) -> any().
flush(_Log) ->
    ok.
