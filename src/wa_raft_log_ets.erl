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
-compile(warn_missing_spec_all).
-behaviour(wa_raft_log).

%% RAFT log provider interface for accessing log data
-export([
    first_index/1,
    last_index/1,
    fold/6,
    fold_terms/5,
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
    init/1,
    open/1,
    close/2,
    reset/3,
    truncate/3,
    trim/3,
    flush/1
]).

-include_lib("wa_raft/include/wa_raft.hrl").

-type state() :: undefined.

%%-------------------------------------------------------------------
%% RAFT log provider interface for accessing log data
%%-------------------------------------------------------------------

-spec first_index(Log :: wa_raft_log:log()) -> undefined | wa_raft_log:log_index().
first_index(#raft_log{name = Name}) ->
    case ets:first(Name) of
        '$end_of_table' -> undefined;
        Key             -> Key
    end.

-spec last_index(Log :: wa_raft_log:log()) -> undefined | wa_raft_log:log_index().
last_index(#raft_log{name = Name}) ->
    case ets:last(Name) of
        '$end_of_table' -> undefined;
        Key             -> Key
    end.

-spec fold(Log :: wa_raft_log:log(),
           Start :: wa_raft_log:log_index() | '$end_of_table',
           End :: wa_raft_log:log_index(),
           SizeLimit :: non_neg_integer() | infinity,
           Func :: fun((Index :: wa_raft_log:log_index(), Size :: non_neg_integer(), Entry :: wa_raft_log:log_entry(), Acc) -> Acc),
           Acc) -> {ok, Acc}.
fold(Log, Start, End, SizeLimit, Func, Acc) ->
    fold_impl(Log, Start, End, 0, SizeLimit, Func, Acc).

-spec fold_impl(
    Log :: wa_raft_log:log(),
    Start :: wa_raft_log:log_index() | '$end_of_table',
    End :: wa_raft_log:log_index(),
    Size :: non_neg_integer(),
    SizeLimit :: non_neg_integer() | infinity,
    Func :: fun((Index :: wa_raft_log:log_index(), Size :: non_neg_integer(), Entry :: wa_raft_log:log_entry(), Acc) -> Acc),
    Acc
) -> {ok, Acc}.
fold_impl(_Log, Start, End, Size, SizeLimit, _Func, Acc) when End < Start orelse Size >= SizeLimit ->
    {ok, Acc};
fold_impl(#raft_log{name = Name} = Log, Start, End, Size, SizeLimit, Func, Acc) ->
    case ets:lookup(Name, Start) of
        [{Start, Entry}] ->
            EntrySize = erlang:external_size(Entry),
            fold_impl(Log, ets:next(Name, Start), End, Size + EntrySize, SizeLimit, Func, Func(Start, EntrySize, Entry, Acc));
        [] ->
            fold_impl(Log, ets:next(Name, Start), End, Size, SizeLimit, Func, Acc)
    end.

-spec fold_terms(Log :: wa_raft_log:log(),
    Start :: wa_raft_log:log_index() | '$end_of_table',
    End :: wa_raft_log:log_index(),
    Func :: fun((Index :: wa_raft_log:log_index(), Entry :: wa_raft_log:log_term(), Acc) -> Acc),
    Acc) -> {ok, Acc}.
fold_terms(Log, Start, End, Func, Acc) ->
    fold_terms_impl(Log, Start, End, Func, Acc).

-spec fold_terms_impl(
    Log :: wa_raft_log:log(),
    Start :: wa_raft_log:log_index() | '$end_of_table',
    End :: wa_raft_log:log_index(),
    Func :: fun((Index :: wa_raft_log:log_index(), Term :: wa_raft_log:log_term(), Acc) -> Acc),
    Acc
    ) -> {ok, Acc}.
fold_terms_impl(_Log, Start, End, _Func, Acc) when End < Start ->
    {ok, Acc};
fold_terms_impl(#raft_log{name = Name} = Log, Start, End, Func, Acc) ->
    case ets:lookup(Name, Start) of
        [{Start, {Term, _Op}}] ->
            fold_terms_impl(Log, ets:next(Name, Start), End, Func, Func(Start, Term, Acc));
        [] ->
            fold_terms_impl(Log, ets:next(Name, Start), End, Func, Acc)
        end.

-spec get(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index()) -> {ok, Entry :: wa_raft_log:log_entry()} | not_found.
get(#raft_log{name = Name}, Index) ->
    case ets:lookup(Name, Index) of
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
config(#raft_log{name = Name}) ->
    case ets:select_reverse(Name, [{{'$1', {'_', {'_', {config, '$2'}}}}, [], [{{'$1', '$2'}}]}], 1) of
        {[{Index, Config}], _Cont} -> {ok, Index, Config};
        _                          -> not_found
    end.

%%-------------------------------------------------------------------
%% RAFT log provider interface for writing new log data
%%-------------------------------------------------------------------

-spec append(View :: wa_raft_log:view(), Entries :: [wa_raft_log:log_entry() | binary()], Mode :: strict | relaxed, Priority :: wa_raft_acceptor:priority()) -> ok.
append(View, Entries, _Mode, _Priority) ->
    Name = wa_raft_log:log_name(View),
    Last = wa_raft_log:last_index(View),
    true = ets:insert(Name, append_decode(Last + 1, Entries)),
    ok.

-spec append_decode(Index :: wa_raft_log:log_index(), Entries :: [wa_raft_log:log_entry() | binary()]) ->
    [{wa_raft_log:log_index(), wa_raft_log:log_entry()}].
append_decode(_, []) ->
    [];
append_decode(Index, [Entry | Entries]) ->
    NewEntry =
        case is_binary(Entry) of
            true -> binary_to_term(Entry);
            false -> Entry
        end,
    [{Index, NewEntry} | append_decode(Index + 1, Entries)].

%%-------------------------------------------------------------------
%% RAFT log provider interface for managing underlying RAFT log
%%-------------------------------------------------------------------

-spec init(Log :: wa_raft_log:log()) -> ok.
init(#raft_log{name = LogName}) ->
    ets:new(LogName, [ordered_set, public, named_table]),
    ok.

-spec open(Log :: wa_raft_log:log()) -> {ok, State :: state()}.
open(_Log) ->
    {ok, undefined}.

-spec close(Log :: wa_raft_log:log(), State :: state()) -> term().
close(_Log, _State) ->
    ok.

-spec reset(Log :: wa_raft_log:log(), Position :: wa_raft_log:log_pos(), State :: state()) ->
    {ok, NewState :: state()}.
reset(#raft_log{name = Name}, #raft_log_pos{index = Index, term = Term}, State) ->
    true = ets:delete_all_objects(Name),
    true = ets:insert(Name, {Index, {Term, undefined}}),
    {ok, State}.

-spec truncate(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index() | '$end_of_table', State :: state()) ->
    {ok, NewState :: state()}.
truncate(_Log, '$end_of_table', State) ->
    {ok, State};
truncate(#raft_log{name = Name} = Log, Index, State) ->
    true = ets:delete(Name, Index),
    truncate(Log, ets:next(Name, Index), State).

-spec trim(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index(), State :: state()) ->
    {ok, NewState :: state()}.
trim(Log, Index, State) ->
    trim_impl(Log, Index - 1),
    {ok, State}.

-spec trim_impl(Log :: wa_raft_log:log(), Index :: wa_raft_log:log_index() | '$end_of_table') -> ok.
trim_impl(_Log, '$end_of_table') ->
    ok;
trim_impl(#raft_log{name = Name} = Log, Index) ->
    true = ets:delete(Name, Index),
    trim_impl(Log, ets:prev(Name, Index)).

-spec flush(Log :: wa_raft_log:log()) -> term().
flush(_Log) ->
    ok.
