%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements storage interface by using ets table as
%%% underlying storage. It's for testing or experimental purpose.
%%% Don't use it in prod.

-module(wa_raft_storage_ets).
-compile(warn_missing_spec).
-behaviour(wa_raft_storage).

%% Callbacks
-export([
    storage_open/1,
    storage_close/2,
    storage_apply/3,
    storage_create_snapshot/2,
    storage_create_empty_snapshot/2,
    storage_delete_snapshot/2,
    storage_open_snapshot/2,
    storage_status/1
]).

-export([
    storage_read/2,
    storage_read_version/2,
    storage_write/5,
    storage_delete/3,
    storage_match_delete/4,
    storage_write_metadata/4,
    storage_read_metadata/2
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

-type storage_handle() :: {wa_raft:table(), wa_raft:partition()}.

-define(SNAP_FILE(RootDir, Name), lists:concat([RootDir, Name, "/data"])).

%% Tag in key for metadata
-define(METADATA_TAG, '$metadata').

-spec storage_open(#raft_storage{}) -> {wa_raft_log:log_pos(), storage_handle()}.
storage_open(#raft_storage{table = Table, partition = Partition}) ->
    new_table(Table, Partition),
    {#raft_log_pos{index = 0, term = 0}, {Table, Partition}}.

-spec storage_close(storage_handle(), #raft_storage{}) -> ok.
storage_close({Table, Partition}, _State) ->
    Tab = ?RAFT_STORAGE_NAME(Table, Partition),
    catch ets:delete(Tab),
    ok.

-spec storage_create_snapshot(string(), #raft_storage{}) -> ok.
storage_create_snapshot(SnapName, #raft_storage{root_dir = RootDir, name = Tab}) ->
    SnapFile = ?SNAP_FILE(RootDir, SnapName),
    ok = filelib:ensure_dir(SnapFile),
    ok = ets:tab2file(Tab, SnapFile),
    ok.

-spec storage_create_empty_snapshot(string(), #raft_storage{}) -> ok.
storage_create_empty_snapshot(SnapName, #raft_storage{root_dir = RootDir}) ->
    SnapFile = ?SNAP_FILE(RootDir, SnapName),
    file:delete(SnapFile),
    ok = filelib:ensure_dir(SnapFile),
    ok.

-spec storage_delete_snapshot(string(), #raft_storage{}) -> ok.
storage_delete_snapshot(SnapName, #raft_storage{root_dir = RootDir}) ->
    SnapFile = ?SNAP_FILE(RootDir, SnapName),
    case filelib:is_file(SnapFile) of
        true ->
            ok = file:delete(SnapFile),
            file:del_dir(filename:dirname(SnapFile)),
            ok;
        false ->
            ok
    end.

-spec storage_open_snapshot(#raft_snapshot{}, #raft_storage{}) -> {wa_raft_log:log_pos(), storage_handle()} | wa_raft_storage:error().
storage_open_snapshot(#raft_snapshot{last_applied = LogPos, name = Name}, #raft_storage{table = Table, partition = Partition, root_dir = RootDir, name = Tab}) ->
    SnapFile = ?SNAP_FILE(RootDir, Name),
    catch ets:delete(Tab),
    case filelib:is_regular(SnapFile) of
        true ->
            case ets:file2tab(SnapFile) of
                {ok, Tab} -> ok;
                {ok, OtherTab} -> Tab = ets:rename(OtherTab, Tab)
            end,
            {LogPos, {Table, Partition}};
        false ->
            new_table(Table, Partition),
            {LogPos, {Table, Partition}}
    end.

-spec storage_status(Handle :: storage_handle()) -> wa_raft_storage:status().
storage_status(_Handle) ->
    [].

-spec storage_apply(Command :: wa_raft_acceptor:command(), LogPos :: wa_raft_log:log_pos(), Storage :: storage_handle()) -> {ok, storage_handle()} | {{ok, Return :: term()}, storage_handle()} | {wa_raft_storage:error(), storage_handle()}.
storage_apply({read, _Table, Key}, _LogPos, Handle) ->
    {ok, {LogIndex, _LogTerm, Header, Value}} = storage_read(Handle, Key),
    {{ok, {Value, Header, LogIndex}}, Handle};
storage_apply({delete, _Table, Key}, LogPos, Handle) ->
    {storage_delete(Handle, LogPos, Key), Handle};
storage_apply({write, _Table, Key, Value}, LogPos, Handle) ->
    {storage_write(Handle, LogPos, Key, Value, #{}), Handle};
storage_apply(noop, _LogPos, Handle) ->
    {ok, Handle};
storage_apply(Command, LogPos, Handle) ->
    ?LOG_ERROR("Unrecognized command ~0p at ~0p", [Command, LogPos]),
    {{error, enotsup}, Handle}.

-spec storage_write(storage_handle(), wa_raft_log:log_pos(), term(), binary(), map()) -> {ok, wa_raft_log:log_index()} | wa_raft_storage:error().
storage_write({Table, Partition}, #raft_log_pos{index = LogIndex, term = LogTerm}, Key, Value, Header) ->
    Tab = ?RAFT_STORAGE_NAME(Table, Partition),
    true = ets:insert(Tab, {Key, {LogIndex, LogTerm, Header, Value}}),
    {ok, LogIndex}.

-spec storage_read(storage_handle(), term()) ->
    {ok, {wa_raft_log:log_index(), wa_raft_log:log_term(), map(), binary()} | {undefined, undefined, map(), undefined}} | wa_raft_storage:error().
storage_read({Table, Partition}, Key) ->
    Tab = ?RAFT_STORAGE_NAME(Table, Partition),
    try ets:lookup(Tab, Key) of
        [{_, {LogIndex, LogTerm, Header, Value}}] -> {ok, {LogIndex, LogTerm, Header, Value}};
        [] -> {ok, {undefined, undefined, #{}, undefined}}
    catch
        _:Error ->
            ?LOG_ERROR("Read table ~p key ~p error ~p", [Tab, Key, Error], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.ets.read.error'),
            {error, enoent}
    end.

-spec storage_read_version(storage_handle(), term()) -> wa_raft_log:log_index() | undefined | wa_raft_storage:error().
storage_read_version({Table, Partition}, Key) ->
    Tab = ?RAFT_STORAGE_NAME(Table, Partition),
    try ets:lookup(Tab, Key) of
        [{_, {LogIndex, _LogTerm, _Header, _Value}}] -> LogIndex;
        [] -> undefined
    catch
        _:Error ->
            ?LOG_ERROR("Read table ~p key ~p error ~p", [Tab, Key, Error], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.ets.read_version.error'),
            {error, enoent}
    end.

-spec storage_delete(storage_handle(), wa_raft_log:log_pos(), term()) -> ok.
storage_delete({Table, Partition}, _LogPos, Key) ->
    Tab = ?RAFT_STORAGE_NAME(Table, Partition),
    true = ets:delete(Tab, Key),
    ok.

-spec storage_match_delete(storage_handle(), term(), wa_raft_log:log_index(), wa_raft_log:log_term()) -> ok.
storage_match_delete({Table, Partition}, Key, MatchLogIndex, MatchLogTerm) ->
    Tab = ?RAFT_STORAGE_NAME(Table, Partition),
    true = ets:match_delete(Tab, {Key, {MatchLogIndex, MatchLogTerm, '_', '_'}}),
    ok.

-spec storage_write_metadata(storage_handle(), wa_raft_storage:metadata(), wa_raft_log:log_pos(), term()) -> ok | wa_raft_storage:error().
storage_write_metadata(Handle, Key, Version, Value) ->
    {ok, _Index} = storage_write(Handle, Version, {?METADATA_TAG, Key}, term_to_binary(Value), #{}),
    ok.

-spec storage_read_metadata(storage_handle(), wa_raft_storage:metadata()) -> {ok, wa_raft_log:log_pos(), term()} | undefined | wa_raft_storage:error().
storage_read_metadata(Handle, Key) ->
    case storage_read(Handle, {?METADATA_TAG, Key}) of
        {ok, {undefined, _, _, _}} ->
            undefined;
        {ok, {Index, Term, _Meta, Binary}} ->
            {ok, #raft_log_pos{index = Index, term = Term}, binary_to_term(Binary)};
        Other ->
            Other
    end.

-spec new_table(wa_raft:table(), wa_raft:partition()) -> ets:tid() | atom().
new_table(Table, Partition) ->
    ets:new(?RAFT_STORAGE_NAME(Table, Partition), [set, named_table, public, {read_concurrency, true}, {write_concurrency, true}]).
