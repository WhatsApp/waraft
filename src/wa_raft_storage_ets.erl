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
    storage_open/4,
    storage_position/1,
    storage_close/1,
    storage_apply/3,
    storage_create_snapshot/2,
    storage_create_empty_snapshot/2,
    storage_delete_snapshot/2,
    storage_open_snapshot/2,
    storage_status/1
]).

-export([
    storage_write_metadata/4,
    storage_read_metadata/2
]).

%% Test API
-export([
    storage_read/2
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

-define(SNAPSHOT_FILE(Table, Partition, Name), filename:join(?RAFT_SNAPSHOT_PATH(Table, Partition, Name), "data")).

-record(state, {
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    position :: wa_raft_log:log_pos()
}).

%% Tag in key for metadata
-define(METADATA_TAG, '$metadata').

-spec storage_open(atom(), wa_raft:table(), wa_raft:partition(), file:filename()) -> #state{}.
storage_open(_Name, Table, Partition, _RootDir) ->
    new_table(Table, Partition),
    #state{table = Table, partition = Partition, position = #raft_log_pos{index = 0, term = 0}}.

-spec storage_position(#state{}) -> wa_raft_log:log_pos().
storage_position(#state{position = Position}) ->
    Position.

-spec storage_close(#state{}) -> ok.
storage_close(#state{table = Table, partition = Partition}) ->
    Tab = wa_raft_storage:registered_name(Table, Partition),
    true = ets:delete(Tab),
    ok.

-spec storage_create_snapshot(string(), #raft_storage{}) -> ok.
storage_create_snapshot(SnapName, #raft_storage{table = Table, partition = Partition, name = Tab}) ->
    SnapFile = ?SNAPSHOT_FILE(Table, Partition, SnapName),
    ok = filelib:ensure_dir(SnapFile),
    ok = ets:tab2file(Tab, SnapFile),
    ok.

-spec storage_create_empty_snapshot(string(), #raft_storage{}) -> ok.
storage_create_empty_snapshot(SnapName, #raft_storage{table = Table, partition = Partition}) ->
    SnapFile = ?SNAPSHOT_FILE(Table, Partition, SnapName),
    file:delete(SnapFile),
    ok = filelib:ensure_dir(SnapFile),
    ok.

-spec storage_delete_snapshot(string(), #raft_storage{}) -> ok.
storage_delete_snapshot(SnapName, #raft_storage{table = Table, partition = Partition}) ->
    SnapFile = ?SNAPSHOT_FILE(Table, Partition, SnapName),
    case filelib:is_file(SnapFile) of
        true ->
            ok = file:delete(SnapFile),
            file:del_dir(filename:dirname(SnapFile)),
            ok;
        false ->
            ok
    end.

-spec storage_open_snapshot(#raft_snapshot{}, #state{}) -> {ok, #state{}} | wa_raft_storage:error().
storage_open_snapshot(#raft_snapshot{last_applied = LogPos, name = Name}, #state{table = Table, partition = Partition} = State) ->
    Tab = wa_raft_storage:registered_name(Table, Partition),
    Temp = list_to_atom(atom_to_list(Tab) ++ "_temp"),
    Temp = ets:rename(Tab, Temp),
    SnapFile = ?SNAPSHOT_FILE(Table, Partition, Name),
    case filelib:is_regular(SnapFile) of
        true ->
            case ets:file2tab(SnapFile) of
                {ok, NewTab} ->
                    NewTab =/= Tab andalso (Tab = ets:rename(NewTab, Tab)),
                    catch ets:delete(Temp),
                    {ok, State#state{position = LogPos}};
                {error, Reason} ->
                    Tab = ets:rename(Temp, Tab),
                    {error, Reason}
            end;
        false ->
            Tab = ets:rename(Temp, Tab),
            {error, invalid_snapshot}
    end.

-spec storage_status(Handle :: #state{}) -> wa_raft_storage:status().
storage_status(_Handle) ->
    [].

-spec storage_apply(Command :: wa_raft_acceptor:command(), LogPos :: wa_raft_log:log_pos(), Storage :: #state{}) -> {ok, #state{}} | {{ok, Return :: term()}, #state{}} | {wa_raft_storage:error(), #state{}}.
storage_apply({read, _Table, Key}, LogPos, Handle) ->
    {ok, {LogIndex, _LogTerm, Header, Value}} = storage_read(Handle, Key),
    {{ok, {Value, Header, LogIndex}}, Handle#state{position = LogPos}};
storage_apply({delete, _Table, Key}, LogPos, Handle) ->
    {storage_delete(Handle, LogPos, Key), Handle#state{position = LogPos}};
storage_apply({write, _Table, Key, Value}, LogPos, Handle) ->
    {storage_write(Handle, LogPos, Key, Value, #{}), Handle#state{position = LogPos}};
storage_apply(noop, LogPos, Handle) ->
    {ok, Handle#state{position = LogPos}};
storage_apply(Command, LogPos, Handle) ->
    ?LOG_ERROR("Unrecognized command ~0p at ~0p", [Command, LogPos]),
    {{error, enotsup}, Handle#state{position = LogPos}}.

-spec storage_write(#state{}, wa_raft_log:log_pos(), term(), binary(), map()) -> {ok, wa_raft_log:log_index()} | wa_raft_storage:error().
storage_write(#state{table = Table, partition = Partition}, #raft_log_pos{index = LogIndex, term = LogTerm}, Key, Value, Header) ->
    Tab = wa_raft_storage:registered_name(Table, Partition),
    true = ets:insert(Tab, {Key, {LogIndex, LogTerm, Header, Value}}),
    {ok, LogIndex}.

-spec storage_read(#state{} | {wa_raft:table(), wa_raft:partition()}, term()) ->
    {ok, {wa_raft_log:log_index(), wa_raft_log:log_term(), map(), binary()} | {undefined, undefined, map(), undefined}} | wa_raft_storage:error().
storage_read({Table, Partition}, Key) ->
    Tab = wa_raft_storage:registered_name(Table, Partition),
    try ets:lookup(Tab, Key) of
        [{_, {LogIndex, LogTerm, Header, Value}}] -> {ok, {LogIndex, LogTerm, Header, Value}};
        [] -> {ok, {undefined, undefined, #{}, undefined}}
    catch
        _:Error ->
            ?LOG_ERROR("Read table ~p key ~p error ~p", [Tab, Key, Error], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.ets.read.error'),
            {error, enoent}
    end;
storage_read(#state{table = Table, partition = Partition}, Key) ->
    storage_read({Table, Partition}, Key).

-spec storage_delete(#state{}, wa_raft_log:log_pos(), term()) -> ok.
storage_delete(#state{table = Table, partition = Partition}, _LogPos, Key) ->
    Tab = wa_raft_storage:registered_name(Table, Partition),
    true = ets:delete(Tab, Key),
    ok.

-spec storage_write_metadata(#state{}, wa_raft_storage:metadata(), wa_raft_log:log_pos(), term()) -> ok | wa_raft_storage:error().
storage_write_metadata(Handle, Key, Version, Value) ->
    {ok, _Index} = storage_write(Handle, Version, {?METADATA_TAG, Key}, term_to_binary(Value), #{}),
    ok.

-spec storage_read_metadata(#state{}, wa_raft_storage:metadata()) -> {ok, wa_raft_log:log_pos(), term()} | undefined | wa_raft_storage:error().
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
    ets:new(wa_raft_storage:registered_name(Table, Partition), [set, named_table, public, {read_concurrency, true}, {write_concurrency, true}]).
