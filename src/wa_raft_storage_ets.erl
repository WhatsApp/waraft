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
    storage_open_snapshot/3,
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

-define(SNAPSHOT_FILENAME, "data").

-record(state, {
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    position = #raft_log_pos{} :: wa_raft_log:log_pos()
}).

%% Tag in key for metadata
-define(METADATA_TAG, '$metadata').

-spec storage_open(atom(), wa_raft:table(), wa_raft:partition(), file:filename()) -> #state{}.
storage_open(Name, Table, Partition, _RootDir) ->
    #state{name = new_table(Name), table = Table, partition = Partition}.

-spec storage_position(#state{}) -> wa_raft_log:log_pos().
storage_position(#state{position = Position}) ->
    Position.

-spec storage_close(#state{}) -> ok.
storage_close(#state{name = Name}) ->
    true = ets:delete(Name),
    ok.

-spec storage_create_snapshot(file:filename(), #state{}) -> ok | wa_raft_storage:error().
storage_create_snapshot(SnapshotPath, #state{name = Name}) ->
    case filelib:ensure_path(SnapshotPath) of
        ok              -> ets:tab2file(Name, filename:join(SnapshotPath, ?SNAPSHOT_FILENAME));
        {error, Reason} -> {error, Reason}
    end.

-spec storage_open_snapshot(file:filename(), wa_raft_log:log_pos(), #state{}) -> {ok, #state{}} | wa_raft_storage:error().
storage_open_snapshot(SnapshotPath, LastApplied, #state{name = Name} = State) ->
    Temp = list_to_atom(atom_to_list(Name) ++ "_temp"),
    Temp = ets:rename(Name, Temp),
    SnapshotData = filename:join(SnapshotPath, ?SNAPSHOT_FILENAME),
    case ets:file2tab(SnapshotData) of
        {ok, Created} ->
            Created =/= Name andalso (Name = ets:rename(Created, Name)),
            catch ets:delete(Temp),
            {ok, State#state{position = LastApplied}};
        {error, Reason} ->
            Name = ets:rename(Temp, Name),
            {error, Reason}
    end.

-spec storage_status(Handle :: #state{}) -> wa_raft_storage:status().
storage_status(_Handle) ->
    [].

-spec storage_apply(Command :: wa_raft_acceptor:command(), LogPos :: wa_raft_log:log_pos(), Storage :: #state{}) ->
    {ok | {ok, eqwalizer:dynamic()} | not_found | wa_raft_storage:error(), #state{}}.
storage_apply(noop, LogPos, State) ->
    {ok, State#state{position = LogPos}};
storage_apply({read, _Table, Key}, LogPos, State) ->
    {storage_read(Key, State), State#state{position = LogPos}};
storage_apply({write, _Table, Key, Value}, LogPos, State) ->
    {storage_write(Key, Value, State), State#state{position = LogPos}};
storage_apply({delete, _Table, Key}, LogPos, State) ->
    {storage_delete(Key, State), State#state{position = LogPos}};
storage_apply(Command, LogPos, State) ->
    ?LOG_ERROR("Unrecognized command ~0p at ~0p", [Command, LogPos]),
    {{error, enotsup}, State#state{position = LogPos}}.

-spec storage_read(term(), #state{} | atom()) -> {ok, eqwalizer:dynamic()} | not_found | wa_raft_storage:error().
storage_read(Key, Name) when is_atom(Name) ->
    try ets:lookup(Name, Key) of
        [{_, Value}] -> {ok, Value};
        []           -> not_found
    catch
        _:Error ->
            ?LOG_ERROR("Read table ~p key ~p error ~p", [Name, Key, Error], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.ets.read.error'),
            {error, Error}
    end;
storage_read(Key, #state{name = Name}) ->
    storage_read(Key, Name).

-spec storage_write(term(), term(), #state{}) -> ok.
storage_write(Key, Value, #state{name = Name}) ->
    true = ets:insert(Name, {Key, Value}),
    ok.

-spec storage_delete(term(), #state{}) -> ok.
storage_delete(Key, #state{name = Name}) ->
    true = ets:delete(Name, Key),
    ok.

-spec storage_write_metadata(#state{}, wa_raft_storage:metadata(), wa_raft_log:log_pos(), term()) -> ok.
storage_write_metadata(State, Key, Version, Value) ->
    storage_write({?METADATA_TAG, Key}, {Version, Value}, State).

-spec storage_read_metadata(#state{}, wa_raft_storage:metadata()) -> {ok, wa_raft_log:log_pos(), term()} | undefined | wa_raft_storage:error().
storage_read_metadata(State, Key) ->
    case storage_read({?METADATA_TAG, Key}, State) of
        {ok, {Version, Value}} -> {ok, Version, Value};
        not_found              -> undefined;
        {error, Reason}        -> {error, Reason}
    end.

-spec new_table(Name :: atom()) -> atom().
new_table(Name) ->
    Name = ets:new(Name, [set, named_table, public, {read_concurrency, true}, {write_concurrency, true}]).
