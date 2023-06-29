%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% An example implementation of the RAFT storage provider behaviour that
%%% uses ETS as a backing store. This implementation is for demonstration
%%% purposes only and should not be used in actual applications.

-module(wa_raft_storage_ets).
-compile(warn_missing_spec).
-behaviour(wa_raft_storage).

-export([
    storage_open/4,
    storage_close/1,
    storage_position/1,
    storage_apply/3,
    storage_write_metadata/4,
    storage_read/3,
    storage_read_metadata/2,
    storage_create_snapshot/2,
    storage_open_snapshot/3
]).

-include("wa_raft.hrl").

%% Filename used to store ETS table in a snapshot
-define(SNAPSHOT_FILENAME, "data").

%% Tag used in keys for metadata stored on the behalf of RAFT
-define(METADATA_TAG, '$metadata').

-record(state, {
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    position = #raft_log_pos{} :: wa_raft_log:log_pos()
}).

-spec storage_open(atom(), wa_raft:table(), wa_raft:partition(), file:filename()) -> #state{}.
storage_open(Name, Table, Partition, _RootDir) ->
    Name = ets:new(Name, [set, named_table, public, {read_concurrency, true}, {write_concurrency, true}]),
    #state{name = Name, table = Table, partition = Partition}.

-spec storage_close(#state{}) -> ok.
storage_close(#state{name = Name}) ->
    true = ets:delete(Name),
    ok.

-spec storage_position(#state{}) -> wa_raft_log:log_pos().
storage_position(#state{position = Position}) ->
    Position.

-spec storage_apply(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Storage :: #state{}) -> {ok, #state{}}.
storage_apply(noop, Position, #state{} = State) ->
    {ok, State#state{position = Position}};
storage_apply({write, _Table, Key, Value}, Position, #state{name = Name} = State) ->
    true = ets:insert(Name, {Key, Value}),
    {ok, State#state{position = Position}};
storage_apply({delete, _Table, Key}, Position, #state{name = Name} = State) ->
    true = ets:delete(Name, Key),
    {ok, State#state{position = Position}}.

-spec storage_write_metadata(#state{}, wa_raft_storage:metadata(), wa_raft_log:log_pos(), term()) -> ok.
storage_write_metadata(#state{name = Name}, Key, Version, Value) ->
    true = ets:insert(Name, {{?METADATA_TAG, Key}, {Version, Value}}),
    ok.

-spec storage_read(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), State :: #state{}) -> ok | {ok, Value :: eqwalizer:dynamic()} | not_found.
storage_read(noop, _Position, #state{}) ->
    ok;
storage_read({read, _Table, Key}, _Position, #state{name = Name}) ->
    case ets:lookup(Name, Key) of
        [{_, Value}] -> {ok, Value};
        []           -> not_found
    end.

-spec storage_read_metadata(#state{}, wa_raft_storage:metadata()) -> {ok, wa_raft_log:log_pos(), term()} | undefined.
storage_read_metadata(#state{name = Name}, Key) ->
    case ets:lookup(Name, {?METADATA_TAG, Key}) of
        [{_, {Version, Value}}] -> {ok, Version, Value};
        []                      -> undefined
    end.

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
