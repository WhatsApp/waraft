%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% An example implementation of the RAFT storage provider behaviour that
%%% uses ETS as a backing store. This implementation is for demonstration
%%% purposes only and should not be used in actual applications.

-module(wa_raft_storage_ets).
-compile(warn_missing_spec_all).
-behaviour(wa_raft_storage).

-export([
    storage_open/2,
    storage_close/1,
    storage_label/1,
    storage_position/1,
    storage_config/1,
    storage_apply/3,
    storage_apply/4,
    storage_apply_config/3,
    storage_read/3,
    storage_create_snapshot/2,
    storage_create_witness_snapshot/2,
    storage_open_snapshot/3,
    storage_make_empty_snapshot/5
]).

-include("wa_raft.hrl").

%% Options used for the ETS table
-define(OPTIONS, [set, public, {read_concurrency, true}, {write_concurrency, true}]).

%% Filename used for the actual ETS table file in a snapshot
-define(SNAPSHOT_FILENAME, "data").

%% Tag used in keys for metadata stored on the behalf of RAFT
-define(METADATA_TAG, '$metadata').
%% Tag used for label metadata stored on behalf of RAFT.
-define(LABEL_TAG, '$label').
%% Tag used for recording the current storage position
-define(POSITION_TAG, '$position').
%% Tag used for tracking if the current storage is incomplete.
-define(INCOMPLETE_TAG, '$incomplete').

-record(state, {
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    self :: #raft_identity{},
    storage :: ets:table()
}).

-spec storage_open(#raft_options{}, file:filename()) -> #state{}.
storage_open(#raft_options{table = Table, partition = Partition, self = Self, storage_name = Name}, _RootDir) ->
    Storage = ets:new(Name, ?OPTIONS),
    #state{name = Name, table = Table, partition = Partition, self = Self, storage = Storage}.

-spec storage_close(#state{}) -> ok.
storage_close(#state{storage = Storage}) ->
    true = ets:delete(Storage),
    ok.

-spec storage_position(#state{}) -> wa_raft_log:log_pos().
storage_position(#state{storage = Storage}) ->
    ets:lookup_element(Storage, ?POSITION_TAG, 2, #raft_log_pos{}).

-spec storage_label(#state{}) -> {ok, Label :: wa_raft_label:label()}.
storage_label(#state{storage = Storage}) ->
    case ets:lookup(Storage, ?LABEL_TAG) of
        [{_, Label}] -> {ok, Label};
        []           -> {ok, undefined}
    end.

-spec storage_config(#state{}) -> {ok, wa_raft_log:log_pos(), wa_raft_server:config()} | undefined.
storage_config(#state{storage = Storage}) ->
    case ets:lookup(Storage, {?METADATA_TAG, config}) of
        [{_, {Version, Value}}] -> {ok, Version, Value};
        []                      -> undefined
    end.

-spec storage_incomplete(#state{}) -> boolean().
storage_incomplete(#state{storage = Storage}) ->
    ets:lookup_element(Storage, ?INCOMPLETE_TAG, 2, false).

-spec storage_apply(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Label :: wa_raft_label:label(), Storage :: #state{}) -> {ok, #state{}}.
storage_apply(Command, Position, Label, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, {?LABEL_TAG, Label}),
    storage_apply(Command, Position, State).

-spec storage_apply(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Storage :: #state{}) -> {ok, #state{}}.
storage_apply(noop, Position, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, {?POSITION_TAG, Position}),
    {ok, State};
storage_apply(noop_omitted, Position, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, [{?INCOMPLETE_TAG, true}, {?POSITION_TAG, Position}]),
    {ok, State};
storage_apply({write, _Table, Key, Value}, Position, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, [{Key, Value}, {?POSITION_TAG, Position}]),
    {ok, State};
storage_apply({delete, _Table, Key}, Position, #state{storage = Storage} = State) ->
    true = ets:delete(Storage, Key),
    true = ets:insert(Storage, {?POSITION_TAG, Position}),
    {ok, State}.

-spec storage_apply_config(
    Config :: wa_raft_server:config(),
    LogPos :: wa_raft_log:log_pos(),
    State :: #state{}
) -> {ok | wa_raft_storage:error(), #state{}}.
storage_apply_config(Config, LogPos, State) ->
    storage_check_config(Config, State),
    storage_apply_config(Config, LogPos, LogPos, State).

-spec storage_apply_config(
    Config :: wa_raft_server:config(),
    ConfigPos :: wa_raft_log:log_pos(),
    LogPos :: wa_raft_log:log_pos(),
    State :: #state{}
) -> {ok | wa_raft_storage:error(), #state{}}.
storage_apply_config(Config, ConfigPos, LogPos, #state{storage = Storage} = State) ->
    true = ets:insert(Storage, [{{?METADATA_TAG, config}, {ConfigPos, Config}}, {?POSITION_TAG, LogPos}]),
    {ok, State}.

-spec storage_read(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), State :: #state{}) -> ok | {ok, Value :: dynamic()} | not_found.
storage_read(noop, _Position, #state{}) ->
    ok;
storage_read({read, _Table, Key}, _Position, #state{storage = Storage}) ->
    case ets:lookup(Storage, Key) of
        [{_, Value}] -> {ok, Value};
        []           -> not_found
    end.

-spec storage_create_snapshot(file:filename(), #state{}) -> ok | wa_raft_storage:error().
storage_create_snapshot(SnapshotPath, #state{storage = Storage}) ->
    case filelib:ensure_path(SnapshotPath) of
        ok              -> ets:tab2file(Storage, filename:join(SnapshotPath, ?SNAPSHOT_FILENAME));
        {error, Reason} -> {error, Reason}
    end.

-spec storage_create_witness_snapshot(file:filename(), #state{}) -> ok | wa_raft_storage:error().
storage_create_witness_snapshot(SnapshotPath, #state{name = Name, table = Table, partition = Partition, self = Self} = State) ->
    {ok, ConfigPosition, Config} = storage_config(State),
    SnapshotPosition = storage_position(State),
    storage_make_empty_snapshot(Name, Table, Partition, Self, SnapshotPath, SnapshotPosition, Config, ConfigPosition, #{}).

-spec storage_open_snapshot(file:filename(), wa_raft_log:log_pos(), #state{}) -> {ok, #state{}} | wa_raft_storage:error().
storage_open_snapshot(SnapshotPath, SnapshotPosition, #state{storage = Storage} = State) ->
    SnapshotData = filename:join(SnapshotPath, ?SNAPSHOT_FILENAME),
    case ets:file2tab(SnapshotData) of
        {ok, NewStorage} ->
            case ets:lookup_element(NewStorage, ?POSITION_TAG, 2, #raft_log_pos{}) of
                SnapshotPosition ->
                    NewState = State#state{storage = NewStorage},
                    storage_check_config(NewState),
                    catch ets:delete(Storage),
                    {ok, NewState};
                _IncorrectPosition ->
                    catch ets:delete(NewStorage),
                    {error, bad_position}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec storage_check_config(#state{}) -> ok.
storage_check_config(State) ->
    case storage_config(State) of
        {ok, _, Config} -> storage_check_config(Config, State);
        undefined -> ok
    end.

-spec storage_check_config(wa_raft_server:config(), #state{}) -> ok.
storage_check_config(Config, #state{self = Self} = State) ->
    case storage_incomplete(State) andalso wa_raft_server:is_data_replica(Self, Config) of
        true -> error(invalid_incomplete_replica);
        false -> ok
    end.

-spec storage_make_empty_snapshot(#raft_options{}, file:filename(), wa_raft_log:log_pos(), wa_raft_server:config(), dynamic()) -> ok | wa_raft_storage:error().
storage_make_empty_snapshot(#raft_options{table = Table, partition = Partition, self = Self, storage_name = Name}, SnapshotPath, SnapshotPosition, Config, Data) ->
    storage_make_empty_snapshot(Name, Table, Partition, Self, SnapshotPath, SnapshotPosition, Config, SnapshotPosition, Data).

-spec storage_make_empty_snapshot(atom(), wa_raft:table(), wa_raft:partition(), #raft_identity{}, file:filename(), wa_raft_log:log_pos(), wa_raft_server:config(), wa_raft_log:log_pos(), dynamic()) -> ok | wa_raft_storage:error().
storage_make_empty_snapshot(Name, Table, Partition, Self, SnapshotPath, SnapshotPosition, Config, ConfigPosition, _Data) ->
    Storage = ets:new(Name, ?OPTIONS),
    State = #state{name = Name, table = Table, partition = Partition, self = Self, storage = Storage},
    storage_apply_config(Config, ConfigPosition, SnapshotPosition, State),
    storage_create_snapshot(SnapshotPath, State).
