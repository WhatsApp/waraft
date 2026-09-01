%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

%% OTP supervision
-export([
    child_spec/0,
    start_link/0
]).

%% Bulk Transfer API
-export([
    start_transfer/4,
    start_transfer/5,
    transfer/5
]).

%% Snapshot Transfer API
-export([
    may_accept/3,
    start_snapshot_transfer/6,
    start_snapshot_transfer/7,
    transfer_snapshot/7
]).

%% Transport API
-export([
    cancel/2
]).

%% Transport Status
-export([
    transports/0,
    transport_info/1,
    transport_info/2
]).

%% File Status
-export([
    file_info/2
]).

%% Transport Implementation APIs
-export([
    start_file/2,
    advance_file/3,
    complete_file/3
]).

%% ETS API
-export([
    setup_tables/0
]).

%% Internal API - Configuration
-export([
    default_directory/1,
    registered_directory/2,
    registered_module/2
]).

%% Internal API - Transport Workers
-export([
    next_file/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Test exports
-ifdef(TEST).
-export([
    resolve_transport_files/2
]).
-endif.

-export_type([
    transport_id/0,
    transport_info/0,
    file_id/0,
    file_info/0,
    meta/0
]).

-include_lib("kernel/include/file.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_logger.hrl").

%%------------------------------------------------------------------------------
%% Public and Internal Types - Transport
%%------------------------------------------------------------------------------

-type side() :: sender | receiver.
-type status() :: requested | running | completed | failed.

-define(STATUS_REQUESTED, 0).
-define(STATUS_RUNNING, 1).
-define(STATUS_COMPLETED, 2).
-define(STATUS_FAILED, 4).

-type transport_id() :: pos_integer().
-type transport_info() :: #{
    type := side(),
    status := status(),
    error => term(),

    peer := atom(),
    module := module(),
    meta := meta(),

    root := string(),

    start_ts := Millis :: integer(),
    end_ts => Millis :: integer(),
    updated_ts := Millis :: integer(),

    total_files := non_neg_integer(),
    completed_files := non_neg_integer(),
    current_file := non_neg_integer()
}.

-type transport_record() :: #{
    type := side(),
    peer := atom(),
    module := module(),
    meta := meta(),
    root := string()
}.
-type transport_row() ::
    {ID :: transport_id(), Record :: transport_record(), Error :: term(), TransportAtomics :: atomics:atomics_ref()}.

-define(TRANSPORT_STATUS_IDX, 1).
-define(TRANSPORT_START_TS_IDX, 2).
-define(TRANSPORT_END_TS_IDX, 3).
-define(TRANSPORT_UPDATED_TS_IDX, 4).
-define(TRANSPORT_TOTAL_FILES_IDX, 5).
-define(TRANSPORT_COMPLETED_FILES_IDX, 6).
-define(TRANSPORT_CURRENT_FILE_IDX, 7).
-define(TRANSPORT_ATOMICS_COUNT, 7).

-type meta() :: meta_transfer() | meta_snapshot().
-type meta_transfer() :: #{
    type := transfer,
    table := wa_raft:table(),
    partition := wa_raft:partition()
}.
-type meta_snapshot() :: #{
    type := snapshot,
    table := wa_raft:table(),
    partition := wa_raft:partition(),
    position := wa_raft_log:log_pos(),
    witness := boolean()
}.

%%------------------------------------------------------------------------------
%% Public and Internal Types - File
%%------------------------------------------------------------------------------

-type file_id() :: pos_integer().
-type file_info() :: #{
    type := side(),
    status := status(),
    error => term(),

    name := string(),
    path := string(),
    meta => map(),
    mtime => integer(),

    retries := non_neg_integer(),
    start_ts => Millis :: integer(),
    end_ts => Millis :: integer(),
    updated_ts => Millis :: integer(),

    total_bytes := non_neg_integer(),
    completed_bytes := non_neg_integer()
}.

-type file_record() :: #{
    type := side(),
    name := string(),
    path := string(),
    meta => map(),
    mtime => integer()
}.
-type file_key() :: {ID :: transport_id(), FileID :: file_id()}.
-type file_row() ::
    {
        Key :: file_key(),
        Record :: file_record(),
        Error :: term(),
        TransportAtomics :: atomics:atomics_ref(),
        FileAtomics :: atomics:atomics_ref()
    }.

-define(FILE_STATUS_IDX, 1).
-define(FILE_RETRIES_IDX, 2).
-define(FILE_START_TS_IDX, 3).
-define(FILE_END_TS_IDX, 4).
-define(FILE_UPDATED_TS_IDX, 5).
-define(FILE_TOTAL_BYTES_IDX, 6).
-define(FILE_COMPLETED_BYTES_IDX, 7).
-define(FILE_ATOMICS_COUNT, 7).

%%% ------------------------------------------------------------------------
%%%  Behaviour callbacks
%%%

%% Perform any setup required before transport can be started.
-callback transport_init(Node :: node()) -> {ok, State :: term()} | {stop, Reason :: term()}.

%% Send a file to the target peer.
-callback transport_send(ID :: transport_id(), FileID :: file_id(), State :: term()) ->
    {ok, NewState :: term()} |
    {continue, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.

%% Optional callback for when a transport has completed.
-callback transport_complete(ID :: transport_id()) -> ok.

%% Optional callback for performing any shutdown operations.
-callback transport_terminate(Reason :: term(), State :: term()) -> term().

%% Optional callback allowing the transport implementation to reject an incoming
%% transport before it is accepted, e.g. when the receiver is under disk pressure.
%% IncomingBytes is the total size of the files about to be received.
-callback transport_accept(Meta :: meta(), IncomingBytes :: non_neg_integer()) -> ok | {error, Reason :: term()}.

-optional_callbacks([
    transport_complete/1,
    transport_terminate/2,
    transport_accept/2
]).

%% Name of the ETS table to keep records for transports
-define(TRANSPORT_TABLE, wa_raft_transport_transports).
%% Name of the ETS table to keep records for files
-define(FILE_TABLE, wa_raft_transport_files).

-define(RAFT_TRANSPORT_PARTITION_SUBDIRECTORY, "transport").

-define(RAFT_TRANSPORT_SCAN_INTERVAL_SECS, 30).

-define(GLOBAL_ACTIVE_INCOMING_IDX, 1).
-define(GLOBAL_ACTIVE_INCOMING_WITNESS_IDX, 2).
-define(GLOBAL_ATOMICS_COUNT, 2).

%% Minimum signed value allowed in atomics field
-define(EMPTY_TIMESTAMP, -16#8000000000000000).

-define(MAY_ACCEPT(Witness), {may_accept, Witness}).

%%% ------------------------------------------------------------------------

-record(state, {
    global_atomics :: atomics:atomics_ref(),
    pending_notify = #{} :: #{transport_id() => gen_server:from()}
}).

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        modules => [?MODULE]
    }.

-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% ------------------------------------------------------------------------
%%%  Internal API
%%%

-spec start_transport(Peer :: atom(), Meta :: meta(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
start_transport(Peer, Meta, Root, Timeout) ->
    gen_server:call(?MODULE, {start, Peer, Meta, Root}, Timeout).

-spec start_transport_and_wait(Peer :: atom(), Meta :: meta(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
start_transport_and_wait(Peer, Meta, Root, Timeout) ->
    gen_server:call(?MODULE, {start_wait, Peer, Meta, Root}, Timeout).

%%% ------------------------------------------------------------------------
%%%  Bulk Transfer API
%%%

-spec start_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), Root :: string()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
start_transfer(Peer, Table, Partition, Root) ->
    start_transfer(Peer, Table, Partition, Root, 10000).

-spec start_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
start_transfer(Peer, Table, Partition, Root, Timeout) ->
    start_transport(Peer, #{type => transfer, table => Table, partition => Partition}, Root, Timeout).

-spec transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
transfer(Peer, Table, Partition, Root, Timeout) ->
    start_transport_and_wait(Peer, #{type => transfer, table => Table, partition => Partition}, Root, Timeout).

%%% ------------------------------------------------------------------------
%%%  Snapshot Transfer API
%%%

%% Ask a peer whether it currently has capacity to accept an incoming snapshot
%% transport. This is advisory and reserves nothing: the peer performs the
%% authoritative check again when the transport is offered, so a concurrent
%% sender may still consume the last slot in between. Peers that do not support
%% the query answer 'unsupported' so that callers fall back to offering the
%% transport directly.
-spec may_accept(Peer :: atom(), Witness :: boolean(), Timeout :: timeout()) ->
    ok | {error, receiver_overloaded} | {error, unsupported}.
may_accept(Peer, Witness, Timeout) ->
    try gen_server:call({?MODULE, Peer}, ?MAY_ACCEPT(Witness), Timeout) of
        ok                                  -> ok;
        {error, receiver_overloaded}        -> {error, receiver_overloaded};
        _                                   -> {error, unsupported}
    catch
        exit:_ -> {error, unsupported}
    end.

-spec start_snapshot_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string(), Witness :: boolean()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
start_snapshot_transfer(Peer, Table, Partition, LogPos, Root, Witness) ->
    start_snapshot_transfer(Peer, Table, Partition, LogPos, Root, Witness, 10000).

-spec start_snapshot_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string(), Witness :: boolean(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
start_snapshot_transfer(Peer, Table, Partition, LogPos, Root, Witness, Timeout) ->
    start_transport(Peer, #{type => snapshot, table => Table, partition => Partition, position => LogPos, witness => Witness}, Root, Timeout).

-spec transfer_snapshot(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string(), Witness :: boolean(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | {error, Reason :: term()}.
transfer_snapshot(Peer, Table, Partition, LogPos, Root, Witness, Timeout) ->
    start_transport_and_wait(Peer, #{type => snapshot, table => Table, partition => Partition, position => LogPos, witness => Witness}, Root, Timeout).

%%% ------------------------------------------------------------------------
%%%  Transport API
%%%

-spec cancel(ID :: transport_id(), Reason :: term()) -> ok | {error, Reason :: term()}.
cancel(ID, Reason) ->
    gen_server:call(?MODULE, {cancel, ID, Reason}).

-spec complete(ID :: transport_id(), FileID :: file_id(), Status :: dynamic()) -> ok.
complete(ID, FileID, Status) ->
    gen_server:cast(?MODULE, {complete, ID, FileID, Status}).

%%------------------------------------------------------------------------------
%% Public API - Transport Status
%%------------------------------------------------------------------------------

-spec transports() -> [transport_id()].
transports() ->
    ets:select(?TRANSPORT_TABLE, [{{'$1', '_', '_', '_'}, [], ['$1']}]).

-spec transport_info(ID :: transport_id()) -> {ok, Info :: transport_info()} | not_found.
transport_info(ID) ->
    case transport_lookup(ID) of
        [] ->
            not_found;
        [{_, Record, Error, TransportAtomics}] ->
            Info0 = maybe_add_error(Error, Record),
            Info1 = Info0#{
                status => decode_status(atomics:get(TransportAtomics, ?TRANSPORT_STATUS_IDX)),
                start_ts => atomics:get(TransportAtomics, ?TRANSPORT_START_TS_IDX),
                updated_ts => atomics:get(TransportAtomics, ?TRANSPORT_UPDATED_TS_IDX),
                total_files => atomics:get(TransportAtomics, ?TRANSPORT_TOTAL_FILES_IDX),
                completed_files => atomics:get(TransportAtomics, ?TRANSPORT_COMPLETED_FILES_IDX),
                current_file => atomics:get(TransportAtomics, ?TRANSPORT_CURRENT_FILE_IDX)
            },
            Info2 = maybe_add_timestamp(end_ts, TransportAtomics, ?TRANSPORT_END_TS_IDX, Info1),
            {ok, Info2}
    end.

-spec transport_lookup(ID :: transport_id()) -> [transport_row()].
transport_lookup(ID) ->
    ets:lookup(?TRANSPORT_TABLE, ID).

-spec transport_info(ID :: transport_id(), Item :: atom()) -> Info :: term() | undefined.
transport_info(ID, Item) ->
    case transport_info(ID) of
        {ok, #{Item := Value}} -> Value;
        _                      -> undefined
    end.

%%------------------------------------------------------------------------------
%% Public API - File Status
%%------------------------------------------------------------------------------

-spec file_info(ID :: transport_id(), FileID :: file_id()) -> {ok, Info :: file_info()} | not_found.
file_info(ID, FileID) ->
    case file_lookup(ID, FileID) of
        [] ->
            not_found;
        [{_, Record, Error, _, FileAtomics}] ->
            Info0 = maybe_add_error(Error, Record),
            Info1 = Info0#{
                status => decode_status(atomics:get(FileAtomics, ?FILE_STATUS_IDX)),
                retries => atomics:get(FileAtomics, ?FILE_RETRIES_IDX),
                total_bytes => atomics:get(FileAtomics, ?FILE_TOTAL_BYTES_IDX),
                completed_bytes => atomics:get(FileAtomics, ?FILE_COMPLETED_BYTES_IDX)
            },
            Info2 = maybe_add_timestamp(start_ts, FileAtomics, ?FILE_START_TS_IDX, Info1),
            Info3 = maybe_add_timestamp(end_ts, FileAtomics, ?FILE_END_TS_IDX, Info2),
            Info4 = maybe_add_timestamp(updated_ts, FileAtomics, ?FILE_UPDATED_TS_IDX, Info3),
            {ok, Info4}
    end.

-spec file_lookup(ID :: transport_id(), FileID :: file_id()) -> [file_row()].
file_lookup(ID, FileID) ->
    ets:lookup(?FILE_TABLE, {ID, FileID}).

-spec maybe_add_error(Error :: term(), Map :: #{Keys => Values}) -> #{Keys => Values, error => term()}.
maybe_add_error(undefined, Map) ->
    Map;
maybe_add_error(Error, Map) ->
    Map#{error => Error}.

-spec maybe_add_timestamp(
    Key :: atom(),
    Atomics :: atomics:atomics_ref(),
    Index :: pos_integer(),
    Map :: map()
) -> map().
maybe_add_timestamp(Key, Atomics, Index, Map) ->
    case atomics:get(Atomics, Index) of
        ?EMPTY_TIMESTAMP -> Map;
        Timestamp -> Map#{Key => Timestamp}
    end.

%%------------------------------------------------------------------------------
%% Public API - Transport Implementation APIs
%%------------------------------------------------------------------------------

-spec start_file(ID :: transport_id(), FileID :: file_id()) -> ok.
start_file(ID, FileID) ->
    case file_lookup(ID, FileID) of
        [] ->
            ok;
        [{_, _, _, TransportAtomics, FileAtomics}] ->
            case atomics:get(FileAtomics, ?FILE_STATUS_IDX) of
                ?STATUS_REQUESTED ->
                    atomics:put(FileAtomics, ?FILE_STATUS_IDX, ?STATUS_RUNNING),
                    atomics:put(FileAtomics, ?FILE_START_TS_IDX, erlang:system_time(millisecond)),
                    update_file_updated_ts(FileAtomics),
                    update_transport_updated_ts(TransportAtomics),
                    ok;
                _ ->
                    ok
            end
    end.

-spec advance_file(ID :: transport_id(), FileID :: file_id(), NewCompleted :: non_neg_integer()) ->
    Prev :: non_neg_integer().
advance_file(ID, FileID, NewCompleted) ->
    case file_lookup(ID, FileID) of
        [] ->
            0;
        [{_, _, _, TransportAtomics, FileAtomics}] ->
            Prev = atomics:exchange(FileAtomics, ?FILE_COMPLETED_BYTES_IDX, NewCompleted),
            update_file_updated_ts(FileAtomics),
            update_transport_updated_ts(TransportAtomics),
            Prev
    end.

-spec complete_file(ID :: transport_id(), FileID :: file_id(), Status :: term()) -> ok.
complete_file(ID, FileID, Status) ->
    complete(ID, FileID, Status),
    ok.

%%------------------------------------------------------------------------------
%% Internal API - ETS Tables
%%------------------------------------------------------------------------------

-spec setup_tables() -> ok.
setup_tables() ->
    ?TRANSPORT_TABLE = ets:new(?TRANSPORT_TABLE, [named_table, set, public]),
    ?FILE_TABLE = ets:new(?FILE_TABLE, [named_table, set, public]),
    ok.

-spec decode_status(Encoded :: integer()) -> Status :: status().
decode_status(?STATUS_REQUESTED) -> requested;
decode_status(?STATUS_RUNNING) -> running;
decode_status(?STATUS_COMPLETED) -> completed;
decode_status(?STATUS_FAILED) -> failed.

-spec update_global_active_incoming(
    Record :: transport_record(),
    PrevStatus :: status() | undefined,
    NewStatus :: status(),
    State :: #state{}
) -> ok.
update_global_active_incoming(#{type := sender}, _, _, _) ->
    ok;
update_global_active_incoming(_, Status, Status, _) ->
    ok;
update_global_active_incoming(Record, _, running, #state{global_atomics = GlobalAtomics}) ->
    atomics:add(GlobalAtomics, global_active_incoming_idx(Record), 1);
update_global_active_incoming(Record, running, _, #state{global_atomics = GlobalAtomics}) ->
    atomics:sub(GlobalAtomics, global_active_incoming_idx(Record), 1).

-spec global_active_incoming_idx(Record :: transport_record()) -> pos_integer().
global_active_incoming_idx(#{meta := #{witness := true}}) -> ?GLOBAL_ACTIVE_INCOMING_WITNESS_IDX;
global_active_incoming_idx(_) -> ?GLOBAL_ACTIVE_INCOMING_IDX.

-spec register_transport(
    ID :: transport_id(),
    Record :: transport_record(),
    TotalFiles :: non_neg_integer(),
    State :: #state{}
) -> {ok, TransportAtomics :: atomics:atomics_ref()}.
register_transport(ID, Record, TotalFiles, State) ->
    % atomics always start with value 0
    TransportAtomics = atomics:new(?TRANSPORT_ATOMICS_COUNT, [{signed, true}]),
    atomics:put(TransportAtomics, ?TRANSPORT_STATUS_IDX, ?STATUS_RUNNING),
    atomics:put(TransportAtomics, ?TRANSPORT_START_TS_IDX, erlang:system_time(millisecond)),
    atomics:put(TransportAtomics, ?TRANSPORT_END_TS_IDX, ?EMPTY_TIMESTAMP),
    update_transport_updated_ts(TransportAtomics),
    atomics:put(TransportAtomics, ?TRANSPORT_TOTAL_FILES_IDX, TotalFiles),
    ets:insert(?TRANSPORT_TABLE, {ID, Record, undefined, TransportAtomics}),
    update_global_active_incoming(Record, undefined, running, State),
    {ok, TransportAtomics}.

-spec transport_is_running(TransportAtomics :: atomics:atomics_ref()) -> boolean().
transport_is_running(TransportAtomics) ->
    atomics:get(TransportAtomics, ?TRANSPORT_STATUS_IDX) =:= ?STATUS_RUNNING.

-spec increment_transport_completed_files(ID :: transport_id(), State :: #state{}) -> #state{}.
increment_transport_completed_files(ID, State) ->
    case transport_lookup(ID) of
        [] ->
            State;
        [{_, Record, _, TransportAtomics}] ->
            case transport_is_running(TransportAtomics) of
                true ->
                    TotalFiles = atomics:get(TransportAtomics, ?TRANSPORT_TOTAL_FILES_IDX),
                    case atomics:add_get(TransportAtomics, ?TRANSPORT_COMPLETED_FILES_IDX, 1) of
                        TotalFiles -> complete_transport_impl(ID, Record, TransportAtomics, State);
                        _ -> State
                    end;
                false ->
                    State
            end
    end.

-spec complete_transport(ID :: transport_id(), State :: #state{}) -> #state{}.
complete_transport(ID, State) ->
    case transport_lookup(ID) of
        [] ->
            State;
        [{_, Record, _, TransportAtomics}] ->
            case transport_is_running(TransportAtomics) of
                true  -> complete_transport_impl(ID, Record, TransportAtomics, State);
                false -> State
            end
    end.

-spec complete_transport_impl(
    ID :: transport_id(),
    Record :: transport_record(),
    TransportAtomics :: atomics:atomics_ref(),
    State :: #state{}
) -> #state{}.
complete_transport_impl(ID, Record, TransportAtomics, State) ->
    update_transport_updated_ts(TransportAtomics),
    NewStatus =
        case maybe_notify_complete(ID, Record, State) of
            ok ->
                update_transport_status(TransportAtomics, ?STATUS_COMPLETED),
                completed;
            {error, Reason} ->
                update_transport_error(ID, {notify_failed, Reason}),
                update_transport_status(TransportAtomics, ?STATUS_FAILED),
                failed
        end,
    set_transport_end_ts(TransportAtomics),
    update_global_active_incoming(Record, running, NewStatus, State),
    maybe_notify(ID, Record, TransportAtomics, State).

-spec fail_transport(ID :: transport_id(), Error :: term(), State :: #state{}) -> {boolean(), #state{}}.
fail_transport(ID, Error, State) ->
    case transport_lookup(ID) of
        [] ->
            {false, State};
        [{_, Record, _, TransportAtomics}] ->
            case atomics:get(TransportAtomics, ?TRANSPORT_STATUS_IDX) of
                ?STATUS_RUNNING ->
                    {true, fail_transport_impl(ID, Record, TransportAtomics, Error, State)};
                _ ->
                    {false, State}
            end
    end.

-spec fail_transport_impl(
    ID :: transport_id(),
    Record :: transport_record(),
    TransportAtomics :: atomics:atomics_ref(),
    Error :: term(),
    State :: #state{}
) -> #state{}.
fail_transport_impl(ID, Record, TransportAtomics, Error, State) ->
    update_transport_updated_ts(TransportAtomics),
    update_transport_error(ID, Error),
    update_transport_status(TransportAtomics, ?STATUS_FAILED),
    set_transport_end_ts(TransportAtomics),
    update_global_active_incoming(Record, running, failed, State),
    maybe_notify(ID, Record, TransportAtomics, State).

-spec update_transport_error(ID :: transport_id(), Error :: term()) -> ok.
update_transport_error(ID, Error) ->
    ets:update_element(?TRANSPORT_TABLE, ID, {3, Error}),
    ok.

-spec update_transport_status(TransportAtomics :: atomics:atomics_ref(), Status :: non_neg_integer()) -> ok.
update_transport_status(TransportAtomics, Status) ->
    atomics:put(TransportAtomics, ?TRANSPORT_STATUS_IDX, Status).

-spec set_transport_end_ts(TransportAtomics :: atomics:atomics_ref()) -> ok.
set_transport_end_ts(TransportAtomics) ->
    atomics:put(TransportAtomics, ?TRANSPORT_END_TS_IDX, erlang:system_time(millisecond)).

-spec update_transport_updated_ts(TransportAtomics :: atomics:atomics_ref()) -> ok.
update_transport_updated_ts(TransportAtomics) ->
    atomics:put(TransportAtomics, ?TRANSPORT_UPDATED_TS_IDX, erlang:system_time(millisecond)).

-spec delete_transport_info(ID :: transport_id()) -> ok | not_found.
delete_transport_info(ID) ->
    case transport_lookup(ID) of
        [{_, _, _, TransportAtomics}] ->
            TotalFiles = atomics:get(TransportAtomics, ?TRANSPORT_TOTAL_FILES_IDX),
            lists:foreach(fun (FileID) -> delete_file_info(ID, FileID) end, lists:seq(1, TotalFiles)),
            ets:delete(?TRANSPORT_TABLE, ID),
            ok;
        [] ->
            not_found
    end.

-spec register_file(
    ID :: transport_id(),
    FileID :: file_id(),
    Record :: file_record(),
    TransportAtomics :: atomics:atomics_ref(),
    TotalBytes :: non_neg_integer()
) -> ok.
register_file(ID, FileID, Record, TransportAtomics, TotalBytes) ->
    % atomics always start with value zero
    FileAtomics = atomics:new(?FILE_ATOMICS_COUNT, [{signed, true}]),
    atomics:put(FileAtomics, ?FILE_START_TS_IDX, ?EMPTY_TIMESTAMP),
    atomics:put(FileAtomics, ?FILE_END_TS_IDX, ?EMPTY_TIMESTAMP),
    atomics:put(FileAtomics, ?FILE_UPDATED_TS_IDX, ?EMPTY_TIMESTAMP),
    atomics:put(FileAtomics, ?FILE_TOTAL_BYTES_IDX, TotalBytes),
    ets:insert(?FILE_TABLE, {{ID, FileID}, Record, undefined, TransportAtomics, FileAtomics}),
    ok.

-spec complete_or_fail_file(
    Table :: wa_raft:table(),
    ID :: transport_id(),
    FileID :: file_id(),
    Status :: term(),
    State :: #state{}
) -> {boolean(), #state{}}.
complete_or_fail_file(Table, ID, FileID, Status, State) ->
    case file_lookup(ID, FileID) of
        [] ->
            {false, State};
        [{_, _, _, TransportAtomics, FileAtomics}] ->
            % A file may complete directly from "requested" when its transport never
            % transitioned it to "running" (e.g. the dist receiver path, or a zero-byte
            % file that carries no data chunk). Terminal states are rejected so a
            % duplicate completion reports "completed twice" rather than double-counting.
            case atomics:get(FileAtomics, ?FILE_STATUS_IDX) of
                Encoded when Encoded =:= ?STATUS_REQUESTED; Encoded =:= ?STATUS_RUNNING ->
                    StartMillis = atomics:get(FileAtomics, ?FILE_START_TS_IDX),
                    StartMillis =/= ?EMPTY_TIMESTAMP andalso
                        ?RAFT_GATHER_LATENCY(Table, {'transport.file.send.latency_ms', Status}, erlang:system_time(millisecond) - StartMillis),
                    update_file_updated_ts(FileAtomics),
                    NewState = case Status of
                        ok ->
                            update_file_status(FileAtomics, ?STATUS_COMPLETED),
                            increment_transport_completed_files(ID, State);
                        _ ->
                            update_file_error(ID, FileID, Status),
                            update_file_status(FileAtomics, ?STATUS_FAILED),
                            {_, State1} = fail_transport(ID, {file, FileID, Status}, State),
                            State1
                    end,
                    set_file_end_ts(FileAtomics),
                    update_transport_updated_ts(TransportAtomics),
                    {true, NewState};
                _ ->
                    {false, State}
            end
    end.

-spec update_file_error(ID :: transport_id(), FileID :: file_id(), Error :: term()) -> ok.
update_file_error(ID, FileID, Error) ->
    ets:update_element(?FILE_TABLE, {ID, FileID}, {3, Error}),
    ok.

-spec update_file_status(FileAtomics :: atomics:atomics_ref(), Status :: non_neg_integer()) -> ok.
update_file_status(FileAtomics, Status) ->
    atomics:put(FileAtomics, ?FILE_STATUS_IDX, Status).

-spec set_file_end_ts(FileAtomics :: atomics:atomics_ref()) -> ok.
set_file_end_ts(FileAtomics) ->
    atomics:put(FileAtomics, ?FILE_END_TS_IDX, erlang:system_time(millisecond)).

-spec update_file_updated_ts(FileAtomics :: atomics:atomics_ref()) -> ok.
update_file_updated_ts(FileAtomics) ->
    atomics:put(FileAtomics, ?FILE_UPDATED_TS_IDX, erlang:system_time(millisecond)).

-spec delete_file_info(ID :: transport_id(), FileID :: file_id()) -> ok.
delete_file_info(ID, FileID) ->
    ets:delete(?FILE_TABLE, {ID, FileID}),
    ok.

%%-------------------------------------------------------------------
%% Internal API - Configuration
%%-------------------------------------------------------------------

%% Get the default directory for incoming transports associated with the
%% provided RAFT partition given that RAFT partition's database directory.
-spec default_directory(Database :: file:filename()) -> Directory :: file:filename().
default_directory(Database) ->
    filename:join(Database, ?RAFT_TRANSPORT_PARTITION_SUBDIRECTORY).

%% Get the registered directory for incoming transports associated with the
%% provided RAFT partition or 'undefined' if no registration exists.
-spec registered_directory(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Directory :: file:filename() | undefined.
registered_directory(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> undefined;
        Options   -> Options#raft_options.transport_directory
    end.

%% Get the registered module for outgoing transports associated with the
%% provided RAFT partition or the default transport module if no registration exists.
-spec registered_module(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Module :: module() | undefined.
registered_module(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> ?RAFT_DEFAULT_TRANSPORT_MODULE;
        Options   -> Options#raft_options.transport_module
    end.

%%-------------------------------------------------------------------
%% Internal API - Transport Workers
%%-------------------------------------------------------------------

-spec next_file(ID :: transport_id()) -> {ok, FileID :: file_id()} | empty | not_found.
next_file(ID) ->
    case transport_lookup(ID) of
        [] ->
            not_found;
        [{_, _, _, TransportAtomics}] ->
            case transport_is_running(TransportAtomics) of
                false ->
                    empty;
                true ->
                    TotalFiles = atomics:get(TransportAtomics, ?TRANSPORT_TOTAL_FILES_IDX),
                    Next = atomics:add_get(TransportAtomics, ?TRANSPORT_CURRENT_FILE_IDX, 1),
                    case Next =< TotalFiles andalso Next > 0 of
                        true -> {ok, Next};
                        false -> empty
                    end
            end
    end.

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init(Args :: []) -> {ok, State :: #state{}}.
init(_) ->
    process_flag(trap_exit, true),
    GlobalAtomics = atomics:new(?GLOBAL_ATOMICS_COUNT, []),
    schedule_scan(),
    {ok, #state{global_atomics = GlobalAtomics}}.

-spec handle_call(Request, From :: gen_server:from(), State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} | {noreply, NewState :: #state{}}
    when
        Request ::
            {may_accept, Witness :: boolean()} |
            {start, Peer :: node(), Meta :: meta(), Root :: string()} |
            {start_wait, Peer :: node(), Meta :: meta(), Root :: string()} |
            {transport, ID :: transport_id(), Peer :: node(), Module :: module(), Meta :: meta(), Files :: [{file_id(), RelPath :: string(), Size :: integer()}]} |
            {cancel, ID :: transport_id(), Reason :: term()}.
handle_call(?MAY_ACCEPT(Witness), _From, State) ->
    {reply, check_capacity(Witness, State), State};
handle_call({start, Peer, Meta, Root}, _From, State) ->
    {Result, NewState} = handle_transport_start(undefined, Peer, Meta, Root, State),
    {reply, Result, NewState};
handle_call({start_wait, Peer, Meta, Root}, From, State) ->
    case handle_transport_start(From, Peer, Meta, Root, State) of
        {{ok, _ID}, NewState}       -> {noreply, NewState};
        {{error, Reason}, NewState} -> {reply, {error, Reason}, NewState}
    end;
handle_call({transport, ID, Peer, Module, Meta, Files}, From, State) ->
    Table = maps:get(table, Meta, undefined),
    try
        IsWitness = maps:get(witness, Meta, false),
        Admission = case check_capacity(IsWitness, State) of
            {error, _} = Throttled ->
                Throttled;
            ok ->
                IncomingBytes = lists:sum([Size || {_FileID, _Name, Size} <- Files]),
                transport_accept(Module, Meta, IncomingBytes)
        end,
        case {transport_info(ID), Admission} of
            {{ok, _Info}, _} ->
                ?RAFT_LOG_WARNING("wa_raft_transport got duplicate transport receive start for ~p from ~p", [ID, From]),
                {reply, duplicate, State};
            {not_found, {error, receiver_overloaded}} ->
                {reply, {error, receiver_overloaded}, State};
            {not_found, {error, Reason}} ->
                ?RAFT_COUNT(Table, 'transport.receive.rejected'),
                ?RAFT_LOG_WARNING("wa_raft_transport rejecting transport receive for ~p due to ~p", [ID, Reason]),
                {reply, {error, Reason}, State};
            {not_found, ok} ->
                RootDir = transport_destination(ID, Meta),
                case resolve_transport_files(RootDir, Files) of
                    {ok, ResolvedFiles} ->
                        ?RAFT_COUNT(Table, 'transport.receive'),
                        ?RAFT_LOG_NOTICE("wa_raft_transport starting transport receive for ~p", [ID]),

                        TotalFiles = length(Files),

                        % Force the receiving directory to always exist
                        try filelib:ensure_dir([RootDir, $/]) catch _:_ -> ok end,

                        % Setup overall transport info
                        TransportRecord = #{
                            type => receiver,
                            peer => Peer,
                            module => Module,
                            meta => Meta,
                            root => RootDir
                        },
                        {ok, TransportAtomics} = register_transport(ID, TransportRecord, TotalFiles, State),

                        % Setup file info for each file
                        [
                            begin
                                FileRecord = #{
                                    type => receiver,
                                    name => RelativePath,
                                    path => Path
                                },
                                register_file(ID, FileID, FileRecord, TransportAtomics, Size)
                            end || {FileID, RelativePath, Path, Size} <- ResolvedFiles
                        ],

                        % If the transport is empty, then immediately complete it
                        NewState = case TotalFiles of
                            0 -> complete_transport(ID, State);
                            _ -> State
                        end,

                        {reply, ok, NewState};
                    {error, invalid_file_path} ->
                        ?RAFT_COUNT(Table, 'transport.receive.rejected'),
                        ?RAFT_LOG_WARNING(
                            "wa_raft_transport rejecting transport receive for ~p from ~p due to invalid file path",
                            [ID, Peer]
                        ),
                        {reply, {error, invalid_file_path}, State}
                end
        end
    catch
        T:E:S ->
            ?RAFT_COUNT(Table, 'transport.receive.error'),
            ?RAFT_LOG_WARNING("wa_raft_transport failed to accept transport ~0p~n~s", [ID, erl_error:format_exception(T, E, S)]),
            {_, FailedState} = fail_transport(ID, {receive_failed, {T, E, S}}, State),
            {reply, {error, failed}, FailedState}
    end;
handle_call({cancel, ID, Reason}, _, State) ->
    ?RAFT_LOG_NOTICE("cancelling transport ~0p for reason ~0P", [ID, Reason, 20]),
    case fail_transport(ID, {cancelled, Reason}, State) of
        {true, NewState}  -> {reply, ok, NewState};
        {false, NewState} -> {reply, {error, not_found}, NewState}
    end;
handle_call(Request, From, #state{} = State) ->
    ?RAFT_LOG_WARNING("received unrecognized call ~0P from ~0p", [Request, 20, From]),
    {reply, {error, unsupported}, State}.

-spec handle_cast(Request, State :: #state{}) -> {noreply, NewState :: #state{}}
    when Request :: {complete, ID :: transport_id(), FileID :: file_id(), Status :: term()}.
handle_cast({complete, ID, FileID, Status}, State) ->
    Table = case transport_info(ID) of
        {ok, #{meta := Meta}} -> maps:get(table, Meta, undefined);
        _                     -> undefined
    end,
    ?RAFT_COUNT(Table, {'transport.file.send', normalize_status(Status)}),
    {Handled, NewState} = complete_or_fail_file(Table, ID, FileID, Status, State),
    Handled orelse
        ?RAFT_LOG_WARNING("for transport ~0p, file ~0p completed twice or is missing file or transport record", [ID, FileID]),
    {noreply, NewState};
handle_cast(Request, State) ->
    ?RAFT_LOG_NOTICE("received unrecognized cast ~0P", [Request, 20]),
    {noreply, State}.

-spec handle_info(Info :: term(), State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_info(scan, State) ->
    {InactiveTransports, NewState} = scan_transports(State),
    ExcessTransports = length(InactiveTransports) - ?RAFT_TRANSPORT_INACTIVE_INFO_LIMIT(),
    ExcessTransports > 0 andalso begin
        ExcessTransportIDs = lists:sublist(lists:sort(InactiveTransports), ExcessTransports),
        lists:foreach(fun delete_transport_info/1, ExcessTransportIDs)
    end,

    schedule_scan(),
    {noreply, NewState};
handle_info(Info, State) ->
    ?RAFT_LOG_NOTICE("wa_raft_transport got unrecognized info ~p", [Info]),
    {noreply, State}.

%%% ------------------------------------------------------------------------
%%%  Helper functions
%%%

-spec make_id() -> non_neg_integer().
make_id() ->
    NowMicros = erlang:system_time(microsecond),
    ID = NowMicros * 1000000 + rand:uniform(1000000) - 1,
    case transport_info(ID) of
        {ok, _Info} -> make_id();
        not_found   -> ID
    end.

-spec handle_transport_start(From :: gen_server:from() | undefined, Peer :: node(), Meta :: meta(), Root :: string(), State :: #state{}) ->
    {{ok, ID :: transport_id()} | {error, Reason :: term()}, NewState :: #state{}}.
handle_transport_start(From, Peer, Meta, Root, State) ->
    ID = make_id(),
    Table = maps:get(table, Meta, undefined),

    ?RAFT_COUNT(Table, 'transport.start'),
    ?RAFT_LOG_NOTICE("starting transport ~0p of ~0p to ~0p with metadata ~0P", [ID, Root, Peer, Meta, 20]),

    try
        Files = collect_files(Root),
        Module = transport_module(Meta),
        TotalFiles = length(Files),

        % Notify peer node of incoming transport
        FileData = [{FileID, Filename, Size} || {FileID, Filename, _, _, Size} <- Files],
        case gen_server:call({?MODULE, Peer}, {transport, ID, node(), Module, Meta, FileData}, ?RAFT_RPC_CALL_TIMEOUT()) of
            ok ->
                % Setup overall transport info
                TransportRecord = #{
                    type => sender,
                    peer => Peer,
                    module => Module,
                    meta => Meta,
                    root => Root
                },
                {ok, TransportAtomics} = register_transport(ID, TransportRecord, TotalFiles, State),

                % Setup file info for each file
                [
                    begin
                        FileRecord = #{
                            type => sender,
                            name => Filename,
                            path => Path,
                            mtime => MTime
                        },
                        register_file(ID, FileID, FileRecord, TransportAtomics, Size)
                    end || {FileID, Filename, Path, MTime, Size} <- Files
                ],

                % Complete transport if empty or start workers. The pending reply is
                % registered only once the transport has been fully handed off (workers
                % started, or the empty transport completed), so a failure before this
                % point falls through to the catch clause and replies {error, failed}
                % via handle_call instead of a spurious {ok, ID}.
                case TotalFiles of
                    0 ->
                        {{ok, ID}, complete_transport(ID, add_pending_notify(ID, From, State))};
                    _ ->
                        Sup = wa_raft_transport_sup:get_or_start(Peer),
                        [gen_server:cast(Pid, {notify, ID, Table}) || {_Id, Pid, _Type, _Modules} <- supervisor:which_children(Sup), is_pid(Pid)],
                        {{ok, ID}, add_pending_notify(ID, From, State)}
                end;
            {error, receiver_overloaded} ->
                ?RAFT_COUNT(Table, 'transport.rejected.receiver_overloaded'),
                ?RAFT_LOG_WARNING("wa_raft_transport peer ~p rejected transport ~p because of overload", [Peer, ID]),
                {{error, receiver_overloaded}, State};
            {error, receiver_disk_full} ->
                ?RAFT_COUNT(Table, 'transport.rejected.receiver_disk_full'),
                ?RAFT_LOG_WARNING("wa_raft_transport peer ~p rejected transport ~p because of disk pressure", [Peer, ID]),
                {{error, receiver_disk_full}, State};
            Error ->
                ?RAFT_COUNT(Table, 'transport.rejected'),
                ?RAFT_LOG_WARNING("wa_raft_transport peer ~p rejected transport ~p with error ~p", [Peer, ID, Error]),
                {{error, Error}, State}
        end
    catch
        T:E:S ->
            ?RAFT_COUNT(Table, 'transport.start.error'),
            ?RAFT_LOG_WARNING(
                "wa_raft_transport failed to start transport ~0p~n~s",
                [ID, erl_error:format_exception(T, E, S)]
            ),
            {_, NewState} = fail_transport(ID, {start, {T, E, S}}, State),
            {{error, failed}, NewState}
    end.

-spec add_pending_notify(ID :: transport_id(), From :: gen_server:from() | undefined, State :: #state{}) -> #state{}.
add_pending_notify(_ID, undefined, State) ->
    State;
add_pending_notify(ID, From, #state{pending_notify = PendingNotify} = State) ->
    State#state{pending_notify = PendingNotify#{ID => From}}.

-spec transport_module(Meta :: meta()) -> module().
transport_module(#{table := Table, partition := Partition}) ->
    wa_raft_transport:registered_module(Table, Partition);
transport_module(_Meta) ->
    ?RAFT_DEFAULT_TRANSPORT_MODULE.

%% Check whether this node is below its limit for concurrent incoming snapshot
%% transports. Depends only on the witness flag and the active receive counters,
%% never on the transported files, so it can also answer a precheck from a sender
%% that has not created its snapshot yet.
-spec check_capacity(Witness :: boolean(), State :: #state{}) -> ok | {error, receiver_overloaded}.
check_capacity(Witness, #state{global_atomics = GlobalAtomics}) ->
    MaxIncomingSnapshotTransfers = case Witness of
        true  -> ?RAFT_MAX_CONCURRENT_INCOMING_WITNESS_SNAPSHOT_TRANSFERS();
        false -> ?RAFT_MAX_CONCURRENT_INCOMING_SNAPSHOT_TRANSFERS()
    end,
    GlobalActiveIncomingIdx = case Witness of
        true  -> ?GLOBAL_ACTIVE_INCOMING_WITNESS_IDX;
        false -> ?GLOBAL_ACTIVE_INCOMING_IDX
    end,
    NumActiveReceives = atomics:get(GlobalAtomics, GlobalActiveIncomingIdx),
    case NumActiveReceives >= MaxIncomingSnapshotTransfers of
        true  -> {error, receiver_overloaded};
        false -> ok
    end.

%% Ask the transport implementation whether it can accept an incoming transport of
%% IncomingBytes total. Implementations that do not export the optional callback always accept.
-spec transport_accept(Module :: module(), Meta :: meta(), IncomingBytes :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
transport_accept(Module, Meta, IncomingBytes) ->
    case erlang:function_exported(Module, transport_accept, 2) of
        true  -> Module:transport_accept(Meta, IncomingBytes);
        false -> ok
    end.

-spec transport_destination(ID :: transport_id(), Meta :: meta()) -> string().
transport_destination(ID, #{type := transfer, table := Table, partition := Partition}) ->
    filename:join(wa_raft_transport:registered_directory(Table, Partition), integer_to_list(ID));
transport_destination(ID, #{type := snapshot, table := Table, partition := Partition}) ->
    filename:join(wa_raft_transport:registered_directory(Table, Partition), integer_to_list(ID)).

% Resolve every peer-supplied relative path against RootDir using
% filelib:safe_relative_path/2, which rejects absolute paths, ".." escapes,
% and symlink-based escapes above the root. Returning {error, invalid_file_path}
% if any path is unsafe means the whole transport is rejected with no partial
% writes.
-spec resolve_transport_files(
    RootDir :: string(),
    Files :: [{file_id(), RelPath :: string(), Size :: integer()}]
) ->
    {ok, [{file_id(), RelPath :: string(), Path :: string(), Size :: integer()}]}
    | {error, invalid_file_path}.
resolve_transport_files(RootDir, Files) ->
    try
        Resolved = [
            case filelib:safe_relative_path(RelativePath, RootDir) of
                unsafe -> throw(invalid_file_path);
                Safe   -> {FileID, RelativePath, filename:join(RootDir, Safe), Size}
            end
            || {FileID, RelativePath, Size} <- Files
        ],
        {ok, Resolved}
    catch
        throw:invalid_file_path -> {error, invalid_file_path}
    end.

-spec collect_files(string()) -> [{non_neg_integer(), string(), string(), integer(), non_neg_integer()}].
collect_files(Root) ->
    {_, Files} = collect_files_impl(Root, [""],
        fun (Filename, Path, #file_info{size = Size, mtime = MTime}, {FileID, Acc}) ->
            {FileID + 1, [{FileID, filename:flatten(Filename), filename:flatten(Path), MTime, Size} | Acc]}
        end, {1, []}),
    Files.

-spec collect_files_impl(
    string(), list(), fun(), {integer(), [{non_neg_integer(), string(), string(), integer(), non_neg_integer()}]}
) -> {integer(), [{non_neg_integer(), string(), string(), integer(), non_neg_integer()}]}.
collect_files_impl(_Root, [], _Fun, Acc) ->
    Acc;
collect_files_impl(Root, [Filename | Queue], Fun, Acc0) ->
    Path = [Root, $/, Filename],
    case prim_file:read_file_info(Path, [{time, posix}]) of
        {ok, #file_info{type = regular} = Info} ->
            Acc1 = Fun(Filename, Path, Info, Acc0),
            collect_files_impl(Root, Queue, Fun, Acc1);
        {ok, #file_info{type = directory}} ->
            case prim_file:list_dir(Path) of
                {ok, Files} ->
                    NewQueue = lists:foldl(fun (Subfile, Acc) -> [join_names(Filename, Subfile) | Acc] end, Queue, Files),
                    collect_files_impl(Root, NewQueue, Fun, Acc0);
                {error, Reason} ->
                    ?RAFT_LOG_ERROR("wa_raft_transport failed to list files in ~p due to ~p", [filename:flatten(Path), Reason]),
                    throw({list_dir, Reason})
            end;
        {ok, #file_info{type = Type}} ->
            ?RAFT_LOG_WARNING("wa_raft_transport skipping file ~p with unknown type ~p", [filename:flatten(Path), Type]),
            collect_files_impl(Root, Queue, Fun, Acc0);
        {error, Reason} ->
            ?RAFT_LOG_ERROR("wa_raft_transport failed to read info of file ~p due to ~p", [filename:flatten(Path), Reason]),
            throw({read_file_info, Reason})
    end.

-spec join_names(string(), string()) -> string() | [string() | char()].
join_names("", Name) -> Name;
join_names(Dir, Name) -> [Dir, $/, Name].

-spec maybe_notify_complete(ID :: transport_id(), Record :: transport_record(), State :: #state{}) -> ok | {error, term()}.
maybe_notify_complete(ID, #{type := receiver, module := Module} = Record, State) ->
    case erlang:function_exported(Module, transport_complete, 1) of
        true ->
            try Module:transport_complete(ID) of
                ok -> ok
            catch
                C:R:S ->
                    ?RAFT_LOG_NOTICE(
                        "module ~p failed while handing completion for transport ~0p~n~s",
                        [Module, ID, erl_error:format_exception(C, R, S)]
                    )
            end;
        false ->
            ok
    end,
    maybe_notify_complete_impl(ID, Record, State);
maybe_notify_complete(ID, Record, State) ->
    maybe_notify_complete_impl(ID, Record, State).

-spec maybe_notify_complete_impl(ID :: transport_id(), Record :: transport_record(), State :: #state{}) -> ok | {error, term()}.
maybe_notify_complete_impl(_ID, #{type := sender}, _State) ->
    ok;
maybe_notify_complete_impl(ID, #{type := receiver, root := Root, meta := #{type := snapshot, table := Table, partition := Partition, position := LogPos}}, #state{}) ->
    try wa_raft_server:snapshot_available(wa_raft_server:registered_name(Table, Partition), Root, LogPos) of
        ok ->
            ok;
        {error, Reason} ->
            ?RAFT_LOG_NOTICE(
                "wa_raft_transport failed to notify ~p of transport ~p completion due to ~p",
                [wa_raft_server:registered_name(Table, Partition), ID, Reason]
            ),
            {error, Reason}
    catch
        T:E:S ->
            ?RAFT_LOG_NOTICE(
                "wa_raft_transport failed to notify ~p of transport ~p completion due to ~p ~p: ~n~p",
                [wa_raft_server:registered_name(Table, Partition), ID, T, E, S]
            ),
            {error, {T, E, S}}
    end;
maybe_notify_complete_impl(ID, _Info, #state{}) ->
    ?RAFT_LOG_NOTICE("wa_raft_transport finished transport ~p but does not know what to do with it", [ID]).

-spec maybe_notify(ID :: transport_id(), Record :: transport_record(), TransportAtomics :: atomics:atomics_ref(), State :: #state{}) -> #state{}.
maybe_notify(ID, Record, TransportAtomics, #state{pending_notify = PendingNotify} = State) ->
    Table = maps:get(table, maps:get(meta, Record, #{}), undefined),
    Status = decode_status(atomics:get(TransportAtomics, ?TRANSPORT_STATUS_IDX)),
    Start = atomics:get(TransportAtomics, ?TRANSPORT_START_TS_IDX),
    End = atomics:get(TransportAtomics, ?TRANSPORT_END_TS_IDX),
    ?RAFT_COUNT(Table, {'transport', Status}),
    End =/= ?EMPTY_TIMESTAMP andalso ?RAFT_GATHER_LATENCY(Table, {'transport.latency_ms', Status}, End - Start),
    case PendingNotify of
        #{ID := Notify} ->
            gen_server:reply(Notify, {ok, ID}),
            State#state{pending_notify = maps:remove(ID, PendingNotify)};
        _ ->
            State
    end.

-spec scan_transports(State :: #state{}) -> {Inactive :: [transport_id()], NewState :: #state{}}.
scan_transports(State) ->
    scan_transports(ets:first_lookup(?TRANSPORT_TABLE), [], State).

-spec scan_transports(
    {ID :: transport_id(), Rows :: [transport_row()]} | '$end_of_table',
    Acc :: [transport_id()],
    State :: #state{}
) -> {Inactive :: [transport_id()], NewState :: #state{}}.
scan_transports('$end_of_table', Acc, State) ->
    {Acc, State};
scan_transports({ID, [{_, Record, _, TransportAtomics}]}, Acc, State) ->
    {NewAcc, NewState} = case transport_is_running(TransportAtomics) of
        true ->
            NowTs = erlang:system_time(millisecond),
            UpdatedTs = atomics:get(TransportAtomics, ?TRANSPORT_UPDATED_TS_IDX),
            case NowTs - UpdatedTs >= ?RAFT_TRANSPORT_IDLE_TIMEOUT() * 1000 of
                true ->
                    {[ID | Acc], fail_transport_impl(ID, Record, TransportAtomics, timed_out, State)};
                false ->
                    {Acc, State}
            end;
        false ->
            {[ID | Acc], State}
    end,
    scan_transports(ets:next_lookup(?TRANSPORT_TABLE, ID), NewAcc, NewState).

-spec schedule_scan() -> reference().
schedule_scan() ->
    erlang:send_after(?RAFT_TRANSPORT_SCAN_INTERVAL_SECS * 1000, self(), scan).

-spec normalize_status(term()) -> atom().
normalize_status(Status) when is_atom(Status) ->
    Status;
normalize_status({_Error, Reason}) when is_atom(Reason) ->
    Reason;
normalize_status({_Error, Reason}) when is_tuple(Reason) ->
    normalize_status(element(1, Reason));
normalize_status({Error, _Reason}) when is_atom(Error) ->
    Error;
normalize_status(_) ->
    unknown.
