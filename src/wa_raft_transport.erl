%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

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
    start_snapshot_transfer/5,
    start_snapshot_transfer/6,
    transfer_snapshot/6
]).

%% Transport API
-export([
    cancel/2,
    complete/3
]).

%% ETS API
-export([
    setup_tables/0,
    transports/0,
    transport_info/1,
    transport_info/2,
    file_info/2,
    update_file_info/3
]).

%% Internal API - Configuration
-export([
    default_directory/1,
    registered_directory/2,
    registered_module/2
]).

%% Internal API - Transport Workers
-export([
    pop_file/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export_type([
    transport_id/0,
    transport_info/0,
    file_id/0,
    file_info/0
]).

%% Name of the ETS table to keep records for transports
-define(TRANSPORT_TABLE, wa_raft_transport_transports).
%% Name of the ETS table to keep records for files
-define(FILE_TABLE, wa_raft_transport_files).

-define(RAFT_TRANSPORT_PARTITION_SUBDIRECTORY, "transport").

-define(RAFT_TRANSPORT_SCAN_INTERVAL_SECS, 30).

%% Number of counters
-define(RAFT_TRANSPORT_COUNTERS, 1).

%% Counter - inflight receives
-define(RAFT_TRANSPORT_COUNTER_ACTIVE_RECEIVES, 1).

-type transport_id() :: pos_integer().
-type transport_info() :: #{
    type := sender | receiver,
    status := requested | running | completed | cancelled | timed_out | failed,
    atomics := atomics:atomics_ref(),

    peer := atom(),
    module := module(),
    meta := meta(),
    notify => gen_server:from(),

    root := string(),

    start_ts := Millis :: integer(),
    end_ts => Millis :: integer(),

    total_files := non_neg_integer(),
    completed_files := non_neg_integer(),
    queue => ets:table(),

    error => term()
}.

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
    position := wa_raft_log:log_pos()
}.

-type file_id() :: pos_integer().
-type file_info() :: #{
    status := requested | sending | receiving | completed | cancelled | failed,
    atomics := {Transport :: atomics:atomics_ref(), File :: atomics:atomics_ref()},

    name := string(),
    path := string(),
    mtime => integer(),

    start_ts => Millis :: integer(),
    end_ts => Millis :: integer(),
    retries => non_neg_integer(),

    total_bytes := non_neg_integer(),
    completed_bytes := non_neg_integer(),

    meta => map(),
    error => Reason :: term()
}.

%%% ------------------------------------------------------------------------

-record(state, {
    counters :: counters:counters_ref()
}).

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

%% Optional callback for performing any shutdown operations.
-callback transport_terminate(Reason :: term(), State :: term()) -> term().

-optional_callbacks([
    transport_terminate/2
]).

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

-spec start_transport(Peer :: atom(), Meta :: meta(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_transport(Peer, Meta, Root, Timeout) ->
    gen_server:call(?MODULE, {start, Peer, Meta, Root}, Timeout).

-spec start_transport_and_wait(Peer :: atom(), Meta :: meta(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_transport_and_wait(Peer, Meta, Root, Timeout) ->
    gen_server:call(?MODULE, {start_wait, Peer, Meta, Root}, Timeout).

%%% ------------------------------------------------------------------------
%%%  Bulk Transfer API
%%%

-spec start_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), Root :: string()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_transfer(Peer, Table, Partition, Root) ->
    start_transfer(Peer, Table, Partition, Root, 10000).

-spec start_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_transfer(Peer, Table, Partition, Root, Timeout) ->
    start_transport(Peer, #{type => transfer, table => Table, partition => Partition}, Root, Timeout).

-spec transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | wa_raft:error().
transfer(Peer, Table, Partition, Root, Timeout) ->
    start_transport_and_wait(Peer, #{type => transfer, table => Table, partition => Partition}, Root, Timeout).

%%% ------------------------------------------------------------------------
%%%  Snapshot Transfer API
%%%

-spec start_snapshot_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_snapshot_transfer(Peer, Table, Partition, LogPos, Root) ->
    start_snapshot_transfer(Peer, Table, Partition, LogPos, Root, 10000).

-spec start_snapshot_transfer(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_snapshot_transfer(Peer, Table, Partition, LogPos, Root, Timeout) ->
    start_transport(Peer, #{type => snapshot, table => Table, partition => Partition, position => LogPos}, Root, Timeout).

-spec transfer_snapshot(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string(), Timeout :: timeout()) -> {ok, ID :: transport_id()} | wa_raft:error().
transfer_snapshot(Peer, Table, Partition, LogPos, Root, Timeout) ->
    start_transport_and_wait(Peer, #{type => snapshot, table => Table, partition => Partition, position => LogPos}, Root, Timeout).

%%% ------------------------------------------------------------------------
%%%  Transport API
%%%

-spec cancel(ID :: transport_id(), Reason :: term()) -> ok | wa_raft:error().
cancel(ID, Reason) ->
    gen_server:call(?MODULE, {cancel, ID, Reason}).

-spec complete(ID :: transport_id(), FileID :: file_id(), Status :: term()) -> ok.
complete(ID, FileID, Status) ->
    gen_server:cast(?MODULE, {complete, ID, FileID, Status}).

%%% ------------------------------------------------------------------------
%%%  ETS table helper functions
%%%

-spec setup_tables() -> ok.
setup_tables() ->
    ?TRANSPORT_TABLE = ets:new(?TRANSPORT_TABLE, [named_table, set, public]),
    ?FILE_TABLE = ets:new(?FILE_TABLE, [named_table, set, public]),
    ok.

-spec transports() -> [transport_id()].
transports() ->
    ets:select(?TRANSPORT_TABLE, [{{'$1', '_'}, [], ['$1']}]).

-spec transport_info(ID :: transport_id()) -> {ok, Info :: transport_info()} | not_found.
transport_info(ID) ->
    case ets:lookup_element(?TRANSPORT_TABLE, ID, 2, not_found) of
        not_found -> not_found;
        Info      -> {ok, Info}
    end.

-spec transport_info(ID :: transport_id(), Item :: atom()) -> Info :: term() | undefined.
transport_info(ID, Item) ->
    case transport_info(ID) of
        {ok, #{Item := Value}} -> Value;
        _                      -> undefined
    end.

% This function should only be called from the "factory" process since it does not
% provide any atomicity guarantees.
-spec set_transport_info(ID :: transport_id(), Info :: transport_info(), Counters :: counters:counters_ref()) -> term().
set_transport_info(ID, #{atomics := TransportAtomics} = Info, Counters) ->
    true = ets:insert(?TRANSPORT_TABLE, {ID, Info}),
    maybe_update_active_inbound_transport_counts(undefined, Info, Counters),
    ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, erlang:system_time(millisecond)).

% This function should only be called from the "factory" process since it does not
% provide any atomicity guarantees.
-spec update_and_get_transport_info(
    ID :: transport_id(),
    Fun :: fun((Info :: transport_info()) -> NewInfo :: transport_info()),
    Counters :: counters:counters_ref()
) -> {ok, NewOrExistingInfo :: transport_info()} | not_found.
update_and_get_transport_info(ID, Fun, Counters) ->
    case transport_info(ID) of
        {ok, #{atomics := TransportAtomics} = Info} ->
            case Fun(Info) of
                Info ->
                    {ok, Info};
                NewInfo ->
                    true = ets:insert(?TRANSPORT_TABLE, {ID, NewInfo}),
                    ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, erlang:system_time(millisecond)),
                    ok = maybe_update_active_inbound_transport_counts(Info, NewInfo, Counters),
                    {ok, NewInfo}
            end;
        not_found ->
            not_found
    end.

-spec delete_transport_info(ID :: transport_id()) -> ok | not_found.
delete_transport_info(ID) ->
    case transport_info(ID) of
        {ok, #{total_files := TotalFiles} = Info} ->
            lists:foreach(fun (FileID) -> delete_file_info(ID, FileID) end, lists:seq(1, TotalFiles)),
            ets:delete(?TRANSPORT_TABLE, ID),
            Queue = maps:get(queue, Info, undefined),
            Queue =/= undefined andalso catch ets:delete(Queue),
            ok;
        not_found ->
            not_found
    end.

-spec file_info(ID :: transport_id(), FileID :: file_id()) -> {ok, Info :: file_info()} | not_found.
file_info(ID, FileID) ->
    case ets:lookup_element(?FILE_TABLE, {ID, FileID}, 2, not_found) of
        not_found -> not_found;
        Info      -> {ok, Info}
    end.

-spec maybe_update_active_inbound_transport_counts(OldInfo :: transport_info() | undefined, NewInfo :: transport_info(), Counters :: counters:counters_ref()) -> ok.
maybe_update_active_inbound_transport_counts(undefined, #{type := receiver, status := running}, Counters) ->
    counters:add(Counters, ?RAFT_TRANSPORT_COUNTER_ACTIVE_RECEIVES, 1);
maybe_update_active_inbound_transport_counts(#{type := receiver, status := OldStatus}, #{status := running}, Counters) when OldStatus =/= running ->
    counters:add(Counters, ?RAFT_TRANSPORT_COUNTER_ACTIVE_RECEIVES, 1);
maybe_update_active_inbound_transport_counts(#{type := receiver, status := running}, #{status := NewStatus}, Counters) when NewStatus =/= running ->
    counters:sub(Counters, ?RAFT_TRANSPORT_COUNTER_ACTIVE_RECEIVES, 1);
maybe_update_active_inbound_transport_counts(_, _, _) ->
    ok.

% This function should only be called from the "worker" process responsible for the
% transport of the specified file since it does not provide any atomicity guarantees.
-spec set_file_info(ID :: transport_id(), FileID :: file_id(), Info :: file_info()) -> term().
set_file_info(ID, FileID, #{atomics := {TransportAtomics, FileAtomics}} = Info) ->
    true = ets:insert(?FILE_TABLE, {{ID, FileID}, Info}),
    NowMillis = erlang:system_time(millisecond),
    ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis),
    ok = atomics:put(FileAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis).

% This function should only be called from the "worker" process responsible for the
% transport of the specified file since it does not provide any atomicity guarantees.
-spec update_file_info(ID :: transport_id(), FileID :: file_id(), Fun :: fun((Info :: file_info()) -> NewInfo :: file_info())) -> ok | not_found.
update_file_info(ID, FileID, Fun) ->
    case file_info(ID, FileID) of
        {ok, #{atomics := {TransportAtomics, FileAtomics}} = Info} ->
            case Fun(Info) of
                Info ->
                    ok;
                NewInfo ->
                    true = ets:insert(?FILE_TABLE, {{ID, FileID}, NewInfo}),
                    NowMillis = erlang:system_time(millisecond),
                    ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis),
                    ok = atomics:put(FileAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis),
                    ok
            end;
        not_found ->
            not_found
    end.

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

-spec pop_file(ID :: transport_id()) -> {ok, FileID :: file_id()} | empty | not_found.
pop_file(ID) ->
    case transport_info(ID) of
        {ok, #{queue := Queue}} -> try_pop_file(Queue);
        _Other                  -> not_found
    end.

-spec try_pop_file(Queue :: ets:table()) -> {ok, FileID :: file_id()} | empty | not_found.
try_pop_file(Queue) ->
    try ets:first(Queue) of
        '$end_of_table' ->
            empty;
        FileID ->
            try ets:select_delete(Queue, [{{FileID}, [], [true]}]) of
                0 -> try_pop_file(Queue);
                1 -> {ok, FileID}
            catch
                error:badarg -> not_found
            end
    catch
        error:badarg -> not_found
    end.

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init(Args :: term()) -> {ok, State :: #state{}}.
init(_) ->
    process_flag(trap_exit, true),
    Counters = counters:new(?RAFT_TRANSPORT_COUNTERS, [atomics]),
    schedule_scan(),
    {ok, #state{counters = Counters}}.

-spec handle_call(Request, From :: gen_server:from(), State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} | {noreply, NewState :: #state{}}
    when
        Request ::
            {start, Peer :: node(), Meta :: meta(), Root :: string()} |
            {start_wait, Peer :: node(), Meta :: meta(), Root :: string()} |
            {transport, ID :: transport_id(), Peer :: node(), Module :: module(), Meta :: meta(), Files :: [{file_id(), RelPath :: string(), Size :: integer()}]} |
            {cancel, ID :: transport_id(), Reason :: term()}.
handle_call({start, Peer, Meta, Root}, _From, #state{counters = Counters} = State) ->
    {reply, handle_transport_start(undefined, Peer, Meta, Root, Counters), State};
handle_call({start_wait, Peer, Meta, Root}, From, #state{counters = Counters} = State) ->
    case handle_transport_start(From, Peer, Meta, Root, Counters) of
        {ok, _ID}       -> {noreply, State};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;
handle_call({transport, ID, Peer, Module, Meta, Files}, From, #state{counters = Counters} = State) ->
    try
        MaxIncomingSnapshotTransfers = ?RAFT_MAX_CONCURRENT_INCOMING_SNAPSHOT_TRANSFERS(),
        case {transport_info(ID), counters:get(Counters, ?RAFT_TRANSPORT_COUNTER_ACTIVE_RECEIVES)} of
            {{ok, _Info}, _} ->
                ?LOG_WARNING("wa_raft_transport got duplicate transport receive start for ~p from ~p",
                    [ID, From], #{domain => [whatsapp, wa_raft]}),
                {reply, duplicate, State};
            {not_found, NumActiveReceives} when NumActiveReceives >= MaxIncomingSnapshotTransfers ->
                {reply, {error, receiver_overloaded}, State};
            {not_found, _} ->
                ?RAFT_COUNT('raft.transport.receive'),
                ?LOG_NOTICE("wa_raft_transport starting transport receive for ~p",
                    [ID], #{domain => [whatsapp, wa_raft]}),

                TransportAtomics = atomics:new(?RAFT_TRANSPORT_TRANSPORT_ATOMICS_COUNT, []),
                RootDir = transport_destination(ID, Meta),
                NowMillis = erlang:system_time(millisecond),
                TotalFiles = length(Files),

                % Force the receiving directory to always exist
                catch filelib:ensure_dir([RootDir, $/]),

                % Setup overall transport info
                set_transport_info(ID, #{
                    type => receiver,
                    status => running,
                    atomics => TransportAtomics,
                    peer => Peer,
                    module => Module,
                    meta => Meta,
                    root => RootDir,
                    start_ts => NowMillis,
                    total_files => TotalFiles,
                    completed_files => 0
                }, Counters),

                % Setup file info for each file
                [
                    begin
                        FileAtomics = atomics:new(?RAFT_TRANSPORT_FILE_ATOMICS_COUNT, []),
                        set_file_info(ID, FileID, #{
                            status => requested,
                            atomics => {TransportAtomics, FileAtomics},
                            name => RelativePath,
                            path => filename:join(RootDir, RelativePath),
                            total_bytes => Size,
                            completed_bytes => 0
                        })
                    end || {FileID, RelativePath, Size} <- Files
                ],

                % If the transport is empty, then immediately complete it
                TotalFiles =:= 0 andalso
                    update_and_get_transport_info(
                        ID,
                        fun (Info0) ->
                            Info1 = Info0#{status => completed, end_ts => NowMillis},
                            Info2 = case maybe_notify_complete(ID, Info1, State) of
                                ok              -> Info1;
                                {error, Reason} -> Info1#{status => failed, error => {notify_failed, Reason}}
                            end,
                            maybe_notify(ID, Info2)
                        end,
                        Counters
                    ),

                {reply, ok, State}
        end
    catch
        T:E:S ->
            ?RAFT_COUNT('raft.transport.receive.error'),
            ?LOG_WARNING("wa_raft_transport failed to accept transport ~p due to ~p ~p: ~n~p",
                [ID, T, E, S], #{domain => [whatsapp, wa_raft]}),
            update_and_get_transport_info(
                ID,
                fun (Info) ->
                    Info#{
                        status => failed,
                        end_ts => erlang:system_time(millisecond),
                        error => {receive_failed, {T, E, S}}
                    }
                end,
                Counters
            ),
            {reply, {error, failed}, State}
    end;
handle_call({cancel, ID, Reason}, _From, #state{counters = Counters} = State) ->
    ?LOG_NOTICE("wa_raft_transport got cancellation request for ~p for reason ~p",
        [ID, Reason], #{domain => [whatsapp, wa_raft]}),
    Reply =
        case
            update_and_get_transport_info(
                ID,
                fun
                    (#{status := running} = Info) ->
                        NowMillis = erlang:system_time(millisecond),
                        Info#{status => cancelled, end_ts => NowMillis, error => {cancelled, Reason}};
                    (Info) ->
                        Info
                end,
                Counters
            )
        of
            {ok, _Info} -> ok;
            not_found   -> {error, not_found}
        end,
    {reply, Reply, State};
handle_call(Request, _From, #state{} = State) ->
    ?LOG_WARNING("wa_raft_transport received unrecognized factory call ~p",
        [Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request, State :: #state{}) -> {noreply, NewState :: #state{}}
    when Request :: {complete, ID :: transport_id(), FileID :: file_id(), Status :: term()}.
handle_cast({complete, ID, FileID, Status}, #state{counters = Counters} = State) ->
    NowMillis = erlang:system_time(millisecond),
    ?RAFT_COUNT({'raft.transport.file.send', normalize_status(Status)}),
    Result0 = update_file_info(ID, FileID,
        fun (Info) ->
            case Info of
                #{start_ts := StartMillis} ->
                    ?RAFT_GATHER_LATENCY({'raft.transport.file.send', Status, latency_ms}, NowMillis - StartMillis);
                _ ->
                    ok
            end,
            case Status of
                ok -> Info#{status => completed, end_ts => NowMillis};
                _  -> Info#{status => failed, end_ts => NowMillis, error => Status}
            end
        end),
    Result0 =:= not_found andalso
        ?LOG_WARNING("wa_raft_transport got complete report for unknown file ~p:~p",
            [ID, FileID], #{domain => [whatsapp, wa_raft]}),
    Result1 =
        update_and_get_transport_info(
            ID,
            fun
                (#{status := running, completed_files := CompletedFiles, total_files := TotalFiles} = Info0) ->
                    Info1 = Info0#{completed_files => CompletedFiles + 1},
                    Info2 = case CompletedFiles + 1 of
                        TotalFiles -> Info1#{status => completed, end_ts => NowMillis};
                        _          -> Info1
                    end,
                    Info3 = case Status of
                        ok -> Info2;
                        _  -> Info2#{status => failed, end_ts => NowMillis, error => {file, FileID, Status}}
                    end,
                    Info4 = case maybe_notify_complete(ID, Info3, State) of
                        ok              -> Info3;
                        {error, Reason} -> Info3#{status => failed, error => {notify_failed, Reason}}
                    end,
                    maybe_notify(ID, Info4);
                (Info) ->
                    Info
            end,
            Counters
        ),
    Result1 =:= not_found andalso
        ?LOG_WARNING("wa_raft_transport got complete report for unknown transfer ~p",
            [ID], #{domain => [whatsapp, wa_raft]}),
    {noreply, State};
handle_cast(Request, State) ->
    ?LOG_NOTICE("wa_raft_transport got unrecognized cast ~p", [Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(Info :: term(), State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_info(scan, #state{counters = Counters} = State) ->
    InactiveTransports =
        lists:filter(
            fun (ID) ->
                case update_and_get_transport_info(ID, fun (Info) -> scan_transport(ID, Info) end, Counters) of
                    {ok, #{status := Status}} -> Status =/= requested andalso Status =/= running;
                    not_found                 -> false
                end
            end, transports()),
    ExcessTransports = length(InactiveTransports) - ?RAFT_TRANSPORT_INACTIVE_INFO_LIMIT(),
    ExcessTransports > 0 andalso begin
        ExcessTransportIDs = lists:sublist(lists:sort(InactiveTransports), ExcessTransports),
        lists:foreach(fun delete_transport_info/1, ExcessTransportIDs)
    end,

    schedule_scan(),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_NOTICE("wa_raft_transport got unrecognized info ~p", [Info], #{domain => [whatsapp, wa_raft]}),
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

-spec handle_transport_start(From :: gen_server:from() | undefined, Peer :: node(), Meta :: meta(), Root :: string(), Counters :: counters:counters_ref()) -> {ok, ID :: transport_id()} | wa_raft:error().
handle_transport_start(From, Peer, Meta, Root, Counters) ->
    ID = make_id(),

    ?RAFT_COUNT('raft.transport.start'),
    ?LOG_NOTICE("wa_raft_transport starting transport ~p of ~p to ~p with metadata ~p",
        [ID, Root, Peer, Meta], #{domain => [whatsapp, wa_raft]}),

    try
        Files = collect_files(Root),
        TransportAtomics = atomics:new(?RAFT_TRANSPORT_TRANSPORT_ATOMICS_COUNT, []),
        Module = transport_module(Meta),
        TotalFiles = length(Files),
        NowMillis = erlang:system_time(millisecond),
        Queue = ets:new(?MODULE, [ordered_set, public]),

        % Setup overall transport info
        set_transport_info(ID, #{
            type => sender,
            status => requested,
            atomics => TransportAtomics,
            peer => Peer,
            module => Module,
            meta => Meta,
            root => Root,
            start_ts => NowMillis,
            total_files => TotalFiles,
            completed_files => 0,
            queue => Queue
        }, Counters),

        % Setup file info for each file
        [
            begin
                FileAtomics = atomics:new(?RAFT_TRANSPORT_FILE_ATOMICS_COUNT, []),
                set_file_info(ID, FileID, #{
                    status => requested,
                    atomics => {TransportAtomics, FileAtomics},
                    name => Filename,
                    path => Path,
                    mtime => MTime,
                    total_bytes => Size,
                    completed_bytes => 0
                })
            end || {FileID, Filename, Path, MTime, Size} <- Files
        ],

        % Notify peer node of incoming transport
        FileData = [{FileID, Filename, Size} || {FileID, Filename, _, _, Size} <- Files],
        case gen_server:call({?MODULE, Peer}, {transport, ID, node(), Module, Meta, FileData}, ?RAFT_RPC_CALL_TIMEOUT()) of
            ok ->
                % Add all files to the queue
                ets:insert(Queue, [{FileID} || {FileID, _, _, _, _} <- Files]),

                % Start workers
                update_and_get_transport_info(
                    ID,
                    fun (Info0) ->
                        Info1 = case From of
                            undefined -> Info0;
                            _         -> Info0#{notify => From}
                        end,
                        case TotalFiles of
                            0 ->
                                Info2 = Info1#{status => completed, end_ts => NowMillis},
                                maybe_notify(ID, Info2);
                            _ ->
                                Sup = wa_raft_transport_sup:get_or_start(Peer),
                                [gen_server:cast(Pid, {notify, ID}) || {_Id, Pid, _Type, _Modules} <- supervisor:which_children(Sup), is_pid(Pid)],
                                Info1#{status => running}
                        end
                    end,
                    Counters
                ),
                {ok, ID};
            {error, receiver_overloaded} ->
                ?RAFT_COUNT('raft.transport.rejected.receiver_overloaded'),
                ?LOG_WARNING("wa_raft_transport peer ~p rejected transport ~p because of overload",
                    [Peer, ID], #{domain => [whatsapp, wa_raft]}),
                update_and_get_transport_info(
                    ID,
                    fun (Info) ->
                        Info#{
                            status => failed,
                            end_ts => NowMillis,
                            error => {rejected, receiver_overloaded}
                        }
                    end,
                    Counters
                ),
                {error, receiver_overloaded};
            Error ->
                ?RAFT_COUNT('raft.transport.rejected'),
                ?LOG_WARNING("wa_raft_transport peer ~p rejected transport ~p with error ~p",
                    [Peer, ID, Error], #{domain => [whatsapp, wa_raft]}),
                    update_and_get_transport_info(
                    ID,
                    fun (Info) ->
                        Info#{
                            status => failed,
                            end_ts => NowMillis,
                            error => {rejected, Error}
                        }
                    end,
                    Counters
                ),
                {error, Error}
        end
    catch
        T:E:S ->
            ?RAFT_COUNT('raft.transport.start.error'),
            ?LOG_WARNING("wa_raft_transport failed to start transport ~p due to ~p ~p: ~n~p",
                [ID, T, E, S], #{domain => [whatsapp, wa_raft]}),
            update_and_get_transport_info(
                ID,
                fun (Info) ->
                    Info#{
                        status => failed,
                        end_ts => erlang:system_time(millisecond),
                        error => {start, {T, E, S}}
                    }
                end,
                Counters
            ),
            {error, failed}
    end.

-spec transport_module(Meta :: meta()) -> module().
transport_module(#{table := Table, partition := Partition}) ->
    wa_raft_transport:registered_module(Table, Partition);
transport_module(_Meta) ->
    ?RAFT_DEFAULT_TRANSPORT_MODULE.

-spec transport_destination(ID :: transport_id(), Meta :: meta()) -> string().
transport_destination(ID, #{type := transfer, table := Table, partition := Partition}) ->
    filename:join(wa_raft_transport:registered_directory(Table, Partition), integer_to_list(ID));
transport_destination(ID, #{type := snapshot, table := Table, partition := Partition}) ->
    filename:join(wa_raft_transport:registered_directory(Table, Partition), integer_to_list(ID));
transport_destination(ID, Meta) ->
    ?LOG_WARNING("wa_raft_transport cannot determine transport destination for transport ~0p with metadata ~0p",
        [ID, Meta], #{domain => [whatsapp, wa_raft]}),
    error(no_known_destination).

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
                    ?LOG_ERROR("wa_raft_transport failed to list files in ~p due to ~p",
                        [filename:flatten(Path), Reason], #{domain => [whatsapp, wa_raft]}),
                    throw({list_dir, Reason})
            end;
        {ok, #file_info{type = Type}} ->
            ?LOG_WARNING("wa_raft_transport skipping file ~p with unknown type ~p",
                [filename:flatten(Path), Type], #{domain => [whatsapp, wa_raft]}),
            collect_files_impl(Root, Queue, Fun, Acc0);
        {error, Reason} ->
            ?LOG_ERROR("wa_raft_transport failed to read info of file ~p due to ~p",
                [filename:flatten(Path), Reason], #{domain => [whatsapp, wa_raft]}),
            throw({read_file_info, Reason})
    end.

-spec join_names(string(), string()) -> list().
join_names("", Name) -> Name;
join_names(Dir, Name) -> [Dir, $/, Name].

-spec maybe_notify_complete(transport_id(), transport_info(), #state{}) -> ok | {error, term()}.
maybe_notify_complete(_ID, #{type := sender}, _State) ->
    ok;
maybe_notify_complete(_ID, #{status := Status}, _State) when Status =/= completed ->
    ok;
maybe_notify_complete(ID, #{type := receiver, root := Root, meta := #{type := snapshot, table := Table, partition := Partition, position := LogPos}}, #state{}) ->
    try wa_raft_server:snapshot_available(wa_raft_server:registered_name(Table, Partition), Root, LogPos) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_NOTICE("wa_raft_transport failed to notify ~p of transport ~p completion due to ~p",
                [wa_raft_server:registered_name(Table, Partition), ID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    catch
        T:E:S ->
            ?LOG_NOTICE("wa_raft_transport failed to notify ~p of transport ~p completion due to ~p ~p: ~n~p",
                [wa_raft_server:registered_name(Table, Partition), ID, T, E, S], #{domain => [whatsapp, wa_raft]}),
            {error, {T, E, S}}
    end;
maybe_notify_complete(ID, _Info, #state{}) ->
    ?LOG_NOTICE("wa_raft_transport finished transport ~p but does not know what to do with it",
        [ID], #{domain => [whatsapp, wa_raft]}).

-spec maybe_notify(transport_id(), transport_info()) -> transport_info().
maybe_notify(ID, #{status := Status, notify := Notify, start_ts := Start, end_ts := End} = Info) when Status =/= requested andalso Status =/= running ->
    ?RAFT_COUNT({'raft.transport', Status}),
    ?RAFT_GATHER_LATENCY({'raft.transport', Status, latency_ms}, End - Start),
    gen_server:reply(Notify, {ok, ID}),
    maps:remove(notify, Info);
maybe_notify(_ID, Info) ->
    Info.

-spec scan_transport(ID :: transport_id(), Info :: transport_info()) -> NewInfo :: transport_info().
scan_transport(ID, #{status := running, atomics := TransportAtomics} = Info) ->
    LastUpdateTs = atomics:get(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS),
    NowMillis = erlang:system_time(millisecond),
    case NowMillis - LastUpdateTs >= ?RAFT_TRANSPORT_IDLE_TIMEOUT() * 1000 of
        true  -> maybe_notify(ID, Info#{status := timed_out, end_ts => NowMillis});
        false -> Info
    end;
scan_transport(_ID, Info) ->
    Info.

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
