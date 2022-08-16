%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport).
-compile(warn_missing_spec).
-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% OTP supervision
-export([
    child_spec/0,
    start_link/0
]).

%% Transport API
-export([
    start/3,
    start_snapshot/5,
    transport/4,
    transport_snapshot/6,
    cancel/2,
    complete/3,
    complete/4,
    pin/1,
    unpin/1
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

-define(RAFT_TRANSPORT_DIRNAME, "transport").

-define(RAFT_TRANSPORT_MODULE(), application:get_env(?APP, transport_module, wa_raft_dist_transport)).

-define(RAFT_TRANSPORT_SCAN_INTERVAL_SECS, 30).
-define(RAFT_TRANSPORT_MAX_IDLE_SECS(), application:get_env(?APP, transport_idle_timeout_secs, 30)).

-define(RAFT_TRANSPORT_RETAIN_MIN_SECS(), application:get_env(?APP, transport_retain_min_secs, 300)).

-define(INFO_KEY(ID), {ID, info}).
-define(FILE_KEY(ID, FileID), {ID, {file, FileID}}).

-type transport_id() :: pos_integer().
-type transport_info() :: #{
    type := sender | receiver,
    status := requested | running | completed | cancelled | timed_out | failed,
    atomics := atomics:atomics_ref(),

    peer := atom(),
    module := module(),
    meta := term(),
    notify => term(),

    root := string(),

    start_ts := Millis :: integer(),
    end_ts => Millis :: integer(),

    total_files := non_neg_integer(),
    next_file => non_neg_integer(),
    completed_files := non_neg_integer(),

    error => term()
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

    total_bytes := non_neg_integer(),
    completed_bytes := non_neg_integer(),

    meta => map(),
    error => Reason :: term()
}.

-record(state, {
    pins = #{} :: #{transport_id() => non_neg_integer()},
    monitors = #{} :: #{reference() => transport_id()}
}).
-type state() :: #state{}.

%%% ------------------------------------------------------------------------
%%%  Behaviour callbacks
%%%

%% Perform any setup required before transport can be started.
-callback transport_init() -> {ok, State :: term()} | {stop, Reason :: term()}.

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
%%%  Transport API
%%%

-spec start(Peer :: atom(), Meta :: term(), Root :: string()) -> {ok, ID :: transport_id()} | wa_raft:error().
start(Peer, Meta, Root) ->
    gen_server:call(?MODULE, {start, Peer, Meta, Root}, 10000).

-spec start_snapshot(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_snapshot(Peer, Table, Partition, LogPos, Root) ->
    start(Peer, #{type => snapshot, table => Table, partition => Partition, position => LogPos}, Root).

-spec transport(Peer :: atom(), Meta :: map(), Root :: string(), Timeout :: non_neg_integer()) -> {ok, ID :: transport_id()} | wa_raft:error().
transport(Peer, Meta, Root, Timeout) ->
    gen_server:call(?MODULE, {start_wait, Peer, Meta, Root}, Timeout).

-spec transport_snapshot(Peer :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition(), LogPos :: wa_raft_log:log_pos(), Root :: string(), Timeout :: non_neg_integer()) -> {ok, ID :: transport_id()} | wa_raft:error().
transport_snapshot(Peer, Table, Partition, LogPos, Root, Timeout) ->
    transport(Peer, #{type => snapshot, table => Table, partition => Partition, position => LogPos}, Root, Timeout).

-spec cancel(ID :: transport_id(), Reason :: term()) -> ok | wa_raft:error().
cancel(ID, Reason) ->
    gen_server:call(?MODULE, {cancel, ID, Reason}).

-spec complete(ID :: transport_id(), FileID :: file_id(), Status :: term()) -> ok | invalid.
complete(ID, FileID, Status) ->
    complete(ID, FileID, Status, undefined).

-spec complete(ID :: transport_id(), FileID :: file_id(), Status :: term(), Pid :: pid() | undefined) -> ok | invalid.
complete(ID, FileID, Status, Pid) ->
    gen_server:cast(?MODULE, {complete, ID, FileID, Status, Pid}).

-spec pin(ID :: transport_id()) -> {ok, Ref :: reference()} | wa_raft:error().
pin(ID) ->
    gen_server:call(?MODULE, {pin, ID}).

-spec unpin(Ref :: reference()) -> ok | wa_raft:error().
unpin(Ref) ->
    gen_server:call(?MODULE, {unpin, Ref}).

%%% ------------------------------------------------------------------------
%%%  ETS table helper functions
%%%

-spec setup_tables() -> ok.
setup_tables() ->
    ?MODULE = ets:new(?MODULE, [named_table, set, public]),
    ok.

-spec transports() -> [transport_id()].
transports() ->
    ets:select(?MODULE, [{{?INFO_KEY('$1'), '_'}, [], ['$1']}]).

-spec transport_info(ID :: transport_id()) -> {ok, Info :: transport_info()} | not_found.
transport_info(ID) ->
    case ets:lookup(?MODULE, ?INFO_KEY(ID)) of
        [{_, Info}] -> {ok, Info};
        []          -> not_found
    end.

-spec transport_info(ID :: transport_id(), Item :: atom()) -> Info :: term() | undefined.
transport_info(ID, Item) ->
    case transport_info(ID) of
        {ok, #{Item := Value}} -> Value;
        _                      -> undefined
    end.

% This function should only be called from the "factory" process since it does not
% provide any atomicity guarantees.
-spec set_transport_info(ID :: transport_id(), Info :: transport_info()) -> term().
set_transport_info(ID, #{atomics := TransportAtomics} = Info) ->
    true = ets:insert(?MODULE, {?INFO_KEY(ID), Info}),
    ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, erlang:system_time(millisecond)).

% This function should only be called from the "factory" process since it does not
% provide any atomicity guarantees.
-spec update_transport_info(ID :: transport_id(), Fun :: fun((Info :: transport_info()) -> NewInfo :: transport_info())) -> ok | not_found.
update_transport_info(ID, Fun) ->
    case transport_info(ID) of
        {ok, #{atomics := TransportAtomics} = Info} ->
            true = ets:insert(?MODULE, {?INFO_KEY(ID), Fun(Info)}),
            ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, erlang:system_time(millisecond)),
            ok;
        not_found ->
            not_found
    end.

-spec file_info(ID :: transport_id(), FileID :: file_id()) -> {ok, Info :: file_info()} | not_found.
file_info(ID, FileID) ->
    case ets:lookup(?MODULE, ?FILE_KEY(ID, FileID)) of
        [{_, Info}] -> {ok, Info};
        []          -> not_found
    end.

% This function should only be called from the "worker" process responsible for the
% transport of the specified file since it does not provide any atomicity guarantees.
-spec set_file_info(ID :: transport_id(), FileID :: file_id(), Info :: file_info()) -> term().
set_file_info(ID, FileID, #{atomics := {TransportAtomics, FileAtomics}} = Info) ->
    true = ets:insert(?MODULE, {?FILE_KEY(ID, FileID), Info}),
    NowMillis = erlang:system_time(millisecond),
    ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis),
    ok = atomics:put(FileAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis).

% This function should only be called from the "worker" process responsible for the
% transport of the specified file since it does not provide any atomicity guarantees.
-spec update_file_info(ID :: transport_id(), FileID :: file_id(), Fun :: fun((Info :: file_info()) -> NewInfo :: file_info())) -> ok | not_found.
update_file_info(ID, FileID, Fun) ->
    case file_info(ID, FileID) of
        {ok, #{atomics := {TransportAtomics, FileAtomics}} = Info} ->
            true = ets:insert(?MODULE, {?FILE_KEY(ID, FileID), Fun(Info)}),
            NowMillis = erlang:system_time(millisecond),
            ok = atomics:put(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis),
            ok = atomics:put(FileAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS, NowMillis),
            ok;
        not_found ->
            not_found
    end.

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init(Args :: term()) -> {ok, State :: state()}.
init(_) ->
    process_flag(trap_exit, true),
    schedule_scan(),
    {ok, #state{}}.

-spec handle_call(Request :: term(), From :: {Pid :: pid(), Tag :: term()}, State :: state()) -> {reply, Reply :: term(), NewState :: state()} | {noreply, NewState :: state()}.
handle_call({start, Peer, Meta, Root}, _From, #state{} = State) ->
    {reply, start_transport(undefined, Peer, Meta, Root), State};
handle_call({start_wait, Peer, Meta, Root}, From, #state{} = State) ->
    case start_transport(From, Peer, Meta, Root) of
        {ok, _ID}       -> {noreply, State};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;
handle_call({transport, ID, Peer, Module, Meta, Files}, From, #state{} = State) ->
    try
        case transport_info(ID) of
            {ok, _Info} ->
                ?LOG_WARNING("wa_raft_transport got duplicate transport receive start for ~p from ~p",
                    [ID, From], #{domain => [whatsapp, wa_raft]}),
                {reply, duplicate, State};
            not_found ->
                ?RAFT_COUNT('raft.transport.receive'),
                ?LOG_NOTICE("wa_raft_transport starting transport receive for ~p",
                    [ID], #{domain => [whatsapp, wa_raft]}),

                TransportAtomics = atomics:new(?RAFT_TRANSPORT_TRANSPORT_ATOMICS_COUNT, []),
                RootDir = transport_destination(ID),
                NowMillis = erlang:system_time(millisecond),
                TotalFiles = length(Files),

                % Force the receiving directory to always exist
                catch filelib:ensure_dir([RootDir, $/]),

                % Initialize info in ETS about transport and contained files.
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
                }),
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

                TotalFiles =:= 0 andalso
                    update_transport_info(ID, fun (Info0) ->
                        Info1 = Info0#{status => completed, end_ts => NowMillis},
                        Info2 = case maybe_notify_complete(ID, Info1, State) of
                            ok              -> Info1;
                            {error, Reason} -> Info1#{status => failed, error => {notify_failed, Reason}}
                        end,
                        maybe_notify(ID, Info2)
                    end),

                {reply, ok, State}
        end
    catch
        T:E:S ->
            ?RAFT_COUNT('raft.transport.receive.error'),
            ?LOG_WARNING("wa_raft_transport failed to accept transport ~p due to ~p ~p: ~n~p",
                [ID, T, E, S], #{domain => [whatsapp, wa_raft]}),
            update_transport_info(ID, fun (Info) -> Info#{status => failed, end_ts => erlang:system_time(millisecond), error => {receive_failed, {T, E, S}}} end),
            {reply, {error, failed}, State}
    end;
handle_call({cancel, ID, Reason}, _From, #state{} = State) ->
    ?LOG_NOTICE("wa_raft_transport got cancellation request for ~p for reason ~p",
        [ID, Reason], #{domain => [whatsapp, wa_raft]}),
    Result =
        update_transport_info(ID,
            fun
                (#{status := running} = Info) ->
                    NowMillis = erlang:system_time(millisecond),
                    Info#{status => cancelled, end_ts => NowMillis, error => {cancelled, Reason}};
                (Info) ->
                    Info
            end),
    Reply = case Result of
        ok -> ok;
        not_found -> {error, not_found}
    end,
    {reply, Reply, State};
handle_call({pin, ID}, {Pid, _Tag}, #state{pins = Pins0, monitors = Monitors0} = State0) ->
    Ref = monitor(process, Pid),
    Pins1 = maps:update_with(ID, fun (V) -> V + 1 end, 1, Pins0),
    Monitors1 = Monitors0#{Ref => ID},
    {reply, {ok, Ref}, State0#state{pins = Pins1, monitors = Monitors1}};
handle_call({unpin, Ref}, _, #state{pins = Pins0, monitors = Monitors0} = State0) ->
    case Monitors0 of
        #{Ref := ID} ->
            demonitor(Ref),
            Pins1 = maps:update_with(ID, fun (V) -> V - 1 end, Pins0),
            Monitors1 = maps:remove(Ref, Monitors0),
            {reply, ok, State0#state{pins = Pins1, monitors = Monitors1}};
        _ ->
            {reply, {error, badarg}, State0}
    end;
handle_call(Request, _From, #state{} = State) ->
    ?LOG_WARNING("wa_raft_transport received unrecognized factory call ~p",
        [Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request :: term(), State :: state()) -> {noreply, NewState :: state()}.
handle_cast({complete, ID, FileID, Status, Pid}, #state{} = State) ->
    ?RAFT_COUNT('raft.transport.file.complete'),
    NowMillis = erlang:system_time(millisecond),
    Result0 = update_file_info(ID, FileID,
        fun (Info) ->
            case Status of
                ok -> Info#{status => completed, end_ts => NowMillis};
                _  -> Info#{status => failed, end_ts => NowMillis, error => Status}
            end
        end),
    Result0 =:= not_found andalso
        ?LOG_WARNING("wa_raft_transport got complete report for unknown file ~p:~p",
            [ID, FileID], #{domain => [whatsapp, wa_raft]}),
    Result1 = update_transport_info(ID,
        fun
            (#{status := running, type := Type, completed_files := CompletedFiles, total_files := TotalFiles} = Info0) ->
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
                Info5 = case Type of
                    sender -> maybe_submit_one(ID, Info4, Pid);
                    _      -> Info4
                end,
                maybe_notify(ID, Info5);
            (Info) ->
                Info
        end),
    Result1 =:= not_found andalso
        ?LOG_WARNING("wa_raft_transport got complete report for unknown transfer ~p",
            [ID], #{domain => [whatsapp, wa_raft]}),
    {noreply, State};
handle_cast({cleanup, ID}, #state{} = State) ->
    ?RAFT_COUNT('raft.transport.cleanup'),
    RootDir = transport_destination(ID),
    RenameDir = [RootDir, ".delete"],
    case prim_file:rename(RootDir, RenameDir) of
        ok ->
            case file:del_dir_r(RenameDir) of
                ok ->
                    {noreply, State};
                {error, Reason} ->
                    ?LOG_WARNING("wa_raft_transport failed to cleanup transport ~p due to ~p",
                        [ID, Reason], #{domain => [whatsapp, wa_raft]}),
                    {noreply, State}
            end;
        {error, enoent} ->
            {noreply, State};
        {error, Reason} ->
            ?LOG_WARNING("wa_raft_transport failed to rename transport ~p for cleanup due to ~p",
                [ID, Reason], #{domain => [whatsapp, wa_raft]}),
            {noreply, State}
    end;
handle_cast(Request, State) ->
    ?LOG_NOTICE("wa_raft_transport got unrecognized cast ~p", [Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(Info :: term(), State :: state()) -> {noreply, NewState :: state()}.
handle_info(scan, State) ->
    lists:foreach(
        fun (ID) ->
            update_transport_info(ID, fun (Info) -> scan_transport(ID, Info) end)
        end, transports()),
    maybe_cleanup(State),
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

-spec start_transport(From :: term() | undefined, Peer :: node(), Meta :: term(), Root :: string()) -> {ok, ID :: transport_id()} | wa_raft:error().
start_transport(From, Peer, Meta, Root) ->
    ID = make_id(),

    ?RAFT_COUNT('raft.transport.start'),
    ?LOG_NOTICE("wa_raft_transport starting transport ~p of ~p to ~p with metadata ~p",
        [ID, Root, Peer, Meta], #{domain => [whatsapp, wa_raft]}),

    try
        Files = collect_files(Root),
        TransportAtomics = atomics:new(?RAFT_TRANSPORT_TRANSPORT_ATOMICS_COUNT, []),
        Module = ?RAFT_TRANSPORT_MODULE(),
        TotalFiles = length(Files),
        NowMillis = erlang:system_time(millisecond),

        % Initialize info in ETS about transport and contained files.
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
            completed_files => 0
        }),
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
        case gen_server:call({?MODULE, Peer}, {transport, ID, node(), Module, Meta, FileData}) of
            ok ->
                update_transport_info(ID,
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
                                Info2 = Info1#{status => running, next_file => 1},
                                Workers = [Pid || {_Id, Pid, _Type, _Modules} <- supervisor:which_children(wa_raft_transport_sup)],
                                lists:foldl(fun (Pid, InfoN) -> maybe_submit_one(ID, InfoN, Pid) end, Info2, Workers)
                        end
                end),
                {ok, ID};
            Error ->
                ?RAFT_COUNT('raft.transport.rejected'),
                ?LOG_WARNING("wa_raft_transport peer ~p rejected transport ~p with error ~p",
                    [Peer, ID, Error], #{domain => [whatsapp, wa_raft]}),
                update_transport_info(ID, fun (Info) -> Info#{status => failed, end_ts => NowMillis, error => {rejected, Error}} end),
                {error, rejected}
        end
    catch
        T:E:S ->
            ?RAFT_COUNT('raft.transport.start.error'),
            ?LOG_WARNING("wa_raft_transport failed to start transport ~p due to ~p ~p: ~n~p",
                [ID, T, E, S], #{domain => [whatsapp, wa_raft]}),
            update_transport_info(ID, fun (Info) -> Info#{status => failed, end_ts => erlang:system_time(millisecond), error => {start, {T, E, S}}} end),
            {error, failed}
    end.

-spec transport_destination(non_neg_integer()) -> file:filename_all().
transport_destination(ID) ->
    filename:join(?DB, [?RAFT_TRANSPORT_DIRNAME, $/, integer_to_list(ID)]).

-spec collect_files(string()) -> [{non_neg_integer(), string(), string(), integer(), non_neg_integer()}].
collect_files(Root) ->
    {_, Files} = collect_files_impl(Root, [""],
        fun (Filename, Path, #file_info{size = Size, mtime = MTime}, {FileID, Acc}) ->
            {FileID + 1, [{FileID, filename:flatten(Filename), filename:flatten(Path), MTime, Size} | Acc]}
        end, {1, []}),
    Files.

collect_files_impl(__Root, [], _Fun, Acc) ->
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

-spec maybe_cleanup(state()) -> ok | {error, term()}.
maybe_cleanup(#state{pins = Pins}) ->
    TransportDir = filename:join(?DB, ?RAFT_TRANSPORT_DIRNAME),
    case prim_file:list_dir(TransportDir) of
        {ok, Files} ->
            RetainMillis = ?RAFT_TRANSPORT_RETAIN_MIN_SECS() * 1000,
            NowMillis = erlang:system_time(millisecond),
            lists:foreach(
                fun (Filename) ->
                    Path = filename:join(TransportDir, Filename),
                    ID = list_to_integer(Filename),
                    case maps:get(ID, Pins, 0) of
                        0 ->
                            case transport_info(ID) of
                                {ok, #{end_ts := EndTs}} when NowMillis - EndTs > RetainMillis ->
                                    ?LOG_NOTICE("wa_raft_transport deleting ~p due to expiring after transport ended",
                                        [Filename], #{domain => [whatsapp, wa_raft]}),
                                    cleanup(ID, Path);
                                {ok, _Info} ->
                                    ok;
                                not_found ->
                                    ?LOG_NOTICE("wa_raft_transport deleting ~p due to having no associated transport",
                                        [Filename], #{domain => [whatsapp, wa_raft]}),
                                    cleanup(ID, Path)
                            end;
                        _ ->
                            ok
                    end
                end, Files);
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING("wa_raft_transport failed to list transports for cleanup due to ~p",
                [Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    end.

-spec cleanup(non_neg_integer(), string()) -> ok | {error, term()}.
cleanup(ID, Path) ->
    case file:del_dir_r(Path) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING("wa_raft_transport failed to cleanup transport ~p due to ~p",
                [ID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    end.

-spec maybe_submit_one(non_neg_integer(), map(), pid()) -> map().
maybe_submit_one(ID, #{status := running, next_file := NextFileID, total_files := LastFileID} = Info, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {send, ID, NextFileID}),
    case NextFileID of
        LastFileID -> maps:remove(next_file, Info);
        _          -> Info#{next_file => NextFileID + 1}
    end;
maybe_submit_one(_ID, Info, _Pid) ->
    Info.

-spec maybe_notify_complete(non_neg_integer(), map(), #state{}) -> ok | {error, term()}.
maybe_notify_complete(_ID, #{type := sender}, _State) ->
    ok;
maybe_notify_complete(_ID, #{status := Status}, _State) when Status =/= completed ->
    ok;
maybe_notify_complete(ID, #{type := receiver, root := Root, meta := #{type := snapshot, table := Table, partition := Partition, position := LogPos}}, #state{}) ->
    try wa_raft_server:snapshot_available(?RAFT_SERVER_NAME(Table, Partition), Root, LogPos) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_NOTICE("wa_raft_transport failed to notify ~p of transport ~p completion due to ~p",
                [?RAFT_SERVER_NAME(Table, Partition), ID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    catch
        T:E:S ->
            ?LOG_NOTICE("wa_raft_transport failed to notify ~p of transport ~p completion due to ~p ~p: ~n~p",
                [?RAFT_SERVER_NAME(Table, Partition), ID, T, E, S], #{domain => [whatsapp, wa_raft]}),
            {error, {T, E, S}}
    end;
maybe_notify_complete(ID, _Info, #state{}) ->
    ?LOG_NOTICE("wa_raft_transport finished transport ~p but does not know what to do with it",
        [ID], #{domain => [whatsapp, wa_raft]}).

-spec maybe_notify(non_neg_integer(), map()) -> map().
maybe_notify(ID, #{status := Status, notify := Notify} = Info) when Status =/= requested andalso Status =/= running ->
    ?RAFT_COUNT('raft.transport.complete'),
    gen_server:reply(Notify, {ok, ID}),
    maps:remove(notify, Info);
maybe_notify(_ID, Info) ->
    Info.

-spec scan_transport(non_neg_integer(), transport_info()) -> transport_info().
scan_transport(ID, #{status := running, atomics := TransportAtomics} = Info) ->
    LastUpdateTs = atomics:get(TransportAtomics, ?RAFT_TRANSPORT_ATOMICS_UPDATED_TS),
    NowMillis = erlang:system_time(millisecond),
    case NowMillis - LastUpdateTs >= ?RAFT_TRANSPORT_MAX_IDLE_SECS() * 1000 of
        true  -> maybe_notify(ID, Info#{status := timed_out, end_ts => NowMillis});
        false -> Info
    end;
scan_transport(_ID, Info) ->
    Info.

-spec schedule_scan() -> reference().
schedule_scan() ->
    erlang:send_after(?RAFT_TRANSPORT_SCAN_INTERVAL_SECS * 1000, self(), scan).
