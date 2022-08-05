%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements storage to apply group consensuses. The theory
%%% is that storage is an finite state machine. If we apply a sequence
%%% of changes in exactly same order on finite state machines, we get
%%% identical copies of finite state machines.
%%%
%%% A storage could be filesystem, ets, or any other local storage.
%%% Storage interface is defined as callbacks.

-module(wa_raft_storage).
-compile(warn_missing_spec).
-behaviour(gen_server).

%% OTP supervisor
-export([
    child_spec/1,
    start_link/1
]).

%% Read / Apply / Cancel operations.
-export([
    apply_op/3,
    fulfill_op/3,
    read_op/3,
    cancel/1
]).

%% API
-export([
    register/2,
    open_snapshot/2,
    create_snapshot/1,
    create_snapshot/2,
    create_empty_snapshot/2,
    delete_snapshot/2
]).

%% Cluster state API
-export([
    read_metadata/2
]).

%% Misc API
-export([
    stop/1,
    status/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([
    storage_handle/0,
    metadata/0,
    error/0,
    status/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl"). % used by ets:fun2ms
-include("wa_raft.hrl").

%% Storage plugin need implement the following mandatory callbacks to integrate with raft protocol.
%% Callback to open storage handle for operations
-callback storage_open(#raft_storage{}) -> {wa_raft_log:log_pos(), storage_handle()} | error().
%% Callback to close storage handle
-callback storage_close(storage_handle(), #raft_storage{}) -> ok | error().
%% Callback to apply an update to storage
-callback storage_apply(wa_raft_acceptor:command(), wa_raft_log:log_pos(), Handle :: storage_handle()) -> {term() | error(), storage_handle()}.
%% Callback to create a snapshot for current storage state
-callback storage_create_snapshot(string(), #raft_storage{}) -> ok | error().
%% Callback to create an empty snapshot. Used by snapshot transfer destination node to create an empty snapshot
%% previous to the transfer.
-callback storage_create_empty_snapshot(string(), #raft_storage{}) -> ok | error().
%% Callback to delete snapshot
-callback storage_delete_snapshot(string(), #raft_storage{}) -> ok | error().
%% Callback to open storage handle from a snapshot
-callback storage_open_snapshot(#raft_snapshot{}, #raft_storage{}) -> {wa_raft_log:log_pos(), storage_handle()} | error().

%% Callback to get the status of the RAFT storage module
-callback storage_status(Handle :: storage_handle()) -> proplists:proplist().

%% Callback to write RAFT cluster state values
-callback storage_write_metadata(Handle :: storage_handle(), Key :: metadata(), Version :: wa_raft_log:log_pos(), Value :: term()) -> ok | error().
%% Callback to read RAFT cluster state values
-callback storage_read_metadata(Handle :: storage_handle(), Key :: metadata()) -> {ok, Version :: wa_raft_log:log_pos(), Value :: term()} | undefined | error().

%% Optional callback to write key value for given log pos
-callback storage_write(storage_handle(), wa_raft_log:log_pos(), term(), binary(), map()) -> {ok, wa_raft_log:log_index()} | error().
%% Optional callback to read value for given key
-callback storage_read(storage_handle(), term()) -> {ok, {wa_raft_log:log_index() | undefined, wa_raft_log:log_term() | undefined, map(), binary() | undefined}} | error().
%% Optional callback to read version for given key
-callback storage_read_version(storage_handle(), term()) -> wa_raft_log:log_index() | undefined | error().
%% Optional callback to delete for given key
-callback storage_delete(storage_handle(), wa_raft_log:log_pos(), term()) -> ok | error().

-optional_callbacks([
    storage_read/2,
    storage_read_version/2,
    storage_write/5,
    storage_delete/3
]).

-type metadata() :: config | atom().
-type storage_handle() :: term().
-type error() :: {error, file:posix()}.

-type status() :: [status_element()].
-type status_element() ::
      {name, atom()}
    | {table, wa_raft:table()}
    | {partition, wa_raft:partition()}
    | {module, module()}
    | {last_applied, wa_raft_log:log_index()}
    | ModuleSpecificStatus :: {atom(), term()}.

-spec child_spec(Config :: [term()]) -> supervisor:child_spec().
child_spec(Config) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Config]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

%% Public API
-spec start_link(RaftArgs :: wa_raft:args()) -> {'ok', Pid::pid()} | 'ignore' | {'error', Reason::term()}.
start_link(#{table := Table, partition := Partition} = RaftArgs) ->
    Name = ?RAFT_STORAGE_NAME(Table, Partition),
    gen_server:start_link({local, Name}, ?MODULE, [RaftArgs], []).

-spec register(Pid :: pid(), ServerPid :: pid()) -> {ok, LastApplied :: wa_raft_log:log_pos()}.
register(Pid, ServerPid) ->
    gen_server:call(Pid, {register, ServerPid}, ?RPC_CALL_TIMEOUT_MS).

-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop, ?RPC_CALL_TIMEOUT_MS).

-spec status(Pid :: pid()) -> status().
status(Pid) ->
    gen_server:call(Pid, status, ?STORAGE_CALL_TIMEOUT_MS).

-spec apply_op(pid(), wa_raft_log:log_record(), wa_raft_log:log_term()) -> ok.
apply_op(Pid, LogRecord, ServerTerm) ->
    gen_server:cast(Pid, {apply, LogRecord, ServerTerm}).

-spec fulfill_op(pid(), reference(), term()) -> ok.
fulfill_op(Pid, OpRef, Return) ->
    gen_server:cast(Pid, {fulfill, OpRef, Return}).

-spec read_op(Pid :: pid(), ExpectedLogPos :: non_neg_integer(), {From :: {pid(), term()}, Command :: term()}) -> ok.
read_op(Pid, ExpectedLogPos, {From, Command}) ->
    gen_server:cast(Pid, {read, ExpectedLogPos, From, Command}).

-spec cancel(pid()) -> ok.
cancel(Pid) ->
    gen_server:cast(Pid, cancel).

-spec open_snapshot(Pid :: pid(), LastAppliedPos :: wa_raft_log:log_pos()) -> ok | error().
open_snapshot(Pid, LastAppliedPos) ->
    gen_server:call(Pid, {snapshot_open, LastAppliedPos}, ?STORAGE_CALL_TIMEOUT_MS).

-spec create_snapshot(ServerRef :: pid() | atom() | {Name :: atom(), Node :: atom()}) -> {ok, Pos :: wa_raft_log:log_pos()} | error().
create_snapshot(ServerRef) ->
    gen_server:call(ServerRef, snapshot_create, ?STORAGE_CALL_TIMEOUT_MS).

-spec create_snapshot(Pid :: pid(), Name :: string()) -> ok | error().
create_snapshot(Pid, Name) ->
    gen_server:call(Pid, {snapshot_create, Name}, ?STORAGE_CALL_TIMEOUT_MS).

-spec create_empty_snapshot(Pid :: pid(), Name :: string()) -> ok | error().
create_empty_snapshot(Pid, Name) ->
    gen_server:call(Pid, {snapshot_create_empty, Name}, ?STORAGE_CALL_TIMEOUT_MS).

-spec delete_snapshot(Pid :: pid(), Name :: string()) -> ok.
delete_snapshot(Pid, Name) ->
    gen_server:cast(Pid, {snapshot_delete, Name}).

-spec read_metadata(Pid :: pid(), Key :: metadata()) -> {ok, Version :: wa_raft_log:log_pos(), Value :: term()} | undefined | error().
read_metadata(Pid, Key) ->
    gen_server:call(Pid, {read_metadata, Key}, ?STORAGE_CALL_TIMEOUT_MS).

%% gen_server callbacks
-spec init([wa_raft:args()]) -> {ok, #raft_storage{}}.
init([#{table := Table, partition := Partition, counters := Counters} = Args]) ->
    process_flag(trap_exit, true),
    Module = maps:get(storage_module, Args, ?RAFT_CONFIG(raft_storage_module, wa_raft_storage_ets)),
    ?LOG_NOTICE("Starting raft storage module ~p on ~p:~p", [Module, Table, Partition], #{domain => [whatsapp, wa_raft]}),
    Name = ?RAFT_STORAGE_NAME(Table, Partition),
    RootDir = ?ROOT_DIR(Table, Partition),
    State0 = #raft_storage{name = Name, table = Table, partition = Partition, root_dir = RootDir, module = Module, counters = Counters},
    {LastApplied, Handle} = Module:storage_open(State0),
    State1 = State0#raft_storage{last_applied = LastApplied, handle = Handle},
    counters:put(Counters, ?RAFT_LOCAL_COUNTER_APPLY, 0),
    %% Here, supervisor should have already started wa_raft_acceptor.
    AcceptorPid = whereis(?RAFT_ACCEPTOR_NAME(Table, Partition)),
    %% Notify the wa_raft_acceptor of the wa_raft_storage pid.
    ok = wa_raft_acceptor:register_storage(AcceptorPid, self()),
    {ok, State1}.

%% The interaction between the RAFT server and the RAFT storage server is designed to be
%% as asynchronous as possible since the RAFT storage server may be caught up in handling
%% a long running I/O request while it is working on applying new log entries.
%% If you are adding a new call to the RAFT storage server, make sure that it is either
%% guaranteed to not be used when the storage server is busy (and may not reply in time)
%% or timeouts and other failures are handled properly.
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: #raft_storage{}) ->
    {reply, Reply :: term(), NewState :: #raft_storage{}} | {stop, Reason :: term(), Reply :: term(), NewState :: #raft_storage{}}.
handle_call({register, ServerPid}, _From, #raft_storage{name = Name, last_applied = LastApplied} = State) ->
    ?LOG_NOTICE("[~p] registering server pid ~p", [Name, ServerPid], #{domain => [whatsapp, wa_raft]}),
    {reply, {ok, LastApplied}, State#raft_storage{server_pid = ServerPid}};

handle_call(snapshot_create, _From, #raft_storage{last_applied = #raft_log_pos{index = LastIndex, term = LastTerm}} = State) ->
    Name = ?SNAPSHOT_NAME(LastIndex, LastTerm),
    case create_snapshot_impl(Name, State) of
        ok ->
            {reply, {ok, #raft_log_pos{index = LastIndex, term = LastTerm}}, State};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({snapshot_create, Name}, _From, State) ->
    Result = create_snapshot_impl(Name, State),
    {reply, Result, State};

handle_call({snapshot_create_empty, Name}, _From, #raft_storage{module = Module} = State) ->
    cleanup_snapshots(State),
    ?LOG_NOTICE("Create empty snapshot ~s for ~p.", [Name, Name], #{domain => [whatsapp, wa_raft]}),
    Result = Module:storage_create_empty_snapshot(Name, State),
    {reply, Result, State};

handle_call({snapshot_open, #raft_log_pos{index = LastIndex, term = LastTerm} = LogPos}, _From, #raft_storage{name = Name, module = Module, handle = OldHandle} = State) ->
    ?LOG_NOTICE("~100p reads snapshot at ~200p.", [Name, LogPos], #{domain => [whatsapp, wa_raft]}),
    ok = Module:storage_close(OldHandle, State),
    Snapshot = #raft_snapshot{name = ?SNAPSHOT_NAME(LastIndex, LastTerm), last_applied = LogPos},
    case Module:storage_open_snapshot(Snapshot, State) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {_LastApplied, NewHandle} ->
            {reply, ok, State#raft_storage{last_applied = LogPos, handle = NewHandle}}
    end;

handle_call({read_metadata, Key}, _From, #raft_storage{module = Module, handle = Handle} = State) ->
    ?RAFT_COUNT('raft.storage.read_metadata'),
    Result = Module:storage_read_metadata(Handle, Key),
    {reply, Result, State};

handle_call(status, _From, #raft_storage{module = Module, handle = Handle} = State) ->
    Status = [
        {name, State#raft_storage.name},
        {table, State#raft_storage.table},
        {partition, State#raft_storage.partition},
        {module, State#raft_storage.module},
        {last_applied, State#raft_storage.last_applied#raft_log_pos.index}
    ],
    ModuleStatus = Module:storage_status(Handle),
    {reply, Status ++ ModuleStatus, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Cmd, From, #raft_storage{name = Name} = State) ->
    ?LOG_WARNING("[~p] unexpected call ~p from ~p", [Name, Cmd, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request :: term(), State :: #raft_storage{}) -> {noreply, NewState :: #raft_storage{}}.
handle_cast(cancel, State0) ->
    State1 = cancel_pending_commits(State0),
    State2 = cancel_pending_reads(State1),
    {noreply, State2};

handle_cast({fulfill, Ref, Return}, State0) ->
    State1 = reply(Ref, Return, State0),
    {noreply, State1};

% Apply an op after consensus is made
handle_cast({apply, {LogIndex, {LogTerm, _}} = LogRecord, ServerTerm}, #raft_storage{name = Name} = State0) ->
    ?LOG_DEBUG("[~p] apply ~p:~p", [Name, LogIndex, LogTerm], #{domain => [whatsapp, wa_raft]}),
    State1 = apply_impl(LogRecord, ServerTerm, State0),
    {noreply, State1};

handle_cast({read, ExpectedLogPos, From, Command}, #raft_storage{name = Name} = State0) ->
    ?LOG_DEBUG("[~p] read ~p", [Name, ExpectedLogPos], #{domain => [whatsapp, wa_raft]}),
    State1 = read_impl(ExpectedLogPos, From, Command, State0),
    {noreply, State1};

handle_cast({snapshot_delete, SnapName}, #raft_storage{name = Name, module = Module} = State) ->
    Result = Module:storage_delete_snapshot(SnapName, State),
    ?LOG_NOTICE("~100p delete snapshot ~p. result ~p", [Name, SnapName, Result], #{domain => [whatsapp, wa_raft]}),
    {noreply, State};

handle_cast(Cmd, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Cmd], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(Request :: term(), State :: #raft_storage{}) -> {noreply, NewState :: #raft_storage{}}.
handle_info(Command, State) ->
    ?LOG_WARNING("Unexpected info ~p", [Command], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State0 :: #raft_storage{}) -> State1 :: #raft_storage{}.
terminate(Reason, #raft_storage{name = Name, module = Module, handle = Handle} = State) ->
    try
        Module:storage_close(Handle, State),
        ?LOG_NOTICE("Storage ~p terminated for reason ~p", [Name, Reason], #{domain => [whatsapp, wa_raft]})
    catch
        _T:Error:Stack ->
            ?LOG_ERROR("Storage ~p error ~p stack ~0P", [Name, Error, Stack, 100], #{domain => [whatsapp, wa_raft]})
    end,
    State.

-spec code_change(_OldVsn :: term(), State :: #raft_storage{}, Extra :: term()) -> {ok, State :: #raft_storage{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions

-spec apply_impl(Record :: wa_raft_log:log_record(), ServerTerm :: wa_raft_log:log_term(), State :: #raft_storage{}) -> NewState :: #raft_storage{}.
apply_impl({LogIndex, {LogTerm, {Ref, _} = Op}}, ServerTerm,
           #raft_storage{name = Name, counters = Counters, last_applied = #raft_log_pos{index = LastAppliedIndex}} = State0) ->
    counters:sub(Counters, ?RAFT_LOCAL_COUNTER_APPLY, 1),
    StartT = os:timestamp(),
    case LogIndex of
        LastAppliedIndex ->
            apply_delayed_reads(State0);
        _ when LogIndex =:= LastAppliedIndex + 1 ->
            {Reply, State1} = storage_apply(#raft_log_pos{index = LogIndex, term = LogTerm}, Op, State0),
            State2 = case LogTerm =:= ServerTerm of
                true -> reply(Ref, Reply, State1);
                false -> State1
            end,
            State3 = State2#raft_storage{last_applied = #raft_log_pos{index = LogIndex, term = LogTerm}},
            State4 = apply_delayed_reads(State3),
            ?LOG_DEBUG("applied ~p:~p", [LogIndex, LogTerm], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_GATHER('raft.storage.apply.func', timer:now_diff(os:timestamp(), StartT)),
            State4;
        _ ->
            ?LOG_ERROR("[~p] received out-of-order apply with index ~p. (expected index ~p, op ~0P)", [Name, LogIndex, LastAppliedIndex, Op, 30], #{domain => [whatsapp, wa_raft]}),
            error(out_of_order_apply)
    end.

-spec read_impl(ExpectedLogIndex :: wa_raft_log:log_index(), From :: {pid(), term()}, Command :: term(), State :: #raft_storage{})
    -> NewState :: #raft_storage{}.
read_impl(ExpectedLogIndex, From, Command, #raft_storage{last_applied = LogPos} = State) ->
    {Res, NewState} = execute(Command, LogPos, State),
    Result = case Res of
            ok -> % noop
                ok;
            {ok, {CurValue, Header, CurLogIndex}} when ExpectedLogIndex =:= undefined orelse (CurLogIndex =/= undefined andalso CurLogIndex > ExpectedLogIndex) ->
                {ok, {CurValue, Header, CurLogIndex}};
            {ok, {CurValue, Header, CurLogIndex}} when CurLogIndex =:= ExpectedLogIndex ->
                {ok, {CurValue, Header, CurLogIndex}};
            {ok, {_CurValue, _Header, CurLogIndex}} ->
                ?LOG_WARNING("Stale Read ~p. Expected log index ~p vs current ~p. ", [Command, ExpectedLogIndex, CurLogIndex], #{domain => [whatsapp, wa_raft]}),
                ?RAFT_COUNT('raft.storage.read.error.stale'),
                {error, {stale, CurLogIndex}};
            {error, _} = Error ->
                ?RAFT_COUNT('raft.storage.read.error'),
                ?LOG_WARNING("Read ~p error ~p", [Command, Error], #{domain => [whatsapp, wa_raft]}),
                Error
        end,
    gen_server:reply(From, Result),
    NewState.

-spec storage_apply(wa_raft_log:log_pos(), wa_raft_acceptor:op(), #raft_storage{}) -> {term(), #raft_storage{}}.
storage_apply(LogPos, {_Ref, Command}, State) ->
    ?RAFT_COUNT('raft.storage.apply'),
    execute(Command, LogPos, State).

-spec execute(Command :: term(), LogPos :: wa_raft_log:log_pos(), State :: #raft_storage{}) -> {term() | error(), #raft_storage{}}.
execute(noop, LogPos, #raft_storage{name = Name, module = Module, handle = Handle} = State) ->
    ?LOG_NOTICE("Noop for ~100p at pos ~w", [Name, LogPos], #{domain => [whatsapp, wa_raft]}),
    {Reply, NewHandle} = Module:storage_apply(noop, LogPos, Handle),
    {Reply, State#raft_storage{handle = NewHandle}};
execute({config, Config}, #raft_log_pos{index = Index, term = Term} = Version, #raft_storage{name = Name, module = Module, handle = Handle} = State) ->
    ?LOG_INFO("Storage[~p] applying new configuration ~p at ~p:~p.",
        [Name, Config, Index, Term], #{domain => [whatsapp, wa_raft]}),
    {Module:storage_write_metadata(Handle, config, Version, Config), State};
execute({execute, Table, _Key, Mod, Fun, Args}, LogPos, #raft_storage{partition = Partition, handle = Handle} = State) ->
    Result = try
        erlang:apply(Mod, Fun, [Handle, LogPos, Table] ++ Args)
    catch
        _T:Error:Stack ->
            ?RAFT_COUNT('raft.storage.apply.execute.error'),
            ?LOG_WARNING("Execute ~p:~p ~0P on ~p:~p. error ~p~nStack ~100P", [Mod, Fun, Args, 20, Table, Partition, Error, Stack, 100], #{domain => [whatsapp, wa_raft]}),
            {error, Error}
    end,
    {Result, State};
execute(Command, LogPos, #raft_storage{module = Module, handle = Handle} = State) ->
    {Reply, NewHandle} = Module:storage_apply(Command, LogPos, Handle),
    {Reply, State#raft_storage{handle = NewHandle}}.

-spec reply(reference(), term(), #raft_storage{}) -> #raft_storage{}.
reply(Ref, Reply, #raft_storage{table = Table, partition = Partition, counters = Counters} = State) ->
    case ets:lookup(?RAFT_PENDING_COMMITS_TABLE(Table, Partition), Ref) of
        [] ->
            State;
        [{Ref, From}] ->
            ets:delete(?RAFT_PENDING_COMMITS_TABLE(Table, Partition), Ref),
            counters:sub(Counters, ?RAFT_LOCAL_COUNTER_COMMIT, 1),
            gen_server:reply(From, Reply),
            State
    end.

-spec apply_delayed_reads(State :: #raft_storage{}) -> NewState :: #raft_storage{}.
apply_delayed_reads(#raft_storage{table = Table, partition = Partition, last_applied = #raft_log_pos{index = LastAppliedIndex} = LastAppliedLogPos,
                                  counters = Counters} = State) ->
    case counters:get(Counters, ?RAFT_LOCAL_COUNTER_READ) =:= 0 of
        true ->
            State;
        _ ->
            MatchSpec = ets:fun2ms(
                fun({{LogIndex, R}, F, C}) when LogIndex =:= LastAppliedIndex ->
                    {{LogIndex, R}, F, C}
                end
            ),
            [
                begin
                    ets:delete(?RAFT_PENDING_READS_TABLE(Table, Partition), LogIndexRef),
                    {Reply, _} = execute(Command, LastAppliedLogPos, State),
                    counters:sub(Counters, ?RAFT_LOCAL_COUNTER_READ, 1),
                    gen_server:reply(From, Reply)
                end
            || {LogIndexRef, From, Command} <- ets:select(?RAFT_PENDING_READS_TABLE(Table, Partition), MatchSpec)],
            State
    end.

-spec cancel_pending_commits(#raft_storage{}) -> #raft_storage{}.
cancel_pending_commits(#raft_storage{table = Table, partition = Partition, name = Name, counters = Counters} = State0) ->
    ?LOG_NOTICE("[~p] cancel pending commits", [Name], #{domain => [whatsapp, wa_raft]}),
    [
        begin
            ets:delete(?RAFT_PENDING_COMMITS_TABLE(Table, Partition), Ref),
            counters:sub(Counters, ?RAFT_LOCAL_COUNTER_COMMIT, 1),
            gen_server:reply(From, {error, not_leader})
        end
        || {Ref, From} <- ets:tab2list(?RAFT_PENDING_COMMITS_TABLE(Table, Partition))
    ],
    State0.

-spec cancel_pending_reads(#raft_storage{}) -> #raft_storage{}.
cancel_pending_reads(#raft_storage{table = Table, partition = Partition, name = Name, counters = Counters} = State0) ->
    ?LOG_NOTICE("[~p] cancel pending reads", [Name], #{domain => [whatsapp, wa_raft]}),
    MatchSpec = ets:fun2ms(fun({LR, F, _Op}) -> {LR, F} end),
    [
        begin
            ets:delete(?RAFT_PENDING_READS_TABLE(Table, Partition), LogIndexRef),
            counters:sub(Counters, ?RAFT_LOCAL_COUNTER_READ, 1),
            gen_server:reply(From, {error, not_leader})
        end
        || {LogIndexRef, From} <- ets:select(?RAFT_PENDING_READS_TABLE(Table, Partition), MatchSpec)
    ],
    State0.

-spec list_snapshots(RootDir :: string()) -> [#raft_snapshot{}].
list_snapshots(RootDir) ->
    Dirs = filelib:wildcard(?SNAPSHOT_PREFIX ++ ".*", RootDir),
    Snapshots = lists:foldl(fun decode_snapshot_name/2, [], Dirs),
    lists:keysort(#raft_snapshot.last_applied, Snapshots).

-spec create_snapshot_impl(SnapName :: string(), Storage :: #raft_storage{}) -> ok | error().
create_snapshot_impl(SnapName, #raft_storage{name = Name, root_dir = RootDir, module = Module} = State) ->
    case filelib:is_dir(RootDir ++ SnapName) of
        true ->
            ?LOG_NOTICE("Snapshot ~s for ~p already exists. Skipping snapshot creation.", [SnapName, Name], #{domain => [whatsapp, wa_raft]}),
            ok;
        false ->
            cleanup_snapshots(State),
            ?LOG_NOTICE("Create snapshot ~s for ~p.", [SnapName, Name], #{domain => [whatsapp, wa_raft]}),
            Module:storage_create_snapshot(SnapName, State)
    end.

-define(MAX_RETAINED_SNAPSHOT, 1).

-spec cleanup_snapshots(#raft_storage{}) -> ok.
cleanup_snapshots(#raft_storage{root_dir = RootDir, module = Module} = State) ->
    SnapshotDirs = [Dir || #raft_snapshot{name = Dir} <- list_snapshots(RootDir)],
    case length(SnapshotDirs) > ?MAX_RETAINED_SNAPSHOT of
        true ->
            ToBeRemoved = lists:sublist(SnapshotDirs, length(SnapshotDirs) - ?MAX_RETAINED_SNAPSHOT),
            ?LOG_NOTICE("Remove snapshot ~p", [ToBeRemoved], #{domain => [whatsapp, wa_raft]}),
            [ Module:storage_delete_snapshot(Name, State) || Name <- ToBeRemoved],
            ok;
        _ ->
            ok
    end.

%% Private functions
-spec decode_snapshot_name(Name :: string(), Acc0 :: [#raft_snapshot{}]) -> Acc1 :: [#raft_snapshot{}].
decode_snapshot_name(Name, Acc0) ->
    case string:lexemes(Name, ".") of
        [?SNAPSHOT_PREFIX, IndexStr, TermStr] ->
            case {list_to_integer(IndexStr), list_to_integer(TermStr)} of
                {Index, Term} when Index >= 0 andalso Term >= 0 ->
                    [#raft_snapshot{name = Name, last_applied = #raft_log_pos{index = Index, term = Term}} | Acc0];
                _ ->
                    ?LOG_WARNING("Invalid snapshot with invalid index (~p) and/or term (~p). (full name ~p)", [IndexStr, TermStr, Name], #{domain => [whatsapp, wa_raft]}),
                    Acc0
            end;
        _ ->
            ?LOG_WARNING("Invalid snapshot dir name ~p", [Name], #{domain => [whatsapp, wa_raft]}),
            Acc0
    end.
