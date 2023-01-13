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
    cancel/1
]).

%% API
-export([
    open/1,
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
-callback storage_read(storage_handle(), term()) -> {ok, {wa_raft_log:log_index(), wa_raft_log:log_term(), map(), binary()} | {undefined, undefined, map(), undefined}} | error().
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
-type error() :: {error, term()}.

-type status() :: [status_element()].
-type status_element() ::
      {name, atom()}
    | {table, wa_raft:table()}
    | {partition, wa_raft:partition()}
    | {module, module()}
    | {last_applied, wa_raft_log:log_index()}
    | ModuleSpecificStatus :: {atom(), term()}.

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

%% Public API
-spec start_link(Options :: #raft_options{}) -> {'ok', Pid::pid()} | 'ignore' | {'error', Reason::term()}.
start_link(#raft_options{table = Table, partition = Partition} = Options) ->
    gen_server:start_link({local, ?RAFT_STORAGE_NAME(Table, Partition)}, ?MODULE, Options, []).

-spec status(ServiceRef :: pid() | atom()) -> status().
status(ServiceRef) ->
    gen_server:call(ServiceRef, status, ?STORAGE_CALL_TIMEOUT_MS).

-spec apply_op(ServiceRef :: pid() | atom(), LogRecord :: wa_raft_log:log_record(), ServerTerm :: wa_raft_log:log_term()) -> ok.
apply_op(ServiceRef, LogRecord, ServerTerm) ->
    gen_server:cast(ServiceRef, {apply, LogRecord, ServerTerm}).

-spec fulfill_op(ServiceRef :: pid() | atom(), Reference :: term(), Reply :: term()) -> ok.
fulfill_op(ServiceRef, OpRef, Return) ->
    gen_server:cast(ServiceRef, {fulfill, OpRef, Return}).

-spec cancel(ServiceRef :: pid() | atom()) -> ok.
cancel(ServiceRef) ->
    gen_server:cast(ServiceRef, cancel).

-spec open(ServiceRef :: pid() | atom()) -> {ok, LastApplied :: wa_raft_log:log_pos()}.
open(ServiceRef) ->
    gen_server:call(ServiceRef, open, ?RPC_CALL_TIMEOUT_MS).

-spec open_snapshot(ServiceRef :: pid() | atom(), LastAppliedPos :: wa_raft_log:log_pos()) -> ok | error().
open_snapshot(ServiceRef, LastAppliedPos) ->
    gen_server:call(ServiceRef, {snapshot_open, LastAppliedPos}, ?STORAGE_CALL_TIMEOUT_MS).

-spec create_snapshot(ServiceRef :: pid() | atom()) -> {ok, Pos :: wa_raft_log:log_pos()} | error().
create_snapshot(ServiceRef) ->
    gen_server:call(ServiceRef, snapshot_create, ?STORAGE_CALL_TIMEOUT_MS).

-spec create_snapshot(ServiceRef :: pid() | atom(), Name :: string()) -> ok | error().
create_snapshot(ServiceRef, Name) ->
    gen_server:call(ServiceRef, {snapshot_create, Name}, ?STORAGE_CALL_TIMEOUT_MS).

-spec create_empty_snapshot(ServiceRef :: pid() | atom(), Name :: string()) -> ok | error().
create_empty_snapshot(ServiceRef, Name) ->
    gen_server:call(ServiceRef, {snapshot_create_empty, Name}, ?STORAGE_CALL_TIMEOUT_MS).

-spec delete_snapshot(ServiceRef :: pid() | atom(), Name :: string()) -> ok.
delete_snapshot(ServiceRef, Name) ->
    gen_server:cast(ServiceRef, {snapshot_delete, Name}).

-spec read_metadata(ServiceRef :: pid() | atom(), Key :: metadata()) -> {ok, Version :: wa_raft_log:log_pos(), Value :: eqwalizer:dynamic()} | undefined | error().
read_metadata(ServiceRef, Key) ->
    gen_server:call(ServiceRef, {read_metadata, Key}, ?STORAGE_CALL_TIMEOUT_MS).

%% gen_server callbacks
-spec init(Options :: #raft_options{}) -> {ok, #raft_storage{}}.
init(#raft_options{table = Table, partition = Partition, storage_module = Module}) ->
    process_flag(trap_exit, true),
    ?LOG_NOTICE("Starting raft storage module ~p on ~p:~p", [Module, Table, Partition], #{domain => [whatsapp, wa_raft]}),
    Name = ?RAFT_STORAGE_NAME(Table, Partition),
    RootDir = ?ROOT_DIR(Table, Partition),
    State0 = #raft_storage{name = Name, table = Table, partition = Partition, root_dir = RootDir, module = Module},
    {LastApplied, Handle} = Module:storage_open(State0),
    State1 = State0#raft_storage{last_applied = LastApplied, handle = Handle},
    {ok, State1}.

%% The interaction between the RAFT server and the RAFT storage server is designed to be
%% as asynchronous as possible since the RAFT storage server may be caught up in handling
%% a long running I/O request while it is working on applying new log entries.
%% If you are adding a new call to the RAFT storage server, make sure that it is either
%% guaranteed to not be used when the storage server is busy (and may not reply in time)
%% or timeouts and other failures are handled properly.
-spec handle_call(Request, From :: {pid(), term()}, State :: #raft_storage{}) ->
    {reply, Reply :: term(), NewState :: #raft_storage{}} |
    {noreply, NewState :: #raft_storage{}} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #raft_storage{}}
    when Request ::
        open |
        snapshot_create |
        {snapshot_create, Name :: string()} |
        {snapshot_create_empty, Name :: string()} |
        {snapshot_open, LastAppliedPos :: wa_raft_log:log_pos()} |
        {read_metadata, Key :: metadata()}.
handle_call(open, _From, #raft_storage{last_applied = LastApplied} = State) ->
    {reply, {ok, LastApplied}, State};

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

handle_call(Cmd, From, #raft_storage{name = Name} = State) ->
    ?LOG_WARNING("[~p] unexpected call ~p from ~p", [Name, Cmd, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request, State :: #raft_storage{}) -> {noreply, NewState :: #raft_storage{}}
    when Request ::
        cancel |
        {fulfill, term(), term()} |
        {appy, LogRecord :: wa_raft_log:log_record(), ServerTerm :: wa_raft_log:log_term()} |
        {snapshot_delete, Name :: string()}.
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
           #raft_storage{name = Name, table = Table, partition = Partition, last_applied = #raft_log_pos{index = LastAppliedIndex}} = State0) ->
    wa_raft_queue:fulfill_apply(Table, Partition),
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

-spec storage_apply(wa_raft_log:log_pos(), wa_raft_acceptor:op(), #raft_storage{}) -> {term(), #raft_storage{}}.
storage_apply(LogPos, {_Ref, Command}, State) ->
    ?RAFT_COUNT('raft.storage.apply'),
    execute(Command, LogPos, State).

-spec execute(Command :: wa_raft_acceptor:command(), LogPos :: wa_raft_log:log_pos(), State :: #raft_storage{}) -> {term() | error(), #raft_storage{}}.
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

-spec reply(term(), term(), #raft_storage{}) -> #raft_storage{}.
reply(Ref, Reply, #raft_storage{table = Table, partition = Partition} = State) ->
    wa_raft_queue:fulfill_commit(Table, Partition, Ref, Reply),
    State.

-spec apply_delayed_reads(State :: #raft_storage{}) -> NewState :: #raft_storage{}.
apply_delayed_reads(#raft_storage{table = Table, partition = Partition, last_applied = #raft_log_pos{index = LastAppliedIndex} = LastAppliedLogPos} = State) ->
    lists:foreach(
        fun ({Reference, Command}) ->
            {Reply, _} = execute(Command, LastAppliedLogPos, State),
            wa_raft_queue:fulfill_read(Table, Partition, Reference, Reply)
        end, wa_raft_queue:query_reads(Table, Partition, LastAppliedIndex)),
    State.

-spec cancel_pending_commits(#raft_storage{}) -> #raft_storage{}.
cancel_pending_commits(#raft_storage{table = Table, partition = Partition, name = Name} = State) ->
    ?LOG_NOTICE("[~p] cancel pending commits", [Name], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_all_commits(Table, Partition, {error, not_leader}),
    State.

-spec cancel_pending_reads(#raft_storage{}) -> #raft_storage{}.
cancel_pending_reads(#raft_storage{table = Table, partition = Partition, name = Name} = State) ->
    ?LOG_NOTICE("[~p] cancel pending reads", [Name], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_all_reads(Table, Partition, {error, not_leader}),
    State.

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
