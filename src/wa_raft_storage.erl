%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% The RAFT storage server provides functionality for handling the
%%% state machine replicated by RAFT in a way suitable for implementing
%%% storage solutions on top the RAFT consensus algorithm.

-module(wa_raft_storage).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

%% OTP Supervision
-export([
    child_spec/1,
    start_link/1
]).

%% Read / Apply / Cancel operations.
-export([
    apply_op/3,
    read/2,
    read/3,
    cancel/1
]).

%% API
-export([
    open/1,
    open_snapshot/3,
    create_snapshot/1,
    create_snapshot/2,
    delete_snapshot/2
]).

%% Cluster state API
-export([
    read_metadata/2
]).

%% Label API
-export([
    label/1
]).

%% Misc API
-export([
    status/1
]).

%% Internal API
-export([
    default_name/2,
    registered_name/2
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

%% Test API
-ifdef(TEST).
-export([
    reset/3
]).
-endif.

-export_type([
    storage_handle/0,
    metadata/0,
    error/0,
    status/0
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%%-----------------------------------------------------------------------------
%% RAFT Storage
%%-----------------------------------------------------------------------------
%% The RAFT consensus algorithm provides sequential consistency guarantees by
%% ensuring the consistent replication of the "RAFT log", which is a sequence
%% of "write commands", or "log entries". The RAFT algorithm intends for these
%% entries to be applied sequentially against an underlying state machine. As
%% this implementation of RAFT is primarily designed for implementation of
%% storage solutions, we call the underlying state machine the "storage" and
%% the state of the state machine after the application of each log entry the
%% "storage state". As the sequence of commands is the same on each replica,
%% the observable storage state after each log entry should also be the same.
%%-----------------------------------------------------------------------------
%% RAFT Storage Provider
%%-----------------------------------------------------------------------------
%% This RAFT implementation provides the opportunity for users to define how
%% exactly the "storage" should be implemented by defining a "storage provider"
%% module when setting up a RAFT partition.
%%
%% Apart from certain expectations of the "position" of the storage state and
%% metadata stored on behalf of the RAFT implementation, storage providers are
%% free to handle commands in any way they see fit. However, to take advantage
%% of the consistency guarantees provided by the RAFT algorithm, it is best to
%% ensure a fundamental level of consistency, atomicity, and durability.
%%
%% The RAFT storage server is designed to be able to tolerate crashes caused
%% by storage providers. If any callback could not be handled in a way in
%% which it would be safe to continue operation, then storage providers are
%% expected to raise an error to reset the RAFT replica to a known good state.
%%-----------------------------------------------------------------------------

%% Open the storage state for the specified RAFT partition.
-callback storage_open(Name :: atom(), RaftIdentifier :: #raft_identifier{}, PartitionPath :: file:filename()) -> Handle :: storage_handle().

%% Get any custom status to be reported alongside the status reported by the
%% RAFT storage server.
-callback storage_status(Handle :: storage_handle()) -> [{atom(), term()}].
-optional_callbacks([storage_status/1]).

%% Close a previously opened storage state.
-callback storage_close(Handle :: storage_handle()) -> term().

%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Position
%%-----------------------------------------------------------------------------
%% The position of a storage state is the log position of the write command
%% that was most recently applied against the state. This position should be
%% available anytime immediately after the storage is opened and after any
%% write command is applied.
%%-----------------------------------------------------------------------------

%% Issue a read command to get the position of the current storage state.
-callback storage_position(Handle :: storage_handle()) -> Position :: wa_raft_log:log_pos().

%% Issue a read command to get the label associated with the most
%% recent command that was applied with a label. See the optional
%% callback `storage_apply/4` for details.
-callback storage_label(Handle :: storage_handle()) -> {ok, Label :: wa_raft_label:label()} | error().
-optional_callbacks([storage_label/1]).
%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Write Commands
%%-----------------------------------------------------------------------------
%% A "write command" is one that may cause the results of future read or write
%% commands to produce different results. All write commands are synchronized
%% by being added to the RAFT log and replicated. The RAFT protocol guarantees
%% that all replicas will apply all write commands in the same order without
%% omission. For best behaviour, the handling of write commands should ensure
%% a fundamental level of consistency, atomicity, and durability.
%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Consistency
%%-----------------------------------------------------------------------------
%% For most practical applications, it is sufficient to ensure that, regardless
%% of the internal details of the starting and intermediate storage states,
%% two independent applications of the same sequence of write commands produces
%% a storage state that will continue to produce the same results when any
%% valid sequence of future commands is applied to both identically.
%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Atomicity and Durability against Failures
%%-----------------------------------------------------------------------------
%% As part of ensuring that a RAFT replica can recover from sudden unexpected
%% failure, storage providers should be able to gracefully recover from the
%% unexpected termination of the RAFT storage server or node resulting in
%% the opening of a storage state that was not previously closed or whose
%% operation was interrupted in the middle of a previous command.
%%
%% Generally, ensuring these qualities requires that implementations make
%% changes that may be saved to a durable media that will persist between
%% openings of the storage to be performed atomically (either actually or
%% effectively) so that it is not possible for opening the storage to
%% result in observing any intermediate state. On the other hand, that any
%% applied changes are made durable against restart is only necessary insofar
%% as the log of commands still retains those log entries necessary tt
%% reproduce the lost changes.
%%-----------------------------------------------------------------------------

%% Apply a write command against the storage state, updating the state as
%% required if a standard command or as desired for custom commands.
%% If the command could not be applied in a manner so as to preserve the
%% desired consistency guarantee then implementations can raise an error to
%% cause the apply to be aborted safely.
-callback storage_apply(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Handle :: storage_handle()) -> {Result :: dynamic(), NewHandle :: storage_handle()}.

%% Apply a write command against the storage state, in the same way as the
%% above `storage_apply/3` callback. The provided label should be maintained
%% in the storage state so that it is returned by subsequent calls to
%% `storage_label/1`. If this callback is defined, `storage_label/1` must
%% also be defined.
-callback storage_apply(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Label :: wa_raft_label:label(), Handle :: storage_handle()) -> {Result :: dynamic(), NewHandle :: storage_handle()}.
-optional_callbacks([storage_apply/4]).

%% Apply a write command to update metadata stored by the storage provider
%% on behalf of the RAFT implementation. Subsequent calls to read metadata
%% with the same key should return the updated version and value.
%% If the command could not be applied in a manner so as to preserve the
%% desired consistency guarantee then implementations can raise an error to
%% cause the apply to be aborted safely.
-callback storage_write_metadata(Handle :: storage_handle(), Key :: metadata(), Version :: wa_raft_log:log_pos(), Value :: term()) -> ok | error().

%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Read Commands
%%-----------------------------------------------------------------------------
%% In some cases, the RAFT implementation may request a storage provider to
%% handle commands that could require consulting the storage state but are not
%% commands that were replicated and committed by the RAFT protocol. Such
%% commands are called "read commands".
%%
%% Storage providers are recommended to ensure that the execution of read
%% commands produce no externally visible side-effects. Ideally, the insertion
%% or removal of a read command anywhere into the RAFT log (with any number
%% of other read commands already inserted) would not affect the result
%% returned by any other command or the results of any future commands.
%%
%% Implicitly, use of the `storage_position/1` and `storage_read_metadata/2`
%% callbacks are non-synchronized access of the storage state and should be
%% considered to be read commands.
%%
%% Not exhaustively, the RAFT implementation uses read commands to access
%% metadata stored by in the storage state on behalf of the RAFT implementation
%% or to serve strong read requests issued by users.
%%-----------------------------------------------------------------------------

%% Apply a read command against the storage state, returning the result of
%% the read command.
-callback storage_read(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Handle :: storage_handle()) -> Result :: dynamic().

%% Apply a read command against the storage state to read the most recently
%% written value of the metadata with the provided key, if such a value exists.
-callback storage_read_metadata(Handle :: storage_handle(), Key :: metadata()) -> {ok, Version :: wa_raft_log:log_pos(), Value :: term()} | undefined | error().

%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Snapshots
%%-----------------------------------------------------------------------------
%% A snapshot is a representation of a storage state that can be saved to disk
%% and transmitted as a set of regular files to another RAFT replica using the
%% same storage provider and loaded to reproduce an identical storage state.
%%
%% Not exhaustively, the RAFT implementation uses snapshots to quickly rebuild
%% replicas that have fallen significantly behind in replication.
%%-----------------------------------------------------------------------------

%% Create a new snapshot of the current storage state at the provided path,
%% producing a directory tree rooted at the provided path that represents the
%% current storage state. The produced snapshot should retain the current
%% position when loaded.
-callback storage_create_snapshot(Path :: file:filename(), Handle :: storage_handle()) -> ok | error().

%% Load a snapshot previously created by the same storage provider, possibly
%% copied, rooted at the provided path. If successful, the current storage
%% state should be replaced by the storage state represented by the snapshot.
%% If a recoverable error occured, the storage state should remain unchanged.
%% If the storage state is no longer suitable for use, an error should be
%% raised.
-callback storage_open_snapshot(Path :: file:filename(), ExpectedPosition :: wa_raft_log:log_pos(), Handle :: storage_handle()) -> {ok, NewHandle :: storage_handle()} | error().

%%-----------------------------------------------------------------------------
%% RAFT Storage - Types
%%-----------------------------------------------------------------------------

-type metadata() :: config | atom().
-type storage_handle() :: dynamic().
-type error() :: {error, term()}.

-type status() :: [status_element()].
-type status_element() ::
      {name, atom()}
    | {table, wa_raft:table()}
    | {partition, wa_raft:partition()}
    | {module, module()}
    | {last_applied, wa_raft_log:log_index()}
    | ModuleSpecificStatus :: {atom(), term()}.

-record(state, {
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    root_dir :: file:filename(),
    module :: module(),
    handle :: storage_handle(),
    last_applied :: wa_raft_log:log_pos()
}).

%%-----------------------------------------------------------------------------
%% RAFT Storage - OTP Supervision
%%-----------------------------------------------------------------------------

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) -> {'ok', Pid::pid()} | 'ignore' | {'error', Reason::term()}.
start_link(#raft_options{storage_name = Name} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%%-----------------------------------------------------------------------------
%% RAFT Storage - Public API
%%-----------------------------------------------------------------------------

-spec status(ServiceRef :: pid() | atom()) -> status().
status(ServiceRef) ->
    gen_server:call(ServiceRef, status, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec apply_op(ServiceRef :: pid() | atom(), LogRecord :: {wa_raft_log:log_index(), {wa_raft_log:log_term(), {wa_raft_acceptor:key(), wa_raft_label:label() | undefined, wa_raft_acceptor:command()}}}, EffectiveTerm :: wa_raft_log:log_term() | undefined) -> ok.
apply_op(ServiceRef, LogRecord, ServerTerm) ->
    gen_server:cast(ServiceRef, {apply, LogRecord, ServerTerm}).

-spec read(ServiceRef :: pid() | atom(), Op :: wa_raft_acceptor:command()) -> ok.
read(ServiceRef, Op) ->
    gen_server:call(ServiceRef, {read, Op}).

-spec read(ServiceRef :: pid() | atom(), From :: gen_server:from(), Op :: wa_raft_acceptor:command()) -> ok.
read(ServiceRef, From, Op) ->
    gen_server:cast(ServiceRef, {read, From, Op}).

-spec cancel(ServiceRef :: pid() | atom()) -> ok.
cancel(ServiceRef) ->
    gen_server:cast(ServiceRef, cancel).

-spec open(ServiceRef :: pid() | atom()) -> {ok, LastApplied :: wa_raft_log:log_pos()}.
open(ServiceRef) ->
    gen_server:call(ServiceRef, open, ?RAFT_RPC_CALL_TIMEOUT()).

-spec open_snapshot(ServiceRef :: pid() | atom(), SnapshotPath :: file:filename(), LastAppliedPos :: wa_raft_log:log_pos()) -> ok | error().
open_snapshot(ServiceRef, SnapshotPath, LastAppliedPos) ->
    gen_server:call(ServiceRef, {snapshot_open, SnapshotPath, LastAppliedPos}, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec create_snapshot(ServiceRef :: pid() | atom()) -> {ok, Pos :: wa_raft_log:log_pos()} | error().
create_snapshot(ServiceRef) ->
    gen_server:call(ServiceRef, snapshot_create, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec create_snapshot(ServiceRef :: pid() | atom(), Name :: string()) -> ok | error().
create_snapshot(ServiceRef, Name) ->
    gen_server:call(ServiceRef, {snapshot_create, Name}, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec delete_snapshot(ServiceRef :: pid() | atom(), Name :: string()) -> ok.
delete_snapshot(ServiceRef, Name) ->
    gen_server:cast(ServiceRef, {snapshot_delete, Name}).

-spec read_metadata(ServiceRef :: pid() | atom(), Key :: metadata()) -> {ok, Version :: wa_raft_log:log_pos(), Value :: dynamic()} | undefined | error().
read_metadata(ServiceRef, Key) ->
    gen_server:call(ServiceRef, {read_metadata, Key}, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec label(ServiceRef :: pid() | atom()) -> wa_raft_label:label().
label(ServiceRef) ->
    gen_server:call(ServiceRef, label, ?RAFT_STORAGE_CALL_TIMEOUT()).

-ifdef(TEST).
-spec reset(ServiceRef :: pid() | atom(), Position :: wa_raft_log:log_pos(), Config :: wa_raft_server:config() | undefined) -> ok | error().
reset(ServiceRef, Position, Config) ->
    sys:replace_state(ServiceRef, fun (#state{module = Module, handle = Handle} = State) ->
        Config =/= undefined andalso
            Module:storage_write_metadata(Handle, config, Position, Config),
        State#state{last_applied = Position}
    end, ?RAFT_STORAGE_CALL_TIMEOUT()),
    ok.
-endif.

%%-------------------------------------------------------------------
%% RAFT Storage - Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT storage server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_storage_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT storage server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.storage_name
    end.

%%-------------------------------------------------------------------
%% RAFT Storage - Server Callbacks
%%-------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> {ok, #state{}}.
init(#raft_options{application = App, table = Table, partition = Partition, database = RootDir, storage_name = Name, storage_module = Module}) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Storage[~0p] starting for partition ~0p/~0p at ~0p using ~0p",
        [Name, Table, Partition, RootDir, Module], #{domain => [whatsapp, wa_raft]}),

    Handle = Module:storage_open(Name, #raft_identifier{application = App, table = Table, partition = Partition}, RootDir),
    LastApplied = Module:storage_position(Handle),

    ?LOG_NOTICE("Storage[~0p] opened at position ~0p.",
        [Name, LastApplied], #{domain => [whatsapp, wa_raft]}),

    {ok, #state{
        name = Name,
        table = Table,
        partition = Partition,
        root_dir = RootDir,
        module = Module,
        handle = Handle,
        last_applied = LastApplied
    }}.

%% The interaction between the RAFT server and the RAFT storage server is designed to be
%% as asynchronous as possible since the RAFT storage server may be caught up in handling
%% a long running I/O request while it is working on applying new log entries.
%% If you are adding a new call to the RAFT storage server, make sure that it is either
%% guaranteed to not be used when the storage server is busy (and may not reply in time)
%% or timeouts and other failures are handled properly.
-spec handle_call(Request, From :: gen_server:from(), State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {noreply, NewState :: #state{}} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}}
    when Request ::
        open |
        {read, Op :: wa_raft_acceptor:command()} |
        snapshot_create |
        status |
        {snapshot_create, Name :: string()} |
        {snapshot_open, Path :: file:filename(), LastAppliedPos :: wa_raft_log:log_pos()} |
        {read_metadata, Key :: metadata()} |
        label.
handle_call(open, _From, #state{last_applied = LastApplied} = State) ->
    {reply, {ok, LastApplied}, State};

handle_call({read, Command}, _From, #state{module = Module, handle = Handle, last_applied = Position} = State) ->
    {reply, Module:storage_read(Command, Position, Handle), State};

handle_call(snapshot_create, _From, #state{last_applied = #raft_log_pos{index = LastIndex, term = LastTerm}} = State) ->
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

handle_call({snapshot_open, SnapshotPath, LogPos}, _From, #state{name = Name, module = Module, handle = Handle, last_applied = LastApplied} = State) ->
    ?LOG_NOTICE("Storage[~0p] replacing storage at ~0p with snapshot at ~0p.", [Name, LastApplied, LogPos], #{domain => [whatsapp, wa_raft]}),
    case Module:storage_open_snapshot(SnapshotPath, LogPos, Handle) of
        {ok, NewHandle} -> {reply, ok, State#state{last_applied = LogPos, handle = NewHandle}};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;

handle_call({read_metadata, Key}, _From, #state{module = Module, handle = Handle} = State) ->
    ?RAFT_COUNT('raft.storage.read_metadata'),
    Result = Module:storage_read_metadata(Handle, Key),
    {reply, Result, State};

handle_call(label, _From, #state{module = Module, handle = Handle} = State) ->
    ?RAFT_COUNT('raft.storage.label'),
    Result = Module:storage_label(Handle),
    {reply, Result, State};

handle_call(status, _From, #state{module = Module, handle = Handle} = State) ->
    BaseStatus = [
        {name, State#state.name},
        {table, State#state.table},
        {partition, State#state.partition},
        {module, State#state.module},
        {last_applied, State#state.last_applied#raft_log_pos.index}
    ],
    ModuleStatus = case erlang:function_exported(Module, storage_status, 1) of
        true  -> Module:storage_status(Handle);
        false -> []
    end,
    {reply, BaseStatus ++ ModuleStatus, State};

handle_call(Cmd, From, #state{name = Name} = State) ->
    ?LOG_WARNING("[~p] unexpected call ~p from ~p", [Name, Cmd, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request, State :: #state{}) -> {noreply, NewState :: #state{}}
    when Request ::
        cancel |
        {fulfill, term(), term()} |
        {read, gen_server:from(), wa_raft_acceptor:command()} |
        {apply, LogRecord :: wa_raft_log:log_record(), EffectiveTerm :: wa_raft_log:log_term() | undefined} |
        {snapshot_delete, Name :: string()}.
handle_cast(cancel, #state{name = Name, table = Table, partition = Partition} = State) ->
    ?LOG_NOTICE("[~p] cancel pending commits and reads", [Name], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_all_commits(Table, Partition, {error, not_leader}),
    wa_raft_queue:fulfill_all_reads(Table, Partition, {error, not_leader}),
    {noreply, State};

handle_cast({read, From, Command}, #state{module = Module, handle = Handle, last_applied = Position} = State) ->
    gen_server:reply(From, Module:storage_read(Command, Position, Handle)),
    {noreply, State};

% Apply an op after consensus is made
handle_cast({apply, {LogIndex, {LogTerm, _Op}} = LogRecord, EffectiveTerm}, #state{name = Name} = State0) ->
    ?LOG_DEBUG("[~p] apply ~p:~p", [Name, LogIndex, LogTerm], #{domain => [whatsapp, wa_raft]}),
    State1 = apply_impl(LogRecord, EffectiveTerm, State0),
    {noreply, State1};

handle_cast({snapshot_delete, SnapName}, #state{name = Name, root_dir = RootDir} = State) ->
    Result = catch file:del_dir_r(filename:join(RootDir, SnapName)),
    ?LOG_NOTICE("~100p delete snapshot ~p. result ~p", [Name, SnapName, Result], #{domain => [whatsapp, wa_raft]}),
    {noreply, State};

handle_cast(Cmd, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Cmd], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_info(Command, State) ->
    ?LOG_WARNING("Unexpected info ~p", [Command], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> term().
terminate(Reason, #state{name = Name, module = Module, handle = Handle, last_applied = LastApplied}) ->
    ?LOG_NOTICE("Storage[~0p] terminating at ~0p with reason ~0p.", [Name, LastApplied, Reason], #{domain => [whatsapp, wa_raft]}),
    Module:storage_close(Handle).

-spec code_change(_OldVsn :: term(), State :: #state{}, Extra :: term()) -> {ok, State :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-------------------------------------------------------------------
%% RAFT Storage - Implementation
%%-------------------------------------------------------------------

-spec apply_impl(Record :: wa_raft_log:log_record(), EffectiveTerm :: wa_raft_log:log_term() | undefined, State :: #state{}) -> NewState :: #state{}.
apply_impl({LogIndex, {LogTerm, {Reference, Label, Command} = Op}}, EffectiveTerm,
           #state{name = Name, table = Table, partition = Partition, last_applied = #raft_log_pos{index = LastAppliedIndex}} = State0) ->
    wa_raft_queue:fulfill_apply(Table, Partition),
    StartT = os:timestamp(),
    case LogIndex of
        LastAppliedIndex ->
            apply_delayed_reads(State0);
        _ when LogIndex =:= LastAppliedIndex + 1 ->
            ?RAFT_COUNT('raft.storage.apply'),
            {Reply, State1} = execute(Command, #raft_log_pos{index = LogIndex, term = LogTerm}, Label, State0),
            LogTerm =:= EffectiveTerm andalso
                wa_raft_queue:fulfill_commit(Table, Partition, Reference, Reply),
            State2 = State1#state{last_applied = #raft_log_pos{index = LogIndex, term = LogTerm}},
            State3 = apply_delayed_reads(State2),
            ?LOG_DEBUG("applied ~p:~p", [LogIndex, LogTerm], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_GATHER('raft.storage.apply.func', timer:now_diff(os:timestamp(), StartT)),
            State3;
        _ ->
            ?LOG_ERROR("[~p] received out-of-order apply with index ~p. (expected index ~p, op ~0P)", [Name, LogIndex, LastAppliedIndex, Op, 30], #{domain => [whatsapp, wa_raft]}),
            error(out_of_order_apply)
    end.

-spec execute(Command :: wa_raft_acceptor:command(), LogPos :: wa_raft_log:log_pos(), Label :: wa_raft_label:label(), State :: #state{}) -> {term() | error(), #state{}}.
execute(noop, LogPos, undefined, #state{module = Module, handle = Handle} = State) ->
    {Reply, NewHandle} = Module:storage_apply(noop, LogPos, Handle),
    {Reply, State#state{handle = NewHandle}};
execute(noop, LogPos, Label, #state{module = Module, handle = Handle} = State) ->
    {Reply, NewHandle} = Module:storage_apply(noop, LogPos, Label, Handle),
    {Reply, State#state{handle = NewHandle}};
execute({config, Config}, #raft_log_pos{index = Index, term = Term} = Version, _Label, #state{name = Name, module = Module, handle = Handle} = State) ->
    ?LOG_INFO("Storage[~p] applying new configuration ~p at ~p:~p.",
        [Name, Config, Index, Term], #{domain => [whatsapp, wa_raft]}),
    {Module:storage_write_metadata(Handle, config, Version, Config), State};
execute(Command, LogPos, undefined, #state{module = Module, handle = Handle} = State) ->
    {Reply, NewHandle} = Module:storage_apply(Command, LogPos, Handle),
    {Reply, State#state{handle = NewHandle}};
execute(Command, LogPos, Label, #state{module = Module, handle = Handle} = State) ->
    {Reply, NewHandle} = Module:storage_apply(Command, LogPos, Label, Handle),
    {Reply, State#state{handle = NewHandle}}.

-spec apply_delayed_reads(State :: #state{}) -> NewState :: #state{}.
apply_delayed_reads(#state{table = Table, partition = Partition, module = Module, handle = Handle, last_applied = #raft_log_pos{index = LastAppliedIndex} = LastAppliedLogPos} = State) ->
    lists:foreach(
        fun ({Reference, Command}) ->
            Reply = Module:storage_read(Command, LastAppliedLogPos, Handle),
            wa_raft_queue:fulfill_read(Table, Partition, Reference, Reply)
        end, wa_raft_queue:query_reads(Table, Partition, LastAppliedIndex)),
    State.

-spec create_snapshot_impl(SnapName :: string(), Storage :: #state{}) -> ok | error().
create_snapshot_impl(SnapName, #state{name = Name, root_dir = RootDir, module = Module, handle = Handle} = State) ->
    SnapshotPath = filename:join(RootDir, SnapName),
    case filelib:is_dir(SnapshotPath) of
        true ->
            ?LOG_NOTICE("Snapshot ~s for ~p already exists. Skipping snapshot creation.", [SnapName, Name], #{domain => [whatsapp, wa_raft]}),
            ok;
        false ->
            cleanup_snapshots(State),
            ?LOG_NOTICE("Create snapshot ~s for ~p.", [SnapName, Name], #{domain => [whatsapp, wa_raft]}),
            Module:storage_create_snapshot(SnapshotPath, Handle)
    end.

-define(MAX_RETAINED_SNAPSHOT, 1).

-spec cleanup_snapshots(#state{}) -> ok.
cleanup_snapshots(#state{root_dir = RootDir}) ->
    Snapshots = list_snapshots(RootDir),
    case length(Snapshots) > ?MAX_RETAINED_SNAPSHOT of
        true ->
            lists:foreach(
                fun ({_, Name}) ->
                    SnapshotPath = filename:join(RootDir, Name),
                    ?LOG_NOTICE("Removing snapshot \"~s\".", [SnapshotPath], #{domain => [whatsapp, wa_raft]}),
                    file:del_dir_r(SnapshotPath)
                end, lists:sublist(Snapshots, length(Snapshots) - ?MAX_RETAINED_SNAPSHOT)),
            ok;
        _ ->
            ok
    end.

%% Private functions
-spec list_snapshots(RootDir :: string()) -> [{wa_raft_log:log_pos(), file:filename()}].
list_snapshots(RootDir) ->
    Dirs = filelib:wildcard(?SNAPSHOT_PREFIX ++ ".*", RootDir),
    Snapshots = lists:filtermap(fun decode_snapshot_name/1, Dirs),
    lists:keysort(1, Snapshots).

-spec decode_snapshot_name(Name :: string()) -> {true, {wa_raft_log:log_pos(), file:filename()}} | false.
decode_snapshot_name(Name) ->
    case string:lexemes(Name, ".") of
        [?SNAPSHOT_PREFIX, IndexStr, TermStr] ->
            case {list_to_integer(IndexStr), list_to_integer(TermStr)} of
                {Index, Term} when Index >= 0 andalso Term >= 0 ->
                    {true, {#raft_log_pos{index = Index, term = Term}, Name}};
                _ ->
                    ?LOG_WARNING("Invalid snapshot with invalid index (~p) and/or term (~p). (full name ~p)", [IndexStr, TermStr, Name], #{domain => [whatsapp, wa_raft]}),
                    false
            end;
        _ ->
            ?LOG_WARNING("Invalid snapshot dir name ~p", [Name], #{domain => [whatsapp, wa_raft]}),
            false
    end.
