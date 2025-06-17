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

%% Public API
-export([
    status/1,
    position/1,
    label/1,
    config/1,
    read/2
]).

%% Internal API
-export([
    open/1,
    cancel/1,
    apply/4,
    apply_read/3
]).

%% Internal API
-export([
    open_snapshot/3,
    create_snapshot/1,
    create_snapshot/2,
    create_witness_snapshot/1,
    create_witness_snapshot/2,
    delete_snapshot/2,
    make_empty_snapshot/5
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
    terminate/2
]).

-export_type([
    storage_handle/0,
    metadata/0,
    status/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

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
-callback storage_open(Options :: #raft_options{}, Path :: file:filename()) -> Handle :: storage_handle().

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
-callback storage_label(Handle :: storage_handle()) -> {ok, Label :: wa_raft_label:label()} | {error, Reason :: term()}.
-optional_callbacks([storage_label/1]).


%% Issue a read command to get the config of the current storage state.
-callback storage_config(Handle :: storage_handle()) -> {ok, Version :: wa_raft_log:log_pos(), Config :: wa_raft_server:config()} | undefined.

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

%% Apply a write command to update the raft config stored by the storage provider
%% on behalf of the RAFT implementation. Subsequent calls to read the config
%% should return the updated version and value.
%% If the command could not be applied in a manner so as to preserve the
%% desired consistency guarantee then implementations can raise an error to be
%% aborted safely.
-callback storage_apply_config(Config :: wa_raft_server:config(), Position :: wa_raft_log:log_pos(), Handle :: storage_handle()) -> {Result :: ok | {error, Reason :: term()}, NewHandle :: storage_handle()}.

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
%% Implicitly, use of the `storage_position/1` callback is non-synchronized
%% access of the storage state and should be considered to be read commands.
%%
%% Not exhaustively, the RAFT implementation uses read commands to access
%% metadata stored by in the storage state on behalf of the RAFT implementation
%% or to serve strong read requests issued by users.
%%-----------------------------------------------------------------------------

%% Apply a read command against the storage state, returning the result of
%% the read command.
-callback storage_read(Command :: wa_raft_acceptor:command(), Position :: wa_raft_log:log_pos(), Handle :: storage_handle()) -> Result :: dynamic().

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
-callback storage_create_snapshot(Path :: file:filename(), Handle :: storage_handle()) -> ok | {error, Reason :: term()}.

%% Create a new witness snapshot at the provided path which must contain the current
%% position in storage and configuration.
%% The snapshot will be empty (without actual storage data) but will retain all
%% necessary metadata. When loaded, this witness snapshot will reflect the exact
%% position state of the original storage without the storage contents.
-callback storage_create_witness_snapshot(Path :: file:filename(), Handle :: storage_handle()) -> ok | {error, Reason :: term()}.
-optional_callback([storage_create_witness_snapshot/2]).

%% Load a snapshot previously created by the same storage provider, possibly
%% copied, rooted at the provided path. If successful, the current storage
%% state should be replaced by the storage state represented by the snapshot.
%% If a recoverable error occured, the storage state should remain unchanged.
%% If the storage state is no longer suitable for use, an error should be
%% raised.
-callback storage_open_snapshot(Path :: file:filename(), ExpectedPosition :: wa_raft_log:log_pos(), Handle :: storage_handle()) -> {ok, NewHandle :: storage_handle()} | {error, Reason :: term()}.

%%-----------------------------------------------------------------------------
%% RAFT Storage Provider - Bootstrapping
%%-----------------------------------------------------------------------------

%% Create a new snapshot at the provided path that contains some directory
%% tree that when subsequently loaded using `storage_open_snapshot` results in
%% a storage state with the provided last applied position and for which
%% subsequent calls to `storage_config` returns the provided position as the
%% version and the config as the value. Extra data may be used by implementors
%% to provide extra state via arguments to external APIs that use this
%% endpoint, such as the partition bootstrapping API.
-callback storage_make_empty_snapshot(Options :: #raft_options{}, Path :: file:filename(), Position :: wa_raft_log:log_pos(), Config :: wa_raft_server:config(), Data :: dynamic()) -> ok | {error, Reason :: term()}.
-optional_callback([storage_make_empty_snapshot/5]).

%%-----------------------------------------------------------------------------
%% RAFT Storage - Types
%%-----------------------------------------------------------------------------

-type metadata() :: config | atom().
-type storage_handle() :: dynamic().

-type status() :: [status_element()].
-type status_element() ::
      {name, atom()}
    | {table, wa_raft:table()}
    | {partition, wa_raft:partition()}
    | {module, module()}
    | {last_applied, wa_raft_log:log_index()}
    | ModuleSpecificStatus :: {atom(), term()}.

-record(state, {
    application :: atom(),
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    self :: #raft_identity{},
    options :: #raft_options{},
    path :: file:filename(),
    server :: atom(),
    queues :: wa_raft_queue:queues(),
    module :: module(),
    handle :: storage_handle(),
    position :: wa_raft_log:log_pos(),
    config :: undefined | {ok, wa_raft_log:log_pos(), wa_raft_server:config()},
    witness = false :: boolean(),
    skipped = 0 :: non_neg_integer()
}).

%%-----------------------------------------------------------------------------
%% RAFT Storage - Private Types
%%-----------------------------------------------------------------------------

-define(STATUS_REQUEST, status).
-define(POSITION_REQUEST, position).
-define(LABEL_REQUEST, label).
-define(CONFIG_REQUEST, config).

-define(READ_REQUEST(Command), {read, Command}).

-define(OPEN_REQUEST, open).
-define(CANCEL_REQUEST, cancel).
-define(FULFILL_REQUEST(Key, Result), {fulfill, Key, Result}).
-define(APPLY_REQUEST(Record, Size, EffectiveTerm), {apply, Record, Size, EffectiveTerm}).
-define(APPLY_READ_REQUEST(From, Command), {apply_read, From, Command}).

-define(CREATE_SNAPSHOT_REQUEST(), create_snapshot).
-define(CREATE_SNAPSHOT_REQUEST(Name), {create_snapshot, Name}).
-define(CREATE_WITNESS_SNAPSHOT_REQUEST(), create_witness_snapshot).
-define(CREATE_WITNESS_SNAPSHOT_REQUEST(Name), {create_witness_snapshot, Name}).
-define(OPEN_SNAPSHOT_REQUEST(Path, Position), {open_snapshot, Path, Position}).
-define(DELETE_SNAPSHOT_REQUEST(Name), {delete_snapshot, Name}).

-define(MAKE_EMPTY_SNAPSHOT_REQUEST(Path, Position, Config, Data), {make_empty_snapshot, Path, Position, Config, Data}).

-type call() :: status_request() | position_request() | label_request() | config_request() | read_request() |
                open_request() | create_snapshot_request() | create_witness_snapshot_request() |
                open_snapshot_request() | make_empty_snapshot_request().
-type cast() :: cancel_request() | fulfill_request() | apply_request() | apply_read_request() | delete_snapshot_request().

-type status_request() :: ?STATUS_REQUEST.
-type position_request() :: ?POSITION_REQUEST.
-type label_request() :: ?LABEL_REQUEST.
-type config_request() :: ?CONFIG_REQUEST.

-type read_request() :: ?READ_REQUEST(Command :: wa_raft_acceptor:command()).

-type open_request() :: ?OPEN_REQUEST.
-type cancel_request() :: ?CANCEL_REQUEST.
-type fulfill_request() :: ?FULFILL_REQUEST(Key :: wa_raft_acceptor:key(), Result :: wa_raft_acceptor:commit_result()).
-type apply_request() :: ?APPLY_REQUEST(Record :: wa_raft_log:log_record(), Size :: non_neg_integer(), EffectiveTerm :: wa_raft_log:log_term() | undefined).
-type apply_read_request() :: ?APPLY_READ_REQUEST(From :: gen_server:from(), Comman :: wa_raft_acceptor:command()).

-type create_snapshot_request() :: ?CREATE_SNAPSHOT_REQUEST() | ?CREATE_SNAPSHOT_REQUEST(Name :: string()).
-type create_witness_snapshot_request() :: ?CREATE_WITNESS_SNAPSHOT_REQUEST() | ?CREATE_WITNESS_SNAPSHOT_REQUEST(Name :: string()).
-type open_snapshot_request() :: ?OPEN_SNAPSHOT_REQUEST(Path :: string(), Position :: wa_raft_log:log_pos()).
-type delete_snapshot_request() :: ?DELETE_SNAPSHOT_REQUEST(Name :: string()).

-type make_empty_snapshot_request() :: ?MAKE_EMPTY_SNAPSHOT_REQUEST(Path :: string(), Position :: wa_raft_log:log_pos(), Config :: wa_raft_server:config(), Data :: dynamic()).

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

-spec status(Storage :: gen_server:server_ref()) -> status().
status(Storage) ->
    gen_server:call(Storage, ?STATUS_REQUEST, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec position(Storage :: gen_server:server_ref()) -> Position :: wa_raft_log:log_pos().
position(Storage) ->
    gen_server:call(Storage, ?POSITION_REQUEST, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec label(Storage :: gen_server:server_ref()) -> {ok, Label :: wa_raft_label:label()} | {error, Reason :: term()}.
label(Storage) ->
    gen_server:call(Storage, ?LABEL_REQUEST, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec config(Storage :: gen_server:server_ref()) -> {ok, wa_raft_log:log_pos(), wa_raft_server:config()} | undefined | {error, Reason :: term()}.
config(Storage) ->
    gen_server:call(Storage, ?CONFIG_REQUEST, ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec read(Storage :: gen_server:server_ref(), Command :: wa_raft_acceptor:command()) -> ok.
read(Storage, Command) ->
    gen_server:call(Storage, ?READ_REQUEST(Command), ?RAFT_STORAGE_CALL_TIMEOUT()).

%%-----------------------------------------------------------------------------
%% RAFT Storage - Internal API
%%-----------------------------------------------------------------------------

-spec open(Storage :: gen_server:server_ref()) -> {ok, LastApplied :: wa_raft_log:log_pos()}.
open(Storage) ->
    gen_server:call(Storage, ?OPEN_REQUEST, ?RAFT_RPC_CALL_TIMEOUT()).

-spec cancel(Storage :: gen_server:server_ref()) -> ok.
cancel(Storage) ->
    gen_server:cast(Storage, ?CANCEL_REQUEST).

-spec apply(
    Storage :: gen_server:server_ref(),
    Record :: wa_raft_log:log_record(),
    Size :: non_neg_integer(),
    EffectiveTerm :: wa_raft_log:log_term() | undefined
) -> ok.
apply(Storage, Record, Size, EffectiveTerm) ->
    gen_server:cast(Storage, ?APPLY_REQUEST(Record, Size, EffectiveTerm)).

-spec apply_read(Storage :: gen_server:server_ref(), From :: gen_server:from(), Command :: wa_raft_acceptor:command()) -> ok.
apply_read(Storage, From, Command) ->
    gen_server:cast(Storage, ?APPLY_READ_REQUEST(From, Command)).

-spec open_snapshot(Storage :: gen_server:server_ref(), Path :: file:filename(), Position :: wa_raft_log:log_pos()) -> ok | {error, Reason :: term()}.
open_snapshot(Storage, Path, Position) ->
    gen_server:call(Storage, ?OPEN_SNAPSHOT_REQUEST(Path, Position), ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec create_snapshot(Storage :: gen_server:server_ref()) -> {ok, Pos :: wa_raft_log:log_pos()} | {error, Reason :: term()}.
create_snapshot(Storage) ->
    gen_server:call(Storage, ?CREATE_SNAPSHOT_REQUEST(), ?RAFT_STORAGE_CALL_TIMEOUT()).

%% Be careful when using the same name for two snapshots as the RAFT storage
%% server will not recreate an existing snapshot even if the storage state has
%% advanced since the snapshot was created; however, this method will always
%% return the current position upon success.
-spec create_snapshot(Storage :: gen_server:server_ref(), Name :: string()) -> {ok, Pos :: wa_raft_log:log_pos()} | {error, Reason :: term()}.
create_snapshot(Storage, Name) ->
    gen_server:call(Storage, ?CREATE_SNAPSHOT_REQUEST(Name), ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec create_witness_snapshot(Storage :: gen_server:server_ref()) -> {ok, Pos :: wa_raft_log:log_pos()} | {error, Reason :: term()}.
create_witness_snapshot(Storage) ->
    gen_server:call(Storage, ?CREATE_WITNESS_SNAPSHOT_REQUEST(), ?RAFT_STORAGE_CALL_TIMEOUT()).

%% Be careful when using the same name for two snapshots as the RAFT storage
%% server will not recreate an existing snapshot even if the storage state has
%% advanced since the snapshot was created; however, this method will always
%% return the current position upon success.
-spec create_witness_snapshot(Storage :: gen_server:server_ref(), Name :: string()) -> {ok, Pos :: wa_raft_log:log_pos()} | {error, Reason :: term()}.
create_witness_snapshot(Storage, Name) ->
    gen_server:call(Storage, ?CREATE_WITNESS_SNAPSHOT_REQUEST(Name), ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec delete_snapshot(Storage :: gen_server:server_ref(), Name :: string()) -> ok.
delete_snapshot(Storage, Name) ->
    gen_server:cast(Storage, ?DELETE_SNAPSHOT_REQUEST(Name)).

-spec make_empty_snapshot(Storage :: gen_server:server_ref(), Path :: file:filename(), Position :: wa_raft_log:log_pos(), Config :: wa_raft_server:config(), Data :: term()) -> ok | {error, Reason :: term()}.
make_empty_snapshot(Storage, Path, Position, Config, Data) ->
    gen_server:call(Storage, ?MAKE_EMPTY_SNAPSHOT_REQUEST(Path, Position, Config, Data), ?RAFT_STORAGE_CALL_TIMEOUT()).

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
init(#raft_options{application = Application, table = Table, partition = Partition, self = Self, database = Path, server_name = Server, storage_name = Name, storage_module = Module} = Options) ->
    process_flag(trap_exit, true),

    % This increases the potential overhead of sending log entries to storage
    % to be applied; however, can protect the storage server from GC overhead
    % and other memory-related issues.
    process_flag(message_queue_data, off_heap),

    ?LOG_NOTICE("Storage[~0p] starting for partition ~0p/~0p at ~0p using ~0p",
        [Name, Table, Partition, Path, Module], #{domain => [whatsapp, wa_raft]}),

    Handle = Module:storage_open(Options, Path),
    Position = Module:storage_position(Handle),

    ?LOG_NOTICE("Storage[~0p] opened at position ~0p.",
        [Name, Position], #{domain => [whatsapp, wa_raft]}),

    State = #state{
        application = Application,
        name = Name,
        table = Table,
        partition = Partition,
        self = Self,
        options = Options,
        path = Path,
        server = Server,
        queues = wa_raft_queue:queues(Options),
        module = Module,
        handle = Handle,
        position = Position
    },
    {ok, refresh_config(State)}.

%% The interaction between the RAFT server and the RAFT storage server is designed to be
%% as asynchronous as possible since the RAFT storage server may be caught up in handling
%% a long running I/O request while it is working on applying new log entries.
%% If you are adding a new call to the RAFT storage server, make sure that it is either
%% guaranteed to not be used when the storage server is busy (and may not reply in time)
%% or timeouts and other failures are handled properly.
-spec handle_call(Request :: call(), From :: gen_server:from(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}}.

handle_call(?STATUS_REQUEST, _From, #state{module = Module, handle = Handle} = State) ->
    BaseStatus = [
        {name, State#state.name},
        {table, State#state.table},
        {partition, State#state.partition},
        {module, State#state.module},
        {last_applied, State#state.position#raft_log_pos.index}
    ],
    ModuleStatus = case erlang:function_exported(Module, storage_status, 1) of
        true  -> Module:storage_status(Handle);
        false -> []
    end,
    {reply, BaseStatus ++ ModuleStatus, State};

handle_call(?POSITION_REQUEST, _From, #state{module = Module, handle = Handle} = State) ->
    ?RAFT_COUNT('raft.storage.position'),
    Result = Module:storage_position(Handle),
    {reply, Result, State};

handle_call(?LABEL_REQUEST, _From, #state{module = Module, handle = Handle} = State) ->
    ?RAFT_COUNT('raft.storage.label'),
    Result = Module:storage_label(Handle),
    {reply, Result, State};

handle_call(?CONFIG_REQUEST, _From, #state{config = Config} = State) ->
    {reply, Config, State};

handle_call(?READ_REQUEST(Command), _From, #state{module = Module, handle = Handle, position = Position} = State) ->
    {reply, Module:storage_read(Command, Position, Handle), State};

handle_call(?OPEN_REQUEST, _From, #state{position = Position} = State) ->
    {reply, {ok, Position}, State};

handle_call(?OPEN_SNAPSHOT_REQUEST(SnapshotPath, SnapshotPosition), _From, #state{name = Name, module = Module, handle = Handle, position = Position} = State) ->
    ?LOG_NOTICE("Storage[~0p] at ~0p is opening snapshot ~0p.", [Name, Position, SnapshotPosition], #{domain => [whatsapp, wa_raft]}),
    case Module:storage_open_snapshot(SnapshotPath, SnapshotPosition, Handle) of
        {ok, NewHandle} ->
            {reply, ok, refresh_config(State#state{position = SnapshotPosition, handle = NewHandle})};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(?CREATE_SNAPSHOT_REQUEST(), _From, #state{position = #raft_log_pos{index = Index, term = Term}} = State) ->
    Name = ?SNAPSHOT_NAME(Index, Term),
    {reply, handle_create_snapshot(Name, State), State};

handle_call(?CREATE_SNAPSHOT_REQUEST(Name), _From, #state{} = State) ->
    {reply, handle_create_snapshot(Name, State), State};

handle_call(?CREATE_WITNESS_SNAPSHOT_REQUEST(), _From, #state{position = #raft_log_pos{index = Index, term = Term}} = State) ->
    Name = ?WITNESS_SNAPSHOT_NAME(Index, Term),
    {reply, handle_create_witness_snapshot(Name, State), State};

handle_call(?CREATE_WITNESS_SNAPSHOT_REQUEST(Name), _From, #state{} = State) ->
    {reply, handle_create_witness_snapshot(Name, State), State};

handle_call(?MAKE_EMPTY_SNAPSHOT_REQUEST(SnapshotPath, SnapshotPosition, Config, Data), _From, #state{name = Name, options = Options, module = Module} = State) ->
    ?LOG_NOTICE("Storage[~0p] making bootstrap snapshot ~0p at ~0p with config ~0p and data ~0P.",
        [Name, SnapshotPath, SnapshotPosition, Config, Data, 30], #{domain => [whatsapp, wa_raft]}),
    case erlang:function_exported(Module, storage_make_empty_snapshot, 5) of
        true -> {reply, Module:storage_make_empty_snapshot(Options, SnapshotPath, SnapshotPosition, Config, Data), State};
        false -> {reply, {error, not_supported}, State}
    end;

handle_call(Request, From, #state{name = Name} = State) ->
    ?LOG_WARNING("Storage[~0p] received unexpected call ~0P from ~0p.",
        [Name, Request, 20, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request :: cast(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.

handle_cast(?CANCEL_REQUEST, #state{name = Name, queues = Queues} = State) ->
    ?LOG_NOTICE("Storage[~0p] cancels all pending commits and reads.", [Name], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_all_commits(Queues, {error, not_leader}),
    wa_raft_queue:fulfill_all_reads(Queues, {error, not_leader}),
    {noreply, State};

handle_cast(?APPLY_REQUEST({LogIndex, {LogTerm, {Reference, Label, Command}}}, Size, EffectiveTerm), #state{name = Name, queues = Queues} = State0) ->
    wa_raft_queue:fulfill_apply(Queues, Size),
    LogPosition = #raft_log_pos{index = LogIndex, term = LogTerm},
    ?LOG_DEBUG("Storage[~0p] is starting to apply ~0p", [Name, LogPosition], #{domain => [whatsapp, wa_raft]}),
    {noreply, handle_apply(LogPosition, Reference, Label, Command, EffectiveTerm, State0)};

handle_cast(?APPLY_READ_REQUEST(From, Command), #state{module = Module, handle = Handle, position = Position} = State) ->
    gen_server:reply(From, Module:storage_read(Command, Position, Handle)),
    {noreply, State};

handle_cast(?DELETE_SNAPSHOT_REQUEST(SnapshotName), #state{name = Name, path = Path} = State) ->
    Result = catch file:del_dir_r(filename:join(Path, SnapshotName)),
    ?LOG_NOTICE("Storage[~0p] deletes snapshot ~0p: ~0P.",
        [Name, SnapshotName, Result, 20], #{domain => [whatsapp, wa_raft]}),
    {noreply, State};

handle_cast(Request, #state{name = Name} = State) ->
    ?LOG_WARNING("Storage[~0p] received unexpected cast ~0P.",
        [Name, Request, 20], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> term().
terminate(Reason, #state{name = Name, module = Module, handle = Handle, position = Position}) ->
    ?LOG_NOTICE("Storage[~0p] terminating at ~0p with reason ~0P.",
        [Name, Position, Reason, 30], #{domain => [whatsapp, wa_raft]}),
    Module:storage_close(Handle).

%%-------------------------------------------------------------------
%% RAFT Storage - Implementation
%%-------------------------------------------------------------------

-spec handle_apply(
    LogPosition :: wa_raft_log:log_pos(),
    Reference :: wa_raft_acceptor:key(),
    Label :: wa_raft_label:label(),
    Command :: wa_raft_acceptor:command(),
    EffectiveTerm :: wa_raft_log:log_term() | undefined,
    State :: #state{}
) -> NewState :: #state{}.
%% In the case that a log entry is reapplied, fulfill any new pending reads at that index.
handle_apply(#raft_log_pos{index = LogIndex},
    _Reference,
    _Label,
    _Command,
    _EffectiveTerm,
    #state{position = #raft_log_pos{index = Index}} = State
) when LogIndex =:= Index ->
    handle_delayed_reads(State),
    State;
%% Issue an apply request to storage when the next log entry is to be applied,
%% and respond if the current effect term is equal to the log entry's term.
handle_apply(
    #raft_log_pos{index = LogIndex, term = LogTerm} = LogPosition,
    Reference,
    Label,
    Command,
    EffectiveTerm,
    #state{
        application = Application,
        name = Name,
        server = Server,
        queues = Queues,
        position = #raft_log_pos{index = Index}
    } = State
) when LogIndex =:= Index + 1 ->
    ?RAFT_COUNT('raft.storage.apply'),
    StartT = os:timestamp(),
    {Reply, NewState} = handle_command(Label, Command, LogPosition, State),
    LogTerm =:= EffectiveTerm andalso
        wa_raft_queue:fulfill_commit(Queues, Reference, Reply),
    handle_delayed_reads(NewState),
    wa_raft_queue:apply_queue_size(Queues) =:= 0 andalso ?RAFT_STORAGE_NOTIFY_COMPLETE(Application) andalso
        wa_raft_server:notify_complete(Server),
    ?LOG_DEBUG("Storage[~0p] finishes applying ~0p.",
        [Name, LogPosition], #{domain => [whatsapp, wa_raft]}),
    ?RAFT_GATHER('raft.storage.apply.func', timer:now_diff(os:timestamp(), StartT)),
    NewState;
%% Otherwise, the apply is out of order.
handle_apply(LogPosition, _Reference, _Label, _Command, _EffectiveTerm, #state{name = Name, position = Position}) ->
    ?LOG_ERROR("Storage[~0p] at ~0p received an out-of-order operation at ~0p.",
        [Name, Position, LogPosition], #{domain => [whatsapp, wa_raft]}),
    error(out_of_order_apply).

-spec handle_command(
    Label :: wa_raft_label:label(),
    Command :: wa_raft_acceptor:command(),
    Position :: wa_raft_log:log_pos(),
    State :: #state{}
) -> {Result :: term(), #state{}}.
handle_command(Label, noop = Command, Position, #state{} = State) ->
    handle_command_impl(Label, Command, Position, State);
handle_command(_Label, {config, Config}, Position, #state{name = Name, module = Module, handle = Handle} = State) ->
    ?LOG_INFO("Storage[~0p] is applying a new configuration ~0p at ~0p.",
        [Name, Config, Position], #{domain => [whatsapp, wa_raft]}),
    {Reply, NewHandle} = Module:storage_apply_config(Config, Position, Handle),
    {Reply, refresh_config(State#state{handle = NewHandle, position = Position})};
handle_command(Label, _Command, Position, #state{application = Application, witness = true, skipped = Skipped} = State) ->
    case Skipped >= ?RAFT_STORAGE_WITNESS_APPLY_INTERVAL(Application) of
        true ->
            {Reply, NewState} = handle_command_impl(Label, noop_omitted, Position, State),
            {Reply, NewState#state{skipped = 0}};
        false ->
            {ok, State#state{position = Position, skipped = Skipped + 1}}
    end;
handle_command(Label, Command, Position, #state{} = State) ->
    handle_command_impl(Label, Command, Position, State).

-spec handle_command_impl(
    Label :: wa_raft_label:label(),
    Command :: wa_raft_acceptor:command(),
    Position :: wa_raft_log:log_pos(),
    State :: #state{}
) -> {Result :: term(), #state{}}.
handle_command_impl(Label, Command, Position, #state{module = Module, handle = Handle} = State) ->
    {Reply, NewHandle} = case Label of
        undefined -> Module:storage_apply(Command, Position, Handle);
        _         -> Module:storage_apply(Command, Position, Label, Handle)
    end,
    {Reply, State#state{handle = NewHandle, position = Position}}.

-spec handle_delayed_reads(State :: #state{}) -> ok.
handle_delayed_reads(#state{queues = Queues, module = Module, handle = Handle, position = #raft_log_pos{index = Index} = Position}) ->
    [
        begin
            Reply = Module:storage_read(Command, Position, Handle),
            wa_raft_queue:fulfill_read(Queues, Reference, Reply)
        end || {Reference, Command} <- wa_raft_queue:query_reads(Queues, Index)
    ],
    ok.

-spec handle_create_snapshot(SnapshotName :: string(), Storage :: #state{}) -> {ok, wa_raft_log:log_pos()} | {error, Reason :: term()}.
handle_create_snapshot(SnapshotName, #state{name = Name, path = Path, module = Module, handle = Handle, position = Position} = State) ->
    SnapshotPath = filename:join(Path, SnapshotName),
    case filelib:is_dir(SnapshotPath) of
        true ->
            ?LOG_NOTICE("Storage[~0p] skips recreating existing snapshot ~0p.",
                [Name, SnapshotName], #{domain => [whatsapp, wa_raft]}),
            {ok, Position};
        false ->
            cleanup_snapshots(State),
            ?LOG_NOTICE("Storage[~0p] is creating snapshot ~0p.",
                [Name, SnapshotName], #{domain => [whatsapp, wa_raft]}),
            case Module:storage_create_snapshot(SnapshotPath, Handle) of
                ok -> {ok, Position};
                Other -> Other
            end
    end.

-spec handle_create_witness_snapshot(SnapshotName :: string(), Storage :: #state{}) -> {ok, wa_raft_log:log_pos()} | {error, Reason :: term()}.
handle_create_witness_snapshot(SnapshotName, #state{name = Name, path = Path, module = Module, handle = Handle, position = Position} = State) ->
    SnapshotPath = filename:join(Path, SnapshotName),
    case filelib:is_dir(SnapshotPath) of
        true ->
            ?LOG_NOTICE("Storage[~0p] skips recreating existing witness snapshot ~0p.",
                [Name, SnapshotName], #{domain => [whatsapp, wa_raft]}),
            {ok, Position};
        false ->
            cleanup_snapshots(State),
            ?LOG_NOTICE("Storage[~0p] is creating witness snapshot ~0p.",
                [Name, SnapshotName], #{domain => [whatsapp, wa_raft]}),
            case erlang:function_exported(Module, storage_create_witness_snapshot, 2) of
                true ->
                    case Module:storage_create_witness_snapshot(SnapshotPath, Handle) of
                        ok -> {ok, Position};
                        Other -> Other
                    end;
                false ->
                    {error, not_supported}
            end
    end.

-spec refresh_config(Storage :: #state{}) -> #state{}.
refresh_config(#state{self = Self, module = Module, handle = Handle} = Storage) ->
    case Module:storage_config(Handle) of
        {ok, Version, Config} ->
            Storage#state{
                config = {ok, Version, wa_raft_server:normalize_config(Config)},
                witness = wa_raft_server:is_witness(Self, Config)
            };
        undefined ->
            Storage#state{
                config = undefined,
                witness = false
            }
    end.

-define(MAX_RETAINED_SNAPSHOT, 1).

-spec cleanup_snapshots(#state{}) -> ok.
cleanup_snapshots(#state{path = Path}) ->
    Snapshots = list_snapshots(Path),
    case length(Snapshots) > ?MAX_RETAINED_SNAPSHOT of
        true ->
            lists:foreach(
                fun ({_, SnapshotName}) ->
                    SnapshotPath = filename:join(Path, SnapshotName),
                    ?LOG_NOTICE("Removing snapshot \"~s\".", [SnapshotPath], #{domain => [whatsapp, wa_raft]}),
                    file:del_dir_r(SnapshotPath)
                end, lists:sublist(Snapshots, length(Snapshots) - ?MAX_RETAINED_SNAPSHOT)),
            ok;
        _ ->
            ok
    end.

%% Private functions
-spec list_snapshots(Path :: string()) -> [{wa_raft_log:log_pos(), file:filename()}].
list_snapshots(Path) ->
    SnapshotNames = filelib:wildcard(?SNAPSHOT_PREFIX ++ ".*", Path),
    Snapshots = lists:filtermap(fun decode_snapshot_name/1, SnapshotNames),
    lists:keysort(1, Snapshots).

-spec decode_snapshot_name(SnapshotName :: string()) -> {true, {wa_raft_log:log_pos(), file:filename()}} | false.
decode_snapshot_name(SnapshotName) ->
    case string:lexemes(SnapshotName, ".") of
        [?SNAPSHOT_PREFIX, IndexStr, TermStr | _] ->
            case {list_to_integer(IndexStr), list_to_integer(TermStr)} of
                {Index, Term} when Index >= 0 andalso Term >= 0 ->
                    {true, {#raft_log_pos{index = Index, term = Term}, SnapshotName}};
                _ ->
                    ?LOG_WARNING("Invalid snapshot with invalid index (~p) and/or term (~p). (full name ~p)",
                        [IndexStr, TermStr, SnapshotName], #{domain => [whatsapp, wa_raft]}),
                    false
            end;
        _ ->
            ?LOG_WARNING("Invalid snapshot dir name ~p", [SnapshotName], #{domain => [whatsapp, wa_raft]}),
            false
    end.
