%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements the front-end process for accepting commits / reads

-module(wa_raft_acceptor).
-compile(warn_missing_spec).
-behaviour(gen_server).

%% OTP supervisor
-export([
    child_spec/1,
    start_link/1
]).

%% Client API - data access
-export([
    commit/2,
    commit/3,
    read/2,
    queue_full/2
]).

%% Misc API
-export([
    stop/1,
    status/1,
    register_server/2,
    register_storage/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export_type([
    read_options/0,
    command/0,
    op/0
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% Acceptor state
-record(raft_acceptor, {
    % Service name
    name :: atom(),
    % Table name
    table :: wa_raft:table(),
    % Partition
    partition :: wa_raft:partition(),
    % Callback module
    module :: module(),
    % Server pid
    server_pid :: undefined | pid(),
    % Storage pid
    storage_pid :: undefined | pid(),
    % Local counters
    counters :: counters:counters_ref()
}).

-type command() ::
      noop
    | {config, Config :: wa_raft_server:config()}
    | {execute, Table :: atom(), Key :: term(), Module :: module(), Func :: atom(), Args :: list()}
    | term().
-type op() :: {Ref :: term(), Command :: command()}.

-define(RAFT_PENDING_COMMIT_TABLE_OPTIONS, [
    named_table, set, public,
    {read_concurrency, true},
    {write_concurrency, true}
]).

-define(RAFT_PENDING_READS_TABLE_OPTIONS, [
    named_table, ordered_set, public,
    {read_concurrency, true},
    {write_concurrency, true}
]).

-spec child_spec(Config :: [term()]) -> supervisor:child_spec().
child_spec(Config) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Config]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-type read_options() :: #{
    % Call timeout / Cast ttl. Default is 5000 for call, 'infinity' for cast.
    timeout => timeout(),
    % Expected log index for a command.
    % undefined means any log index works.
    % infinity means to read from leader node
    % {error, stale} is returned if current log index of the data doesn't match
    expected_version => infinity | undefined | wa_raft_log:log_index()
}.

%% Public API
-spec start_link(RaftArgs :: wa_raft:args()) -> {ok, Pid :: pid()} | ignore | wa_raft:error().
start_link(#{table := Table, partition := Partition} = RaftArgs) ->
    Name = ?RAFT_ACCEPTOR_NAME(Table, Partition),
    gen_server:start_link({local, Name}, ?MODULE, [RaftArgs], []).

-spec register_server(Pid :: pid(), ServerPid :: pid()) -> ok | wa_raft:error().
register_server(Pid, ServerPid) ->
    gen_server:call(Pid, {register_server, ServerPid}, ?RPC_CALL_TIMEOUT_MS).

-spec register_storage(Pid :: pid(), StoragePid :: pid()) -> ok | wa_raft:error().
register_storage(Pid, StoragePid) ->
    gen_server:call(Pid, {register_storage, StoragePid}, ?RPC_CALL_TIMEOUT_MS).

-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop, ?RPC_CALL_TIMEOUT_MS).

-spec status(Pid :: pid()) -> [{Name :: atom(), Value :: term()}].
status(Pid) ->
    gen_server:call(Pid, status, ?RPC_CALL_TIMEOUT_MS).

%% Commit a change on leader node specified by pid. It's a blocking call. It returns until it
%% is acknowledged on quorum nodes.
%%
%% See wa_raft_storage:execute() to find all supported commands.
%%
-spec commit(Pid :: pid() | Local :: atom() | {Service :: atom(), Node :: node()}, Op :: op()) -> {ok, term()} | wa_raft:error().
commit(Pid, Op) ->
    gen_server:call(Pid, {commit, Op}, ?RPC_CALL_TIMEOUT_MS).

-spec commit(Dest :: pid() | Local :: atom() | {Service :: atom(), Node :: node()}, From :: {pid(), term()}, Op :: op()) -> ok.
commit(Dest, From, Op) ->
    gen_server:cast(Dest, {commit, From, Op}).

% Strong-read
-spec read(ServerRef :: gen_server:server_ref(), Command :: command()) -> {ok, Result :: term()} | wa_raft:error().
read(Dest, Command) ->
    gen_server:call(Dest, {read, Command}, ?RPC_CALL_TIMEOUT_MS).

-spec queue_full(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> boolean().
queue_full(Table, Partition) ->
    case persistent_term:get(?RAFT_LOCAL_COUNTERS_KEY(Table, Partition), undefined) of
        undefined ->
            false;
        Counters ->
            MaxPendingCommits = ?RAFT_MAX_PENDING_COMMITS(),
            NumPendingCommits = counters:get(Counters, ?RAFT_LOCAL_COUNTER_COMMIT),
            NumPendingCommits >= MaxPendingCommits
    end.

%% gen_server callbacks
-spec init([wa_raft:args()]) -> {ok, #raft_acceptor{}}.
init([#{table := Table, partition := Partition, counters := Counters}]) ->
    process_flag(trap_exit, true),
    ?LOG_NOTICE("Starting raft acceptor on ~p:~p", [Table, Partition], #{domain => [whatsapp, wa_raft]}),
    % Create ETS table for holding pending commit references.
    ets:new(?RAFT_PENDING_COMMITS_TABLE(Table, Partition), ?RAFT_PENDING_COMMIT_TABLE_OPTIONS),
    % Create ETS table for holding pending strong-read references.
    ets:new(?RAFT_PENDING_READS_TABLE(Table, Partition), ?RAFT_PENDING_READS_TABLE_OPTIONS),
    % Reset counters for commits / strong-reads.
    counters:put(Counters, ?RAFT_LOCAL_COUNTER_COMMIT, 0),
    counters:put(Counters, ?RAFT_LOCAL_COUNTER_READ, 0),

    Name = ?RAFT_ACCEPTOR_NAME(Table, Partition),
    State = #raft_acceptor{
        name = Name,
        table = Table,
        partition = Partition,
        counters = Counters
    },
    {ok, State}.

-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: #raft_acceptor{}) ->
    {reply, Reply :: term(), NewState :: #raft_acceptor{}} | {stop, Reason :: term(), Reply :: term(), NewState :: #raft_acceptor{}}.
handle_call({register_server, ServerPid}, _From, #raft_acceptor{name = Name} = State) ->
    ?LOG_NOTICE("[~p] registering server pid ~p", [Name, ServerPid], #{domain => [whatsapp, wa_raft]}),
    {reply, ok, State#raft_acceptor{server_pid = ServerPid}};

handle_call({register_storage, StoragePid}, _From, #raft_acceptor{name = Name} = State) ->
    ?LOG_NOTICE("[~p] registering storage pid ~p", [Name, StoragePid], #{domain => [whatsapp, wa_raft]}),
    {reply, ok, State#raft_acceptor{storage_pid = StoragePid}};

handle_call({read, Command}, From, #raft_acceptor{} = State0) ->
    State1 = read_impl(From, Command, State0),
    {noreply, State1};

handle_call({commit, _Op}, From, #raft_acceptor{name = Name, server_pid = undefined} = State) ->
    ?LOG_NOTICE("[~p] commit op from ~p before registered with server", [Name, From], #{domain => [whatsapp, wa_raft]}),
    {reply, {error, not_registered}, State};

handle_call({commit, Op}, From, State0) ->
    State1 = commit_impl(From, Op, State0),
    {noreply, State1};

handle_call(status, _From, State) ->
    Status = [
        {name, State#raft_acceptor.name},
        {partition, State#raft_acceptor.partition},
        {server_pid, State#raft_acceptor.server_pid},
        {storage_pid, State#raft_acceptor.storage_pid}
    ],
    {reply, Status, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Cmd, From, #raft_acceptor{name = Name} = State) ->
    ?LOG_ERROR("[~p] Unexpected call ~p from ~p", [Name, Cmd, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.


-spec handle_cast(Request :: term(), State :: #raft_acceptor{}) -> {noreply, NewState :: #raft_acceptor{}}.
handle_cast({commit, From, Op}, State0) ->
    State1 = commit_impl(From, Op, State0),
    {noreply, State1};

handle_cast(Cmd, #raft_acceptor{name = Name} = State) ->
    ?LOG_ERROR("[~p] Unexpected cast ~p", [Name, Cmd], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.


-spec handle_info(Request :: term(), State :: #raft_acceptor{}) -> {noreply, NewState :: #raft_acceptor{}}.
handle_info(Command, #raft_acceptor{name = Name} = State) ->
    ?LOG_ERROR("[~p] Unexpected info ~p", [Name, Command], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State0 :: #raft_acceptor{}) -> State1 :: #raft_acceptor{}.
terminate(Reason, #raft_acceptor{name = Name} = State) ->
    ?LOG_NOTICE("[~p] Acceptor terminated for reason ~p", [Name, Reason], #{domain => [whatsapp, wa_raft]}),
    State.

%% Private functions

-spec commit_impl(From :: {pid(), term()}, Request :: op(), State :: #raft_acceptor{}) -> NewState :: #raft_acceptor{}.
commit_impl(From, {Ref, _} = Op, #raft_acceptor{table = Table, partition = Partition, server_pid = ServerPid, name = Name,
                                                counters = Counters} = State) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("[~p] Commit starts", [Name], #{domain => [whatsapp, wa_raft]}),
    MaxPendingCommits = ?RAFT_MAX_PENDING_COMMITS(),
    MaxPendingApplies = ?RAFT_CONFIG(raft_apply_queue_max_size, 1000),
    NumPendingCommits = counters:get(Counters, ?RAFT_LOCAL_COUNTER_COMMIT),
    NumPendingApplies = counters:get(Counters, ?RAFT_LOCAL_COUNTER_APPLY),
    {IsRefPresent, PrevFrom} = case ets:lookup(?RAFT_PENDING_COMMITS_TABLE(Table, Partition), Ref) of
                                   [] -> {false, undefined};
                                   [{Ref, OrigFrom}] -> {true, OrigFrom}
                               end,
    case {IsRefPresent, NumPendingCommits > MaxPendingCommits, NumPendingApplies > MaxPendingApplies} of
        {true, _, _} ->
            ?LOG_WARNING("[~p] Duplicate request ~p. ~0P", [Name, Ref, PrevFrom, 100], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.duplicate_commit'),
            gen_server:reply(From, {error, {duplicate_request, Ref}});
        {false, true, _} ->
            ?LOG_WARNING("[~p] Reject request ~p. Commit queue is full (limit ~p)", [Name, Ref, MaxPendingCommits], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.commit_queue_full'),
            gen_server:reply(From, {error, {commit_queue_full, Ref}});
        {false, false, true} ->
            ?LOG_WARNING("[~p] Reject request ~p. Apply queue is full (limit ~p)", [Name, Ref, MaxPendingApplies], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.apply_queue_full'),
            gen_server:reply(From, {error, {apply_queue_full, Ref}});
        _ ->
            %% Increase number of pending commits by one and insert the new commit reference.
            counters:add(Counters, ?RAFT_LOCAL_COUNTER_COMMIT, 1),
            ets:insert(?RAFT_PENDING_COMMITS_TABLE(Table, Partition), {Ref, From}),
            wa_raft_server:commit(ServerPid, Op),
            ?RAFT_GATHER('raft.acceptor.commit.request.pending', NumPendingCommits + 1)
    end,
    ?RAFT_GATHER('raft.acceptor.commit.func', timer:now_diff(os:timestamp(), StartT)),
    State.

-spec read_impl(From :: gen_server:from(),
                Command :: command(),
                State0 :: #raft_acceptor{}) -> State1 :: #raft_acceptor{}.
%% Strongly-consistent read.
read_impl(From, Command, #raft_acceptor{name = Name, server_pid = ServerPid, counters = Counters} = State) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("[~p] read starts", [Name], #{domain => [whatsapp, wa_raft]}),
    NumPendingReads = counters:get(Counters, ?RAFT_LOCAL_COUNTER_READ),
    MaxPendingReads = ?RAFT_CONFIG(raft_max_pending_reads, 5000),
    NumPendingApplies = counters:get(Counters, ?RAFT_LOCAL_COUNTER_APPLY),
    MaxPendingApplies = ?RAFT_CONFIG(raft_apply_queue_max_size, 1000),
    case {NumPendingReads >= MaxPendingReads, NumPendingApplies >= MaxPendingApplies} of
        {true, _} ->
            ?LOG_WARNING("[~p] Reject read request. Storage queue is full (read limit ~p)",
                         [Name, MaxPendingReads], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.strong_read.read_queue_full'),
            gen_server:reply(From, {error, read_queue_full});
        {_, true} ->
            ?LOG_WARNING("[~p] Reject read request. Apply queue is full (apply limit ~p)",
                         [Name, MaxPendingApplies], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.strong_read.apply_queue_full'),
            gen_server:reply(From, {error, apply_queue_full});
        _ ->
            %% Increase number of pending strong-reads.
            counters:add(Counters, ?RAFT_LOCAL_COUNTER_READ, 1),
            wa_raft_server:read(ServerPid, {From, Command}),
            ?RAFT_GATHER('raft.acceptor.strong_read.request.pending', NumPendingReads + 1)
    end,
    ?RAFT_GATHER('raft.acceptor.strong_read.func', timer:now_diff(os:timestamp(), StartT)),
    State.
