%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements the front-end process for accepting commits / reads

-module(wa_raft_acceptor).
-compile(warn_missing_spec_all).
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
    commit_async/3,
    read/2,
    read/3
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
    command/0,
    key/0,
    op/0,
    read_op/0
]).

-export_type([
    call_error_type/0,
    call_error/0,
    call_result/0,
    read_error/0,
    read_error_type/0,
    read_result/0,
    commit_error_type/0,
    commit_error/0,
    commit_result/0
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-type command() :: noop_command() | noop_omitted_command() | config_command() | dynamic().
-type noop_command() :: noop.
-type noop_omitted_command() :: noop_omitted.
-type config_command() :: {config, Config :: wa_raft_server:config()}.

-type key() :: term().
-type op() :: {Key :: key(), Command :: command()}.
-type read_op() :: {From :: gen_server:from(), Command :: command()}.

-type call_error_type() :: timeout | unreachable | {call_error, Reason :: term()}.
-type call_error() :: {error, call_error_type()}.
-type call_result() :: Result :: dynamic() | Error :: call_error().

-type read_request() :: {read, Command :: command()}.
-type read_error_type() :: not_leader | read_queue_full | apply_queue_full | {notify_redirect, Peer :: node()}.
-type read_error() :: {error, read_error_type()}.
-type read_result() :: Result :: dynamic() | Error :: read_error() | call_error().

-type commit_request() :: {commit, Op :: op()}.
-type commit_async_request() :: {commit, From :: gen_server:from(), Op :: op()}.
-type commit_error_type() ::
    not_leader |
    {duplicate_request, Key :: key()} |
    {commit_queue_full, Key :: key()} |
    {apply_queue_full, Key :: key()} |
    {notify_redirect, Peer :: node()} |
    commit_stalled.
-type commit_error() :: {error, commit_error_type()}.
-type commit_result() :: Result :: dynamic() | Error :: commit_error() | call_error().

%% Acceptor state
-record(state, {
    % Acceptor service name
    name :: atom(),
    % Server service name
    server :: atom(),
    % Queues handle
    queues :: wa_raft_queue:queues()
}).

%%-------------------------------------------------------------------
%% OTP Supervision
%%-------------------------------------------------------------------

%%-------------------------------------------------------------------
%% OTP Supervision
%%-------------------------------------------------------------------

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) -> gen_server:start_ret().
start_link(#raft_options{acceptor_name = Name} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%%-------------------------------------------------------------------
%% Public API
%%-------------------------------------------------------------------

%% Request that the specified RAFT server commit the provided command. The commit can only be
%% successful if the requested RAFT server is the active leader of the RAFT partition it is a
%% part of. Returns either the result returned by the storage module when applying the command
%% or an error indicating some reason for which the command was not able to be committed or
%% should be retried.
-spec commit(ServerRef :: gen_server:server_ref(), Op :: op()) -> commit_result().
commit(ServerRef, Op) ->
    commit(ServerRef, Op, ?RAFT_RPC_CALL_TIMEOUT()).

-spec commit(ServerRef :: gen_server:server_ref(), Op :: op(), Timeout :: timeout()) -> commit_result().
commit(ServerRef, Op, Timeout) ->
    call(ServerRef, {commit, Op}, Timeout).

-spec commit_async(ServerRef :: gen_server:server_ref(), From :: {pid(), term()}, Op :: op()) -> ok.
commit_async(ServerRef, From, Op) ->
    gen_server:cast(ServerRef, {commit, From, Op}).

% Strong-read
-spec read(ServerRef :: gen_server:server_ref(), Command :: command()) -> read_result().
read(ServerRef, Command) ->
    read(ServerRef, Command, ?RAFT_RPC_CALL_TIMEOUT()).

-spec read(ServerRef :: gen_server:server_ref(), Command :: command(), Timeout :: timeout()) -> read_result().
read(ServerRef, Command, Timeout) ->
    call(ServerRef, {read, Command}, Timeout).

-spec call(ServerRef :: gen_server:server_ref(), Request :: term(), Timeout :: timeout()) -> call_result().
call(ServerRef, Request, Timeout) ->
    try
        gen_server:call(ServerRef, Request, Timeout)
    catch
        exit:{timeout, _}       -> {error, timeout};
        exit:{noproc, _}        -> {error, unreachable};
        exit:{{nodedown, _}, _} -> {error, unreachable};
        exit:{shutdown, _}      -> {error, unreachable};
        exit:{Other, _}         -> {error, {call_error, Other}}
    end.

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT acceptor server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_acceptor_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT acceptor server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.acceptor_name
    end.

%%-------------------------------------------------------------------
%% RAFT Acceptor - Server Callbacks
%%-------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> {ok, #state{}}.
init(#raft_options{table = Table, partition = Partition, acceptor_name = Name, server_name = Server} = Options) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Acceptor[~0p] starting for partition ~0p/~0p",
        [Name, Table, Partition], #{domain => [whatsapp, wa_raft]}),

    {ok, #state{
        name = Name,
        server = Server,
        queues = wa_raft_queue:queues(Options)
    }}.

-spec handle_call(read_request(), gen_server:from(), #state{}) -> {reply, read_result(), #state{}} | {noreply, #state{}};
                 (commit_request(), gen_server:from(), #state{}) -> {reply, commit_result(), #state{}} | {noreply, #state{}}.
handle_call({read, Command}, From, State) ->
    case read_impl(From, Command, State) of
        continue           -> {noreply, State};
        {error, _} = Error -> {reply, Error, State}
    end;
handle_call({commit, Op}, From, State) ->
    case commit_impl(From, Op, State) of
        continue           -> {noreply, State};
        {error, _} = Error -> {reply, Error, State}
    end;
handle_call(Request, From, #state{name = Name} = State) ->
    ?LOG_ERROR("Acceptor[~0p] received unexpected call ~0P from ~0p.",
        [Name, Request, 30, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(commit_async_request(), #state{}) -> {noreply, #state{}}.
handle_cast({commit, From, Op}, State) ->
    Result = commit_impl(From, Op, State),
    Result =/= continue andalso gen_server:reply(From, Result),
    {noreply, State};
handle_cast(Request, #state{name = Name} = State) ->
    ?LOG_ERROR("Acceptor[~0p] received unexpected cast ~0P.",
        [Name, Request, 30], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> ok.
terminate(Reason, #state{name = Name}) ->
    ?LOG_NOTICE("Acceptor[~0p] terminating with reason ~0P",
        [Name, Reason, 30], #{domain => [whatsapp, wa_raft]}),
    ok.

%%-------------------------------------------------------------------
%% RAFT Acceptor - Implementations
%%-------------------------------------------------------------------

%% Enqueue a commit.
-spec commit_impl(From :: gen_server:from(), Request :: op(), State :: #state{}) -> continue | commit_error().
commit_impl(From, {Key, _} = Op, #state{name = Name, server = Server, queues = Queues}) ->
    StartT = os:timestamp(),
    try
        ?LOG_DEBUG("Acceptor[~0p] starts to handle commit of ~0P from ~0p.",
            [Name, Op, 30, From], #{domain => [whatsapp, wa_raft]}),
        case wa_raft_queue:commit(Queues, Key, From) of
            duplicate ->
                ?LOG_WARNING("Acceptor[~0p] is rejecting commit request from ~0p because it has duplicate key ~0p.",
                    [Name, From, Key], #{domain => [whatsapp, wa_raft]}),
                ?RAFT_COUNT('raft.acceptor.error.duplicate_commit'),
                {error, {duplicate_request, Key}};
            commit_queue_full ->
                ?LOG_WARNING("Acceptor[~0p] is rejecting commit request from ~0p with key ~0p because the commit queue is full.",
                    [Name, From, Key], #{domain => [whatsapp, wa_raft]}),
                ?RAFT_COUNT('raft.acceptor.error.commit_queue_full'),
                {error, {commit_queue_full, Key}};
            apply_queue_full ->
                ?LOG_WARNING("Acceptor[~0p] is rejecting commit request from ~0p with key ~0p because the apply queue is full.",
                    [Name, From, Key], #{domain => [whatsapp, wa_raft]}),
                ?RAFT_COUNT('raft.acceptor.error.apply_queue_full'),
                {error, {apply_queue_full, Key}};
            ok ->
                wa_raft_server:commit(Server, Op),
                continue
        end
    after
        ?RAFT_GATHER('raft.acceptor.commit.func', timer:now_diff(os:timestamp(), StartT))
    end.

%% Enqueue a strongly-consistent read.
-spec read_impl(gen_server:from(), command(), #state{}) -> continue | read_error().
read_impl(From, Command, #state{name = Name, server = Server, queues = Queues}) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("Acceptor[~p] starts to handle read of ~0P from ~0p.",
        [Name, Command, 100, From], #{domain => [whatsapp, wa_raft]}),
    try
        case wa_raft_queue:reserve_read(Queues) of
            read_queue_full ->
                ?RAFT_COUNT('raft.acceptor.strong_read.error.read_queue_full'),
                ?LOG_WARNING("Acceptor[~0p] is rejecting read request from ~0p because the read queue is full.",
                    [Name, From], #{domain => [whatsapp, wa_raft]}),
                {error, read_queue_full};
            apply_queue_full ->
                ?RAFT_COUNT('raft.acceptor.strong_read.error.apply_queue_full'),
                ?LOG_WARNING("Acceptor[~0p] is rejecting read request from ~0p because the apply queue is full.",
                    [Name, From], #{domain => [whatsapp, wa_raft]}),
                {error, apply_queue_full};
            ok ->
                wa_raft_server:read(Server, {From, Command}),
                continue
        end
    after
        ?RAFT_GATHER('raft.acceptor.strong_read.func', timer:now_diff(os:timestamp(), StartT))
    end.
