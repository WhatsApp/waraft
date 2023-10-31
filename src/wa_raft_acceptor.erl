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
    handle_info/2,
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
-include("wa_raft.hrl").

-type command() :: noop_command() | config_command() | eqwalizer:dynamic().
-type noop_command() :: noop.
-type config_command() :: {config, Config :: wa_raft_server:config()}.

-type key() :: term().
-type op() :: {Key :: key(), Command :: command()}.
-type read_op() :: {From :: gen_server:from(), Command :: command()}.

-type call_error_type() :: timeout | unreachable | {call_error, Reason :: term()}.
-type call_error() :: {error, call_error_type()}.
-type call_result() :: Result :: eqwalizer:dynamic() | Error :: call_error().

-type read_error_type() :: not_leader | {notify_redirect, Peer :: node()}.
-type read_error() :: {error, read_error_type()}.
-type read_result() :: Result :: eqwalizer:dynamic() | Error :: read_error() | call_error().

-type commit_error_type() :: not_leader | {notify_redirect, Peer :: node()} | commit_stalled.
-type commit_error() :: {error, commit_error_type()}.
-type commit_result() :: Result :: eqwalizer:dynamic() | Error :: commit_error() | call_error().

%% Acceptor state
-record(raft_acceptor, {
    % Service name
    name :: atom(),
    % Table name
    table :: wa_raft:table(),
    % Partition
    partition :: wa_raft:partition(),
    % Server service name
    server_name :: atom(),
    % Storage service name
    storage_name :: atom()
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

%% gen_server callbacks
-spec init(Options :: #raft_options{}) -> {ok, #raft_acceptor{}}.
init(#raft_options{table = Table, partition = Partition, acceptor_name = Name, server_name = Server, storage_name = Storage}) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Acceptor[~0p] starting for partition ~0p/~0p",
        [Name, Table, Partition], #{domain => [whatsapp, wa_raft]}),

    {ok, #raft_acceptor{
        name = Name,
        table = Table,
        partition = Partition,
        server_name = Server,
        storage_name = Storage
    }}.

-spec handle_call(Request, From :: gen_server:from(), State :: #raft_acceptor{}) ->
    {noreply, NewState :: #raft_acceptor{}} | {stop, Reason :: term(), Reply :: term(), NewState :: #raft_acceptor{}}
    when Request :: {read, command()} | {commit, op()} | stop.

handle_call({read, Command}, From, #raft_acceptor{} = State0) ->
    State1 = read_impl(From, Command, State0),
    {noreply, State1};

handle_call({commit, Op}, From, State0) ->
    State1 = commit_impl(From, Op, State0),
    {noreply, State1};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Cmd, From, #raft_acceptor{name = Name} = State) ->
    ?LOG_ERROR("[~p] Unexpected call ~p from ~p", [Name, Cmd, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.


-spec handle_cast(Request, State :: #raft_acceptor{}) -> {noreply, NewState :: #raft_acceptor{}}
    when Request :: {commit, gen_server:from(), op()}.
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

-spec commit_impl(From :: gen_server:from(), Request :: op(), State :: #raft_acceptor{}) -> NewState :: #raft_acceptor{}.
commit_impl(From, {Ref, _} = Op, #raft_acceptor{table = Table, partition = Partition, server_name = Server, name = Name} = State) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("[~p] Commit starts", [Name], #{domain => [whatsapp, wa_raft]}),
    case wa_raft_queue:commit(Table, Partition, Ref, From) of
        duplicate ->
            ?LOG_WARNING("[~p] Duplicate request ~p.", [Name, Ref, 100], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.duplicate_commit'),
            gen_server:reply(From, {error, {duplicate_request, Ref}});
        commit_queue_full ->
            ?LOG_WARNING("[~p] Reject request ~p. Commit queue is full", [Name, Ref], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.commit_queue_full'),
            gen_server:reply(From, {error, {commit_queue_full, Ref}});
        apply_queue_full ->
            ?LOG_WARNING("[~p] Reject request ~p. Apply queue is full", [Name, Ref], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_COUNT('raft.acceptor.error.apply_queue_full'),
            gen_server:reply(From, {error, {apply_queue_full, Ref}});
        ok ->
            wa_raft_server:commit(Server, Op)
    end,
    ?RAFT_GATHER('raft.acceptor.commit.func', timer:now_diff(os:timestamp(), StartT)),
    State.

-spec read_impl(From :: gen_server:from(),
                Command :: command(),
                State0 :: #raft_acceptor{}) -> State1 :: #raft_acceptor{}.
%% Strongly-consistent read.
read_impl(From, Command, #raft_acceptor{name = Name, table = Table, partition = Partition, server_name = Server} = State) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("Acceptor[~p] starts to handle read of ~0P from ~0p.",
        [Name, Command, 100, From], #{domain => [whatsapp, wa_raft]}),
    case wa_raft_queue:reserve_read(Table, Partition) of
        read_queue_full ->
            ?RAFT_COUNT('raft.acceptor.strong_read.error.read_queue_full'),
            ?LOG_WARNING("Acceptor[~p] is rejecting read request from ~p because the read queue is full.",
                [Name, From], #{domain => [whatsapp, wa_raft]}),
            gen_server:reply(From, {error, read_queue_full});
        apply_queue_full ->
            ?RAFT_COUNT('raft.acceptor.strong_read.error.apply_queue_full'),
            ?LOG_WARNING("Acceptor[~p] is rejecting read request from ~p because the apply queue is full.",
                [Name, From], #{domain => [whatsapp, wa_raft]}),
            gen_server:reply(From, {error, apply_queue_full});
        ok ->
            wa_raft_server:read(Server, {From, Command})
    end,
    ?RAFT_GATHER('raft.acceptor.strong_read.func', timer:now_diff(os:timestamp(), StartT)),
    State.
