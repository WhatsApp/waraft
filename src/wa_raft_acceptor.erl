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
    commit/4,
    commit_async/3,
    commit_async/4,
    read/2,
    read/3
]).

%% Client API - RAFT apis
-export([
    adjust_config/3
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
    read_op/0,
    priority/0
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

-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_logger.hrl").

%% Request type macros
-define(READ_REQUEST(Command), {read, Command}).
-define(COMMIT_REQUEST(Op, Priority), {commit, Op, Priority}).
-define(COMMIT_ASYNC_REQUEST(From, Op, Priority), {commit, From, Op, Priority}).

%% Commit op type macros
-define(OP_DEFAULT(Op), {default, Op}).
-define(OP_ADJUST_CONFIG(Action, Index), {adjust_config, Action, Index}).

-type command() :: noop_command() | noop_omitted_command() | config_command() | dynamic().
-type noop_command() :: noop.
-type noop_omitted_command() :: noop_omitted.
-type config_command() :: {config, Config :: wa_raft_server:config()}.

-type key() :: term().
-type op() :: {Key :: key(), Command :: command()}.
-type read_op() :: {From :: gen_server:from(), Command :: command()}.
-type priority() :: high | low.

-type call_error_type() :: timeout | unreachable | {call_error, Reason :: term()}.
-type call_error() :: {error, call_error_type()}.
-type call_result() :: Result :: dynamic() | Error :: call_error().

-type read_request() :: ?READ_REQUEST(Command :: command()).
-type read_error_type() :: not_leader | read_queue_full | apply_queue_full | {notify_redirect, Peer :: node()}.
-type read_error() :: {error, read_error_type()}.
-type read_result() :: Result :: dynamic() | Error :: read_error() | call_error().

-type commit_op() :: default_op() | adjust_config_op().
-type default_op() :: ?OP_DEFAULT(Op :: op()).
-type adjust_config_op() :: ?OP_ADJUST_CONFIG(Action :: wa_raft_server:config_action(), Index :: wa_raft_log:log_index() | undefined).
-type commit_request() :: ?COMMIT_REQUEST(Op :: commit_op(), Priority :: priority()).
-type commit_async_request() :: ?COMMIT_ASYNC_REQUEST(From :: gen_server:from(), Op :: op(), Priority :: priority()).
-type commit_error_type() ::
    not_supported |
    not_leader |
    commit_queue_full |
    apply_queue_full |
    {notify_redirect, Peer :: node()} |
    commit_stalled |
    cancelled.
-type commit_error() :: {error, commit_error_type()}.
-type commit_result() :: Result :: dynamic() | Error :: commit_error() | call_error().

%% Acceptor state
-record(state, {
    % Acceptor service name
    name :: atom(),
    % RAFT table
    table :: wa_raft:table(),
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
    commit(ServerRef, Op, Timeout, high).

-spec commit(ServerRef :: gen_server:server_ref(), Op :: op(), Timeout :: timeout(), Priority :: priority()) -> commit_result().
commit(ServerRef, Op, Timeout, Priority) ->
    call(ServerRef, ?COMMIT_REQUEST(?OP_DEFAULT(Op), Priority), Timeout).

-spec commit_async(ServerRef :: gen_server:server_ref(), From :: {pid(), term()}, Op :: op()) -> ok.
commit_async(ServerRef, From, Op) ->
    commit_async(ServerRef, From, Op, high).

-spec commit_async(ServerRef :: gen_server:server_ref(), From :: {pid(), term()}, Op :: op(), Priority :: priority()) -> ok.
commit_async(ServerRef, From, Op, Priority) ->
    gen_server:cast(ServerRef, ?COMMIT_ASYNC_REQUEST(From, Op, Priority)).

% Strong-read
-spec read(ServerRef :: gen_server:server_ref(), Command :: command()) -> read_result().
read(ServerRef, Command) ->
    read(ServerRef, Command, ?RAFT_RPC_CALL_TIMEOUT()).

-spec read(ServerRef :: gen_server:server_ref(), Command :: command(), Timeout :: timeout()) -> read_result().
read(ServerRef, Command, Timeout) ->
    call(ServerRef, ?READ_REQUEST(Command), Timeout).

-spec adjust_config(
    ServerRef :: gen_server:server_ref(),
    Action :: wa_raft_server:config_action(),
    Index :: wa_raft_log:log_index() | undefined
) -> commit_result().
adjust_config(ServerRef, Action, Index) ->
    adjust_config(ServerRef, Action, Index, ?RAFT_RPC_CALL_TIMEOUT()).

-spec adjust_config(
    ServerRef :: gen_server:server_ref(),
    Action :: wa_raft_server:config_action(),
    Index :: wa_raft_log:log_index() | undefined,
    Timeout :: timeout()
) -> commit_result().
adjust_config(ServerRef, Action, Index, Timeout) ->
    call(ServerRef, ?COMMIT_REQUEST(?OP_ADJUST_CONFIG(Action, Index), high), Timeout).

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

    ?RAFT_LOG_NOTICE("Acceptor[~0p] starting for partition ~0p/~0p", [Name, Table, Partition]),

    {ok, #state{
        name = Name,
        table = Table,
        server = Server,
        queues = wa_raft_queue:queues(Options)
    }}.

-spec handle_call(read_request(), gen_server:from(), #state{}) -> {reply, read_result(), #state{}} | {noreply, #state{}};
                 (commit_request(), gen_server:from(), #state{}) -> {reply, commit_result(), #state{}} | {noreply, #state{}}.
handle_call(?READ_REQUEST(Command), From, State) ->
    case read_impl(From, Command, State) of
        continue           -> {noreply, State};
        {error, _} = Error -> {reply, Error, State}
    end;
handle_call(?COMMIT_REQUEST(Op, Priority), From, State) ->
    case commit_impl(From, Op, Priority, State) of
        continue           -> {noreply, State};
        {error, _} = Error -> {reply, Error, State}
    end;
handle_call(Request, From, #state{name = Name} = State) ->
    ?RAFT_LOG_ERROR("Acceptor[~0p] received unexpected call ~0P from ~0p.", [Name, Request, 30, From]),
    {noreply, State}.

-spec handle_cast(commit_async_request(), #state{}) -> {noreply, #state{}}.
handle_cast(?COMMIT_ASYNC_REQUEST(From, Op, Priority), State) ->
    Result = commit_impl(From, ?OP_DEFAULT(Op), Priority, State),
    Result =/= continue andalso gen_server:reply(From, Result),
    {noreply, State};
handle_cast(Request, #state{name = Name} = State) ->
    ?RAFT_LOG_ERROR("Acceptor[~0p] received unexpected cast ~0P.", [Name, Request, 30]),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> ok.
terminate(Reason, #state{name = Name}) ->
    ?RAFT_LOG_NOTICE("Acceptor[~0p] terminating with reason ~0P", [Name, Reason, 30]),
    ok.

%%-------------------------------------------------------------------
%% RAFT Acceptor - Implementations
%%-------------------------------------------------------------------

%% Enqueue a commit.
-spec commit_impl(
    From :: gen_server:from(),
    CommitOp :: commit_op(),
    Priority :: priority(),
    State :: #state{}
) -> continue | commit_error().
commit_impl(From, CommitOp, Priority, #state{table = Table, name = Name, server = Server, queues = Queues}) ->
    StartT = os:timestamp(),
    ?RAFT_LOG_DEBUG("Acceptor[~0p] starts to handle commit of ~0P from ~0p.", [Name, CommitOp, 30, From]),
    try
        case wa_raft_queue:commit_started(Queues, Priority) of
            ok ->
                case CommitOp of
                    ?OP_DEFAULT(Op) ->
                        wa_raft_server:commit(Server, From, Op, Priority),
                        continue;
                    ?OP_ADJUST_CONFIG(Action, Index) ->
                        wa_raft_server:adjust_config(Server, From, Action, Index),
                        continue;
                    _ ->
                        ?RAFT_LOG_WARNING(
                            "Acceptor[~0p] does not know how to handle commit op ~0P.",
                            [Name, CommitOp, 20]
                        ),
                        {error, not_supported}
                end;
            Reason ->
                ?RAFT_COUNT(Table, {'acceptor.error', Reason, Priority}),
                ?RAFT_LOG_WARNING(
                    "Acceptor[~0p] is rejecting commit request from ~0p due to ~0p.",
                    [Name, From, Reason]
                ),
                {error, Reason}
        end
    after
        ?RAFT_GATHER(Table, 'acceptor.commit.func', timer:now_diff(os:timestamp(), StartT))
    end.

%% Enqueue a strongly-consistent read.
-spec read_impl(gen_server:from(), command(), #state{}) -> continue | read_error().
read_impl(From, Command, #state{table = Table, name = Name, server = Server, queues = Queues}) ->
    StartT = os:timestamp(),
    ?RAFT_LOG_DEBUG("Acceptor[~p] starts to handle read of ~0P from ~0p.", [Name, Command, 100, From]),
    try
        case wa_raft_queue:reserve_read(Queues) of
            ok ->
                wa_raft_server:read(Server, {From, Command}),
                continue;
            Reason ->
                ?RAFT_COUNT(Table, {'acceptor.strong_read.error', Reason}),
                ?RAFT_LOG_WARNING(
                    "Acceptor[~0p] is rejecting read request from ~0p due to ~0p.",
                    [Name, From, Reason]
                ),
                {error, Reason}
        end
    after
        ?RAFT_GATHER(Table, 'acceptor.strong_read.func', timer:now_diff(os:timestamp(), StartT))
    end.
