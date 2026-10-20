%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_worker).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

%% OTP supervision
-export([
    child_spec/0,
    start_link/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_logger.hrl").

-define(CONTINUE_TIMEOUT, 0).

-record(state, {
    id :: wa_raft_transport:transport_id(),
    table :: wa_raft:table() | undefined,
    module :: module(),
    file :: undefined | wa_raft_transport:file_id(),
    %% The selected transport module owns this opaque callback state.
    state :: term()
}).

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => temporary,
        shutdown => 5000,
        modules => [?MODULE]
    }.

-spec start_link(ID :: wa_raft_transport:transport_id(), Module :: module()) -> gen_server:start_ret().
start_link(ID, Module) ->
    gen_server:start_link(?MODULE, {ID, Module}, []).

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init({ID :: wa_raft_transport:transport_id(), Module :: module()}) ->
    {ok, State :: #state{}, Timeout :: timeout()} | {stop, Reason :: term()}.
init({ID, Module}) ->
    process_flag(trap_exit, true),
    case wa_raft_transport:transport_info(ID) of
        {ok, #{peer := Peer, meta := Meta}} ->
            case Module:transport_init(Peer) of
                {ok, ModuleState} ->
                    Table = maps:get(table, Meta, undefined),
                    {ok, #state{id = ID, table = Table, module = Module, state = ModuleState}, ?CONTINUE_TIMEOUT};
                {stop, Reason} ->
                    {stop, Reason}
            end;
        not_found ->
            {stop, transport_not_found}
    end.

-spec handle_call(Request :: term(), From :: {Pid :: pid(), Tag :: term()}, State :: #state{}) ->
    {noreply, NewState :: #state{}, Timeout :: timeout()}.
handle_call(Request, From, State) ->
    ?RAFT_LOG_WARNING("received unrecognized call ~0P from ~p", [Request, 20, From]),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}, Timeout :: timeout()}.
handle_cast(Request, State) ->
    ?RAFT_LOG_WARNING("received unrecognized cast ~0P", [Request, 20]),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_info(Info :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}, Timeout :: timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info(timeout, #state{id = ID, table = Table, file = undefined} = State) ->
    maybe_wait_for_test_release(ID),
    case wa_raft_transport:next_file(ID) of
        {ok, FileID} ->
            ?RAFT_COUNT(Table, 'transport.file.send'),
            wa_raft_transport:start_file(ID, FileID),
            {noreply, State#state{file = FileID}, ?CONTINUE_TIMEOUT};
        empty ->
            {stop, normal, State};
        not_found ->
            {stop, {shutdown, transport_not_found}, State}
    end;
handle_info(timeout, #state{id = ID, module = Module, file = FileID, state = ModuleState} = State) ->
    try Module:transport_send(ID, FileID, ModuleState) of
        {ok, NewModuleState} ->
            wa_raft_transport:complete_file(ID, FileID, ok),
            {noreply, State#state{file = undefined, state = NewModuleState}, ?CONTINUE_TIMEOUT};
        {continue, NewModuleState} ->
            {noreply, State#state{state = NewModuleState}, ?CONTINUE_TIMEOUT};
        {stop, Reason, NewModuleState} ->
            wa_raft_transport:complete_file(ID, FileID, {stop, Reason}),
            {stop, {shutdown, Reason}, State#state{file = undefined, state = NewModuleState}}
    catch
        T:E:S ->
            ?RAFT_LOG_WARNING(
                "module ~0p failed to send file ~0p:~0p due to ~0p ~0P: ~0P",
                [Module, ID, FileID, T, E, 20, S, 20]
            ),
            wa_raft_transport:complete_file(ID, FileID, {T, E}),
            {stop, {shutdown, {T, E}}, State#state{file = undefined}}
    end;
handle_info({'EXIT', _From, normal}, State) ->
    {noreply, State, ?CONTINUE_TIMEOUT};
handle_info({'EXIT', _From, Reason}, State) ->
    {stop, Reason, State};
handle_info(Info, State) ->
    ?RAFT_LOG_WARNING("received unrecognized info ~0P", [Info, 20]),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec terminate(term(), #state{}) -> term().
terminate(Reason, #state{id = ID, module = Module, file = FileID, state = ModuleState}) ->
    case FileID of
        undefined -> ok;
        _ -> wa_raft_transport:complete_file(ID, FileID, {stop, Reason})
    end,
    case erlang:function_exported(Module, transport_terminate, 2) of
        true  -> Module:transport_terminate(Reason, ModuleState);
        false -> ok
    end.

-ifdef(TEST).
-spec maybe_wait_for_test_release(wa_raft_transport:transport_id()) -> ok.
maybe_wait_for_test_release(ID) ->
    case application:get_env(wa_raft, transport_worker_test_controller) of
        {ok, Controller} when is_pid(Controller) ->
            Monitor = monitor(process, Controller),
            Controller ! {transport_worker_ready, self(), ID},
            receive
                {transport_worker_continue, Controller} ->
                    demonitor(Monitor, [flush]),
                    ok;
                {'DOWN', Monitor, process, Controller, _Reason} ->
                    ok;
                {'EXIT', _From, Reason} ->
                    exit(Reason)
            end;
        _ ->
            ok
    end.
-else.
-spec maybe_wait_for_test_release(wa_raft_transport:transport_id()) -> ok.
maybe_wait_for_test_release(_ID) ->
    ok.
-endif.
