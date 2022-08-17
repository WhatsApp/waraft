%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_worker).
-compile(warn_missing_spec).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% Internal API
-export([
    name/2
]).

%% OTP supervision
-export([
    child_spec/2,
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

-define(CONTINUE_TIMEOUT, 0).

-record(state, {
    node :: node(),
    number :: non_neg_integer(),
    table :: ets:tid(),

    states = #{} :: #{module() => term()},
    marker :: undefined | 0 | reference()
}).
-type state() :: #state{}.

%%% ------------------------------------------------------------------------
%%%  Internal API
%%%

-spec name(Node :: node(), Number :: non_neg_integer()) -> atom().
name(Node, Number) ->
    list_to_atom(lists:concat([?MODULE, "_", Node, "_", integer_to_list(Number)])).

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%

-spec child_spec(Node :: node(), Number :: non_neg_integer()) -> supervisor:child_spec().
child_spec(Node, Number) ->
    #{
        id => {?MODULE, Node, Number},
        start => {?MODULE, start_link, [Node, Number]},
        restart => permanent,
        shutdown => 5000,
        modules => [?MODULE]
    }.

-spec start_link(Node :: node(), Number :: non_neg_integer()) -> {ok, Pid :: pid()} | wa_raft:error().
start_link(Node, Number) ->
    gen_server:start_link(?MODULE, {Node, Number}, []).

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init(Args :: {node(), non_neg_integer()}) -> {ok, State :: state(), Timeout :: timeout()}.
init({Node, Number}) ->
    Table = ets:new(?MODULE, [ordered_set, public]),
    {ok, #state{node = Node, number = Number, table = Table}, ?CONTINUE_TIMEOUT}.

-spec handle_call(Request :: term(), From :: {Pid :: pid(), Tag :: term()}, State :: state()) ->
    {noreply, NewState :: state(), Timeout :: timeout()}.
handle_call(Request, From, #state{number = Number} = State) ->
    ?LOG_WARNING("[~p] received unrecognized call ~p from ~p",
        [Number, Request, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_cast(Request :: term(), State :: state()) -> {noreply, NewState :: state(), Timeout :: timeout()}.
handle_cast({send, ID, FileID}, #state{table = Table} = State) ->
    ?RAFT_COUNT('raft.transport.send'),
    wa_raft_transport:update_file_info(ID, FileID,
        fun (Info) -> Info#{start_ts => erlang:system_time(millisecond)} end),
    true = ets:insert_new(Table, {make_ref(), ID, FileID}),
    {noreply, State, ?CONTINUE_TIMEOUT};
handle_cast(Request, #state{number = Number} = State) ->
    ?LOG_WARNING("[~p] received unrecognized cast ~p",
        [Number, Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_info(Info :: term(), State :: state()) ->
      {noreply, NewState :: state()}
    | {noreply, NewState :: state(), Timeout :: timeout()}.
handle_info(timeout, #state{table = Table, marker = undefined} = State) ->
    case ets:first(Table) of
        '$end_of_table' -> {noreply, State};                                 % table is empty so wait until there is work
        _FirstKey       -> {noreply, State#state{marker = 0}, ?CONTINUE_TIMEOUT} % 0 compares smaller than any ref
    end;
handle_info(timeout, #state{number = Number, table = Table, states = States, marker = Marker} = State) ->
    case ets:next(Table, Marker) of
        '$end_of_table' ->
            {noreply, State#state{marker = undefined}, ?CONTINUE_TIMEOUT};
        NextKey ->
            [{NextKey, ID, FileID}] = ets:lookup(Table, NextKey),
            {Result, NewState} = case wa_raft_transport:transport_info(ID) of
                {ok, #{module := Module}} ->
                    try get_module_state(Module, State) of
                        {ok, ModuleState0} ->
                            try Module:transport_send(ID, FileID, ModuleState0) of
                                {ok, ModuleState1} ->
                                    {ok, State#state{states = States#{Module => ModuleState1}}};
                                {continue, ModuleState1} ->
                                    {continue, State#state{states = States#{Module => ModuleState1}}};
                                {stop, Reason, ModuleState1} ->
                                    {{stop, Reason}, State#state{states = States#{Module => ModuleState1}}}
                            catch
                                T:E:S ->
                                    ?LOG_WARNING("[~p] module ~p failed to send file ~p:~p due to ~p ~p: ~p",
                                        [Number, Module, ID, FileID, T, E, S], #{domain => [whatsapp, wa_raft]}),
                                    {{T, E}, State}
                            end;
                        {stop, Reason} ->
                            {{stop, Reason}, State}
                    catch
                        T:E:S ->
                            ?LOG_WARNING("[~p] module ~p failed to get/init module state due to ~p ~p: ~p",
                                [Number, Module, T, E, S], #{domain => [whatsapp, wa_raft]}),
                            {{T, E}, State}
                    end;
                _ ->
                    ?LOG_WARNING("[~p] trying to send for unknown transfer ~p",
                        [Number, ID], #{domain => [whatsapp, wa_raft]}),
                    {{stop, invalid_transport}, State}
            end,
            Result =/= continue andalso begin
                ets:delete(Table, NextKey),
                wa_raft_transport:complete(ID, FileID, Result, self())
            end,
            {noreply, NewState#state{marker = NextKey}, ?CONTINUE_TIMEOUT}
    end;
handle_info(Info, #state{number = Number} = State) ->
    ?LOG_WARNING("[~p] received unrecognized info ~p",
        [Number, Info], #{domain => [whatsapp, wa_raft]}),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, #state{states = States}) ->
    maps:fold(
        fun (Module, State, _) ->
            case erlang:function_exported(Module, transport_terminate, 2) of
                true  -> Module:transport_terminate(Reason, State);
                false -> ok
            end
        end, undefined, States).

-spec get_module_state(module(), state()) -> {ok, state()} | {stop, term()}.
get_module_state(Module, #state{node = Node, states = States}) ->
    case States of
        #{Module := ModuleState} -> {ok, ModuleState};
        _                        -> Module:transport_init(Node)
    end.
