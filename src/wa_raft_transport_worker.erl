%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_worker).
-compile(warn_missing_spec).
-author('hsun324@fb.com').

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% OTP supervision
-export([
    child_spec/1,
    start_link/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    number :: non_neg_integer(),
    states = #{} :: #{module() => term()}
}).
-type state() :: #state{}.

%%% ------------------------------------------------------------------------
%%%  OTP supervision callbacks
%%%

-spec child_spec(Number :: non_neg_integer()) -> supervisor:child_spec().
child_spec(Number) ->
    #{
        id => list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Number)),
        start => {?MODULE, start_link, [Number]},
        restart => permanent,
        shutdown => 5000,
        modules => [?MODULE]
    }.

-spec start_link(Number :: non_neg_integer()) -> {ok, Pid :: pid()} | wa_raft:error().
start_link(Number) ->
    gen_server:start_link(?MODULE, [Number], []).

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init(Args :: term()) -> {ok, State :: state()}.
init([Number]) ->
    {ok, #state{number = Number}}.


-spec handle_call(Request :: term(), From :: {Pid :: pid(), Tag :: term()}, State :: state()) ->
    {noreply, NewState :: state()}.
handle_call(Request, From, #state{number = Number} = State) ->
    ?LOG_WARNING("[~p] received unrecognized call ~p from ~p",
        [Number, Request, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.


-spec handle_cast(Request :: term(), State :: state()) -> {noreply, NewState :: state()}.
handle_cast({send, ID, FileID}, #state{number = Number, states = States} = State) ->
    ?RAFT_COUNT('raft.transport.send'),
    case wa_raft_transport:transport_info(ID) of
        {ok, #{module := Module}} ->
            {Result, NewState} =
                try get_module_state(Module, State) of
                    {ok, ModuleState0} ->
                        try Module:transport_send(ID, FileID, ModuleState0) of
                            {ok, ModuleState1} ->
                                {ok, State#state{states = States#{Module => ModuleState1}}};
                            {stop, Reason, ModuleState1} ->
                                {{stop, Reason}, State#state{states = States#{Module => ModuleState1}}}
                        catch
                            T:E:S ->
                                ?LOG_WARNING("[~p] module ~p failed to send file ~p:~p due to ~p ~p: ~n~p",
                                    [Number, Module, ID, FileID, T, E, S], #{domain => [whatsapp, wa_raft]}),
                                {{T, E}, State}
                        end;
                    {stop, Reason} ->
                        {{stop, Reason}, State}
                catch
                    T:E:S ->
                        ?LOG_WARNING("[~p] module ~p failed to get/init module state due to ~p ~p: ~n~p",
                            [Number, Module, T, E, S], #{domain => [whatsapp, wa_raft]}),
                        {{T, E}, State}
                end,
            wa_raft_transport:complete(ID, FileID, Result, self()),
            {noreply, NewState};
        _ ->
            ?LOG_WARNING("[~p] got send request for unknown transfer ~p",
                [Number, ID], #{domain => [whatsapp, wa_raft]}),
            {noreply, State}
    end;
handle_cast(Request, #state{number = Number} = State) ->
    ?LOG_WARNING("[~p] received unrecognized cast ~p",
        [Number, Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(Info :: term(), State :: state()) -> {noreply, NewState :: state()}.
handle_info(Info, #state{number = Number} = State) ->
    ?LOG_WARNING("[~p] received unrecognized info ~p",
        [Number, Info], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

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
get_module_state(Module, #state{states = States}) ->
    case States of
        #{Module := ModuleState} -> {ok, ModuleState};
        _                        -> Module:transport_init()
    end.
