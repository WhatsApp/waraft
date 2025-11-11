%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_worker).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_logger.hrl").

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
    jobs = queue:new() :: queue:queue(job()),
    states = #{} :: #{module() => state()}
}).
-type state() :: #state{}.

-record(transport, {
    id :: wa_raft_transport:transport_id()
}).
-record(file, {
    id :: wa_raft_transport:transport_id(),
    file :: wa_raft_transport:file_id()
}).
-type job() :: #transport{} | #file{}.

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

-spec start_link(Node :: node(), Number :: non_neg_integer()) -> gen_server:start_ret().
start_link(Node, Number) ->
    gen_server:start_link(?MODULE, {Node, Number}, []).

%%% ------------------------------------------------------------------------
%%%  gen_server callbacks
%%%

-spec init(Args :: {node(), non_neg_integer()}) -> {ok, State :: state(), Timeout :: timeout()}.
init({Node, Number}) ->
    {ok, #state{node = Node, number = Number}, ?CONTINUE_TIMEOUT}.

-spec handle_call(Request :: term(), From :: {Pid :: pid(), Tag :: term()}, State :: state()) ->
    {noreply, NewState :: state(), Timeout :: timeout()}.
handle_call(Request, From, #state{number = Number} = State) ->
    ?RAFT_LOG_WARNING("[~p] received unrecognized call ~p from ~p", [Number, Request, From]),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_cast(Request, State :: state()) -> {noreply, NewState :: state(), Timeout :: timeout()}
    when Request :: {notify, wa_raft_transport:transport_id()}.
handle_cast({notify, ID}, #state{jobs = Jobs} = State) ->
    {noreply, State#state{jobs = queue:in(#transport{id = ID}, Jobs)}, ?CONTINUE_TIMEOUT};
handle_cast(Request, #state{number = Number} = State) ->
    ?RAFT_LOG_WARNING("[~p] received unrecognized cast ~p", [Number, Request]),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_info(Info :: term(), State :: state()) ->
      {noreply, NewState :: state()}
    | {noreply, NewState :: state(), Timeout :: timeout() | hibernate}.
handle_info(timeout, #state{number = Number, jobs = Jobs, states = States} = State) ->
    case queue:out(Jobs) of
        {empty, NewJobs} ->
            {noreply, State#state{jobs = NewJobs}, hibernate};
        {{value, #transport{id = ID}}, NewJobs} ->
            case wa_raft_transport:pop_file(ID) of
                {ok, FileID} ->
                    ?RAFT_COUNT('raft.transport.file.send'),
                    wa_raft_transport:update_file_info(ID, FileID,
                        fun (Info) -> Info#{status => sending, start_ts => erlang:system_time(millisecond)} end),
                    NewJob = #file{id = ID, file = FileID},
                    {noreply, State#state{jobs = queue:in(NewJob, NewJobs)}, ?CONTINUE_TIMEOUT};
                _Other ->
                    {noreply, State#state{jobs = NewJobs}, ?CONTINUE_TIMEOUT}
            end;
        {{value, #file{id = ID, file = FileID} = Job}, NewJobs} ->
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
                                    ?RAFT_LOG_WARNING(
                                        "[~p] module ~p failed to send file ~p:~p due to ~p ~p: ~p",
                                        [Number, Module, ID, FileID, T, E, S]
                                    ),
                                    {{T, E}, State}
                            end;
                        Other ->
                            {Other, State}
                    catch
                        T:E:S ->
                            ?RAFT_LOG_WARNING(
                                "[~p] module ~p failed to get/init module state due to ~p ~p: ~p",
                                [Number, Module, T, E, S]
                            ),
                            {{T, E}, State}
                    end;
                _ ->
                    ?RAFT_LOG_WARNING("[~p] trying to send for unknown transfer ~p", [Number, ID]),
                    {{stop, invalid_transport}, State}
            end,
            case Result =:= continue of
                true ->
                    {noreply, NewState#state{jobs = queue:in(Job, NewJobs)}, ?CONTINUE_TIMEOUT};
                false ->
                    wa_raft_transport:complete(ID, FileID, Result),
                    {noreply, NewState#state{jobs = queue:in(#transport{id = ID}, NewJobs)}, ?CONTINUE_TIMEOUT}
            end
    end;
handle_info(Info, #state{number = Number} = State) ->
    ?RAFT_LOG_WARNING("[~p] received unrecognized info ~p", [Number, Info]),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, #state{states = States}) ->
    maps:fold(
        fun (Module, State, _) ->
            case erlang:function_exported(Module, transport_terminate, 2) of
                true  -> Module:transport_terminate(Reason, State);
                false -> ok
            end
        end, ok, States).

-spec get_module_state(module(), state()) -> {ok, state()} | {stop, term()}.
get_module_state(Module, #state{node = Node, states = States}) ->
    case States of
        #{Module := ModuleState} -> {ok, ModuleState};
        _                        -> Module:transport_init(Node)
    end.
