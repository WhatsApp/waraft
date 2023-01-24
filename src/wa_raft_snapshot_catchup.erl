%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module manages requests to trigger snapshot catchup across all
%%% local RAFT partitions.

-module(wa_raft_snapshot_catchup).
-compile(warn_missing_spec).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% Supervisor callbacks
-export([
    child_spec/0,
    start_link/0
]).

%% Internal API
-export([
    current_snapshot_transports/0,
    request_snapshot_transport/3
]).

%% Snapshot catchup server implementation
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(SCAN_EVERY_MS, 500).

-type key() :: {node(), wa_raft:table(), wa_raft:partition()}.
-record(transport, {
    id :: wa_raft_transport:transport_id(),
    snapshot :: #raft_log_pos{}
}).
-record(state, {
    transports = #{} :: #{key() => #transport{}}
}).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec current_snapshot_transports() -> [wa_raft_transport:transport_id()].
current_snapshot_transports() ->
    gen_server:call(?MODULE, current_snapshot_transports).

-spec request_snapshot_transport(Peer :: node(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok.
request_snapshot_transport(Peer, Table, Partition) ->
    gen_server:cast(?MODULE, {request_snapshot_transport, Peer, Table, Partition}).

-spec init(Args :: term()) -> {ok, #state{}}.
init([]) ->
    process_flag(trap_exit, true),
    schedule_scan(),
    {ok, #state{}}.

-spec handle_call(Request :: term(), From :: gen_server:from(), State :: #state{}) -> {noreply, #state{}} | {reply, term(), #state{}}.
handle_call(current_snapshot_transports, _From, #state{transports = Transports} = State) ->
    {reply, maps:values(Transports), State};
handle_call(Request, From, #state{} = State) ->
    ?LOG_NOTICE("received unrecognized call ~P from ~0p", [Request, 25, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast({request_snapshot_transport, node(), wa_raft:table(), wa_raft:partition()}, State :: #state{}) -> {noreply, #state{}}.
handle_cast({request_snapshot_transport, Peer, Table, Partition}, #state{transports = Transports} = State) ->
    case Transports of
        #{{Peer, Table, Partition} := _} ->
            {noreply, State};
        _ ->
            case maps:size(Transports) < ?RAFT_CONFIG(raft_max_snapshot_catchup, 5) of
                true ->
                    try
                        StorageRef = wa_raft_storage:registered_name(Table, Partition),
                        {ok, #raft_log_pos{index = Index, term = Term} = LogPos} = wa_raft_storage:create_snapshot(StorageRef),
                        Path = filename:join(?ROOT_DIR(Table, Partition), ?SNAPSHOT_NAME(Index, Term)),
                        {ok, ID} = wa_raft_transport:start_snapshot_transfer(Peer, Table, Partition, LogPos, Path, infinity),
                        ?LOG_NOTICE("started sending snapshot for ~0p:~0p at ~0p:~0p over transport ~0p",
                            [Table, Partition, Index, Term, ID], #{domain => [whatsapp, wa_raft]}),
                        {noreply, State#state{transports = Transports#{{Peer, Table, Partition} => #transport{id = ID, snapshot = LogPos}}}}
                    catch
                        T:E:S ->
                            ?LOG_ERROR("failed to start accepted snapshot transport of ~0p:~0p to ~0p due to ~0p ~0p at ~p",
                                [Table, Partition, Peer, T, E, S], #{domain => [whatsapp, wa_raft]}),
                            {noreply, State}
                    end;
                false ->
                    {noreply, State}
            end
    end;
handle_cast(Request, #state{} = State) ->
    ?LOG_NOTICE("received unrecognized cast ~P", [Request, 25], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(scan, #state{transports = Transports} = State) ->
    NewTransports =
        maps:filter(
            fun ({_Peer, Table, Partition}, #transport{id = ID, snapshot = #raft_log_pos{index = Index, term = Term}}) ->
                Drop = case wa_raft_transport:transport_info(ID) of
                    {ok, #{status := Status}} -> Status =/= requested andalso Status =/= running;
                    _                         -> true
                end,
                Drop andalso wa_raft_storage:delete_snapshot(wa_raft_storage:registered_name(Table, Partition), ?SNAPSHOT_NAME(Index, Term)),
                not Drop
            end, Transports),
    schedule_scan(),
    {noreply, State#state{transports = NewTransports}};
handle_info(Info, #state{} = State) ->
    ?LOG_NOTICE("received unrecognized info ~P", [Info, 25], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), #state{}) -> term().
terminate(_Reason, #state{transports = Transport}) ->
    maps:foreach(
        fun ({_Peer, Table, Partition}, #transport{id = ID, snapshot = #raft_log_pos{index = Index, term = Term}}) ->
            wa_raft_transport:cancel(ID, terminating),
            wa_raft_storage:delete_snapshot(wa_raft_storage:registered_name(Table, Partition), ?SNAPSHOT_NAME(Index, Term))
        end, Transport).

-spec schedule_scan() -> reference().
schedule_scan() ->
    erlang:send_after(?SCAN_EVERY_MS, self(), scan).
