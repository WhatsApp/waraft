%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module manages requests to trigger snapshot catchup across all
%%% local RAFT partitions.

-module(wa_raft_snapshot_catchup).
-compile(warn_missing_spec_all).
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
    request_snapshot_transport/4
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
-type snapshot_key() :: {wa_raft:table(), wa_raft:partition(), wa_raft_log:log_pos()}.

-record(transport, {
    id :: wa_raft_transport:transport_id(),
    snapshot :: wa_raft_log:log_pos()
}).
-record(state, {
    % currently active transports
    transports = #{} :: #{key() => #transport{}},
    % counts of active transports that are using a particular snapshot
    snapshots = #{} :: #{snapshot_key() => pos_integer()},
    % backoff windows for nodes that previously reported being overloaded
    backoff_windows = #{} :: #{node() => pos_integer()}
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

-spec request_snapshot_transport(App :: atom(), Peer :: node(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok.
request_snapshot_transport(App, Peer, Table, Partition) ->
    gen_server:cast(?MODULE, {request_snapshot_transport, App, Peer, Table, Partition}).

-spec init(Args :: term()) -> {ok, #state{}}.
init([]) ->
    process_flag(trap_exit, true),
    schedule_scan(),
    {ok, #state{}}.

-spec handle_call(Request :: term(), From :: gen_server:from(), State :: #state{}) -> {noreply, #state{}} | {reply, term(), #state{}}.
handle_call(current_snapshot_transports, _From, #state{transports = Transports} = State) ->
    {reply, [ID || #transport{id = ID} <- maps:values(Transports)], State};
handle_call(Request, From, #state{} = State) ->
    ?LOG_NOTICE("received unrecognized call ~P from ~0p", [Request, 25, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast({request_snapshot_transport, atom(), node(), wa_raft:table(), wa_raft:partition()}, State :: #state{}) -> {noreply, #state{}}.
handle_cast({request_snapshot_transport, App, Peer, Table, Partition}, #state{transports = Transports, snapshots = Snapshots, backoff_windows  = BackoffWindows} = State) ->
    NowMillis = erlang:monotonic_time(millisecond),
    case {Transports,  BackoffWindows} of
        {#{{Peer, Table, Partition} := _}, _} ->
            {noreply, State};
        {_, #{Peer := RetryAfterTs}} when RetryAfterTs > NowMillis ->
            {noreply, State};
        {_, _} ->
            case maps:size(Transports) < ?RAFT_MAX_CONCURRENT_SNAPSHOT_CATCHUP() of
                true ->
                    try
                        StorageRef = wa_raft_storage:registered_name(Table, Partition),
                        {ok, #raft_log_pos{index = Index, term = Term} = LogPos} = wa_raft_storage:create_snapshot(StorageRef),
                        Path = ?RAFT_SNAPSHOT_PATH(Table, Partition, Index, Term),
                        case wa_raft_transport:start_snapshot_transfer(Peer, Table, Partition, LogPos, Path, infinity) of
                            {error, receiver_overloaded} ->
                                ?LOG_NOTICE("Peer ~0p reported being overloaded. Not sending snapshot for ~0p:~0p. Will try again later",
                                    [Peer, Table, Partition], #{domain => [whatsapp, wa_raft]}),
                                NewRetryAfterTs = NowMillis + ?RAFT_SNAPSHOT_CATCHUP_OVERLOADED_BACKOFF_MS(App),
                                {noreply, State#state{backoff_windows = BackoffWindows#{Peer => NewRetryAfterTs}}};
                            {ok, ID} ->
                                ?LOG_NOTICE("started sending snapshot for ~0p:~0p at ~0p:~0p over transport ~0p",
                                    [Table, Partition, Index, Term, ID], #{domain => [whatsapp, wa_raft]}),
                                NewTransports = Transports#{{Peer, Table, Partition} => #transport{id = ID, snapshot = LogPos}},
                                NewSnapshots = maps:update_with({Table, Partition, LogPos}, fun(V) -> V + 1 end, 1, Snapshots),
                                NewBackoffWindows = maps:remove(Peer, BackoffWindows),
                                {noreply, State#state{transports = NewTransports, snapshots = NewSnapshots, backoff_windows = NewBackoffWindows}}
                        end
                    catch
                        _T:_E:S ->
                            ?LOG_ERROR("failed to start accepted snapshot transport of ~0p:~0p to ~0p at ~p",
                                [Table, Partition, Peer, S], #{domain => [whatsapp, wa_raft]}),
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
    NewState = maps:fold(fun scan_transport/3, State, Transports),
    schedule_scan(),
    {noreply, NewState};
handle_info(Info, #state{} = State) ->
    ?LOG_NOTICE("received unrecognized info ~P", [Info, 25], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), #state{}) -> term().
terminate(_Reason, #state{transports = Transports, snapshots = Snapshots}) ->
    maps:foreach(
        fun ({_Peer, _Table, _Partition}, #transport{id = ID}) ->
            wa_raft_transport:cancel(ID, terminating)
        end, Transports),
    maps:foreach(
        fun ({Table, Partition, LogPos}, _) ->
            delete_snapshot(Table, Partition, LogPos)
        end, Snapshots).

-spec scan_transport(Key :: key(), Transport :: #transport{}, #state{}) -> #state{}.
scan_transport({_Peer, Table, Partition} = Key, #transport{id = ID, snapshot = LogPos},
               #state{transports = Transports, snapshots = Snapshots} = State) ->
    Status = case wa_raft_transport:transport_info(ID) of
        {ok, #{status := S}} -> S;
        _                    -> undefined
    end,
    case Status =:= requested orelse Status =:= running of
        true ->
            State;
        false ->
            SnapshotKey = {Table, Partition, LogPos},
            NewSnapshots = case Snapshots of
                #{SnapshotKey := 1} ->
                    % try to delete a snapshot if it is the last transport using it
                    delete_snapshot(Table, Partition, LogPos),
                    maps:remove(SnapshotKey, Snapshots);
                #{SnapshotKey := Count} ->
                    % otherwise decrement the reference count for the snapshot
                    Snapshots#{SnapshotKey => Count - 1};
                #{} ->
                    % unexpected that the snapshot is missing, but just ignore
                    Snapshots
            end,
            State#state{transports = maps:remove(Key, Transports), snapshots = NewSnapshots}
    end.

-spec delete_snapshot(Table :: wa_raft:table(), Partition :: wa_raft:partition(),
                      Position :: wa_raft_log:log_pos()) -> ok.
delete_snapshot(Table, Partition, #raft_log_pos{index = Index, term = Term}) ->
    Storage = wa_raft_storage:registered_name(Table, Partition),
    wa_raft_storage:delete_snapshot(Storage, ?SNAPSHOT_NAME(Index, Term)).

-spec schedule_scan() -> reference().
schedule_scan() ->
    erlang:send_after(?SCAN_EVERY_MS, self(), scan).
