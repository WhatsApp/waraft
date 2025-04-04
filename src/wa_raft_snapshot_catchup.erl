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
-include("wa_raft_rpc.hrl").

% erlint-ignore dialyzer_override: improper list expected by gen interface
-dialyzer({no_improper_lists, [handle_cast/2]}).

%% Supervisor callbacks
-export([
    child_spec/0,
    start_link/0
]).

%% Public API
-export([
    which_transports/0
]).

%% Internal API
-export([
    full_replica/5,
    witness_replica/5
]).

%% Snapshot catchup server implementation
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Testing API
-export([
    init_tables/0
]).

-define(SCAN_EVERY_MS, 500).

-define(PENDING_KEY(Name, Node), {pending, Name, Node}).

-define(WHICH_TRANSPORTS, which_transports).
-define(FULL_REPLICA(App, Name, Node, Table, Partition), {full_replica, App, Name, Node, Table, Partition}).
-define(WITNESS_REPLICA(App, Name, Node, Table, Partition), {witness_relica, App, Name, Node, Table, Partition}).

-type key() :: {Name :: atom(), Node :: node()}.
-type snapshot_key() :: {Table :: wa_raft:table(), Partition :: wa_raft:partition(), Position :: wa_raft_log:log_pos()}.

-type which_transports() :: ?WHICH_TRANSPORTS.
-type call() :: which_transports().

-type full_replica() :: ?FULL_REPLICA(atom(), atom(), node(), wa_raft:table(), wa_raft:partition()).
-type witness_replica() :: ?WITNESS_REPLICA(atom(), atom(), node(), wa_raft:table(), wa_raft:partition()).
-type cast() :: full_replica() | witness_replica().

-record(transport, {
    app :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    id :: wa_raft_transport:transport_id(),
    snapshot :: wa_raft_log:log_pos()
}).
-record(state, {
    % currently active transports
    transports = #{} :: #{key() => #transport{}},
    % counts of active transports that are using a particular snapshot
    snapshots = #{} :: #{snapshot_key() => pos_integer()},
    % timestamps (ms) after which transports to previously overloaded nodes can be retried
    overload_backoffs = #{} :: #{node() => integer()},
    % timestamps (ms) after which repeat transports can be retried
    retry_backoffs = #{} :: #{key() => integer()}
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

-spec which_transports() -> [wa_raft_transport:transport_id()].
which_transports() ->
    gen_server:call(?MODULE, ?WHICH_TRANSPORTS).

-spec full_replica(App :: atom(), Name :: atom(), Node :: node(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok.
full_replica(App, Name, Node, Table, Partition) ->
    try
        % Check ETS to avoid putting duplicate requests into the message queue.
        ets:insert_new(?MODULE, {?PENDING_KEY(Name, Node)}) andalso
            gen_server:cast(?MODULE, ?FULL_REPLICA(App, Name, Node, Table, Partition)),
        ok
    catch
        error:badarg ->
            ok
    end.

-spec witness_replica(App :: atom(), Name :: atom(), Node :: node(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok.
witness_replica(App, Name, Node, Table, Partition) ->
    try
        % Check ETS to avoid putting duplicate requests into the message queue.
        ets:insert_new(?MODULE, {?PENDING_KEY(Name, Node)}) andalso
            gen_server:cast(?MODULE, ?WITNESS_REPLICA(App, Name, Node, Table, Partition)),
        ok
    catch
        error:badarg ->
            ok
    end.

-spec init(Args :: term()) -> {ok, #state{}}.
init([]) ->
    process_flag(trap_exit, true),
    init_tables(),
    schedule_scan(),
    {ok, #state{}}.

-spec init_tables() -> ok.
init_tables() ->
    ?MODULE = ets:new(?MODULE, [set, public, named_table]),
    ok.

-spec handle_call(Request :: call(), From :: gen_server:from(), State :: #state{}) -> {noreply, #state{}} | {reply, term(), #state{}}.
handle_call(?WHICH_TRANSPORTS, _From, #state{transports = Transports} = State) ->
    {reply, [ID || #transport{id = ID} <- maps:values(Transports)], State};
handle_call(Request, From, #state{} = State) ->
    ?LOG_NOTICE("received unrecognized call ~P from ~0p", [Request, 25, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request :: cast(), State :: #state{}) -> {noreply, #state{}}.
handle_cast(?FULL_REPLICA(App, Name, Node, Table, Partition), State0) ->
    % Just immediately remove the pending key from the ETS. Doing this here is simpler
    % but permits a bounded number of extra requests to remain in the queue.
    ets:delete(?MODULE, ?PENDING_KEY(Name, Node)),
    Now = erlang:monotonic_time(millisecond),
    case allowed(Now, Name, Node, State0) of
        {true, #state{transports = Transports, snapshots = Snapshots, overload_backoffs = OverloadBackoffs} = State1} ->
            try
                StorageRef = wa_raft_storage:registered_name(Table, Partition),
                {ok, #raft_log_pos{index = Index, term = Term} = LogPos} = wa_raft_storage:create_snapshot(StorageRef),
                Path = ?RAFT_SNAPSHOT_PATH(Table, Partition, Index, Term),
                case wa_raft_transport:start_snapshot_transfer(Node, Table, Partition, LogPos, Path, infinity) of
                    {error, receiver_overloaded} ->
                        ?LOG_NOTICE("destination node ~0p is overloaded, abort new transport for ~0p:~0p and try again later",
                            [Node, Table, Partition], #{domain => [whatsapp, wa_raft]}),
                        NewOverloadBackoff = Now + ?RAFT_SNAPSHOT_CATCHUP_OVERLOADED_BACKOFF_MS(App),
                        NewOverloadBackoffs = OverloadBackoffs#{Node => NewOverloadBackoff},
                        {noreply, State1#state{overload_backoffs = NewOverloadBackoffs}};
                    {ok, ID} ->
                        ?LOG_NOTICE("started sending snapshot for ~0p:~0p at ~0p:~0p over transport ~0p",
                            [Table, Partition, Index, Term, ID], #{domain => [whatsapp, wa_raft]}),
                        NewTransport = #transport{
                            app = App,
                            table = Table,
                            partition = Partition,
                            id = ID,
                            snapshot = LogPos
                        },
                        NewTransports = Transports#{{Name, Node} => NewTransport},
                        NewSnapshots = maps:update_with({Table, Partition, LogPos}, fun(V) -> V + 1 end, 1, Snapshots),
                        {noreply, State1#state{transports = NewTransports, snapshots = NewSnapshots}}
                end
            catch
                _T:_E:S ->
                    ?LOG_ERROR("failed to start accepted snapshot transport of ~0p:~0p to ~0p at ~p",
                        [Table, Partition, Node, S], #{domain => [whatsapp, wa_raft]}),
                    {noreply, State1}
            end;
        {false, State1} ->
            {noreply, State1}
    end;
handle_cast(?WITNESS_REPLICA(App, Name, Node, Table, Partition), State0) ->
    % Just immediately remove the pending key from the ETS. Doing this here is simpler
    % but permits a bounded number of extra requests to remain in the queue.
    ets:delete(?MODULE, ?PENDING_KEY(Name, Node)),
    Now = erlang:monotonic_time(millisecond),
    case allowed(Now, Name, Node, State0) of
        {true, #state{retry_backoffs = RetryBackoffs} = State1} ->
            try
                % If node is a witness, we can avoid creating a full snapshot as witness replicas
                % do not maintain the storage state. Instead, issue a SnapshotAvailableCommand
                % directly to the witness replica.
                StorageRef = wa_raft_storage:registered_name(Table, Partition),
                Position = wa_raft_storage:position(StorageRef),
                ?LOG_NOTICE("sending abridged snapshot at ~0p to witness ~0p on ~0p",
                    [Position, Name, Node], #{domain => [whatsapp, wa_raft]}),

                % Until RAFT server supports casted SnapshotAvailableCommand, create an bogus
                % reference that we pass off as an alias to absorb the response from the peer
                % RAFT server.
                % eqwalizer:ignore - improper list expected by gen interface
                Tag = [alias | make_ref()],
                Request = ?SNAPSHOT_AVAILABLE_COMMAND(undefined, Position),
                erlang:send({Name, Node}, {'$gen_call', {self(), Tag}, Request}, [noconnect, nosuspend]),
                NewRetryBackoff = Now + ?RAFT_SNAPSHOT_CATCHUP_COMPLETED_BACKOFF_MS(App),
                NewRetryBackoffs = RetryBackoffs#{{Name, Node} => NewRetryBackoff},
                {noreply, State1#state{retry_backoffs = NewRetryBackoffs}}
            catch
                _:_:S ->
                    ?LOG_ERROR("failed to send abridged snapshot for ~0p:~0p to ~0p:~n~p",
                        [Table, Partition, Node, S], #{domain => [whatsapp, wa_raft]}),
                    {noreply, State1}
            end;
        {false, State1} ->
            {noreply, State1}
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
        fun ({_Name, _Node}, #transport{id = ID}) ->
            wa_raft_transport:cancel(ID, terminating)
        end, Transports),
    maps:foreach(
        fun ({Table, Partition, LogPos}, _) ->
            delete_snapshot(Table, Partition, LogPos)
        end, Snapshots).

-spec allowed(Now :: integer(), Name :: atom(), Node :: node(), State :: #state{}) -> {boolean(), #state{}}.
allowed(Now, Name, Node, #state{transports = Transports, overload_backoffs = OverloadBackoffs, retry_backoffs = RetryBackoffs} = State0) ->
    Key = {Name, Node},
    Limited = maps:size(Transports) >= ?RAFT_MAX_CONCURRENT_SNAPSHOT_CATCHUP(),
    Exists = maps:is_key(Key, Transports),
    Overloaded = maps:get(Node, OverloadBackoffs, Now) > Now,
    Blocked = maps:get(Key, RetryBackoffs, Now) > Now,
    Allowed = not (Limited orelse Exists orelse Overloaded orelse Blocked),
    State1 = case Overloaded of
        true -> State0;
        false -> State0#state{overload_backoffs = maps:remove(Node, OverloadBackoffs)}
    end,
    State2 = case Blocked of
        true -> State1;
        false -> State1#state{retry_backoffs = maps:remove(Key, RetryBackoffs)}
    end,
    {Allowed, State2}.

-spec scan_transport(Key :: key(), Transport :: #transport{}, #state{}) -> #state{}.
scan_transport(Key, #transport{app = App, id = ID} = Transport, State) ->
    Status = case wa_raft_transport:transport_info(ID) of
        {ok, #{status := S}} -> S;
        _                    -> undefined
    end,
    case Status of
        requested ->
            State;
        running ->
            State;
        completed ->
            finish_transport(Key, Transport, ?RAFT_SNAPSHOT_CATCHUP_COMPLETED_BACKOFF_MS(App), State);
        _Other ->
            finish_transport(Key, Transport, ?RAFT_SNAPSHOT_CATCHUP_FAILED_BACKOFF_MS(App), State)
    end.

-spec finish_transport(Key :: key(), Transport :: #transport{}, Backoff :: pos_integer(), State :: #state{}) -> #state{}.
finish_transport(Key, #transport{table = Table, partition = Partition, snapshot = LogPos}, Backoff,
                 #state{transports = Transports, snapshots = Snapshots, retry_backoffs = RetryBackoffs} = State) ->
    Now = erlang:monotonic_time(millisecond),
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
    NewRetryBackoffs = RetryBackoffs#{Key => Now + Backoff},
    State#state{transports = maps:remove(Key, Transports), snapshots = NewSnapshots, retry_backoffs = NewRetryBackoffs}.

-spec delete_snapshot(Table :: wa_raft:table(), Partition :: wa_raft:partition(),
                      Position :: wa_raft_log:log_pos()) -> ok.
delete_snapshot(Table, Partition, #raft_log_pos{index = Index, term = Term}) ->
    Storage = wa_raft_storage:registered_name(Table, Partition),
    wa_raft_storage:delete_snapshot(Storage, ?SNAPSHOT_NAME(Index, Term)).

-spec schedule_scan() -> reference().
schedule_scan() ->
    erlang:send_after(?SCAN_EVERY_MS, self(), scan).
