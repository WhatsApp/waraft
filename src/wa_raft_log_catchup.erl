%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module manages data catchup to followers.

-module(wa_raft_log_catchup).
-compile(warn_missing_spec_all).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").
-include("wa_raft_rpc.hrl").

%% Private API
-export([
    init_tables/0
]).

%% Supervisor callbacks
-export([
    child_spec/1,
    start_link/1
]).

%% Internal API
-export([
    default_name/2,
    registered_name/2
]).

%% RAFT catchup server implementation
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% API
-export([
    start_catchup_request/5,
    cancel_catchup_request/2,
    is_catching_up/2
]).

%% RAFT log catchup server state
-record(state, {
    application :: atom(),
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    self :: #raft_identity{},
    identifier :: #raft_identifier{},

    distribution_module :: module(),
    log :: wa_raft_log:log(),
    server_name :: atom(),

    lockouts = #{} :: #{#raft_identity{} => non_neg_integer()}
}).

%% Returning a timeout of `0` to a `gen_server` results in the `gen_server`
%% continuing to process any incoming messages in the message queue and
%% triggering a timeout if and only if there are no messages in the message
%% queue. This server uses this to periodically inspect the message queue
%% and perform log catchup work when there are no messages to process.
-define(CONTINUE_TIMEOUT, 0).

%% Time to wait before checking the request ETS table for any incoming
%% catchup requests.
-define(IDLE_TIMEOUT, 100).

%% Time to wait after the completion of a log catchup before starting another
%% log catchup to the same follower. This time should be at least the time
%% it takes for a full heartbeat internval and then the round trip for the
%% heartbeat and response.
-define(LOCKOUT_PERIOD, 1000).

%% An entry in the catchup request ETS table representing a request to
%% trigger log catchup for a particular peer.
-define(CATCHUP_REQUEST(Peer, FollowerLastIndex, LeaderTerm, LeaderCommitIndex), {Peer, FollowerLastIndex, LeaderTerm, LeaderCommitIndex}).

%% An entry in the catchup ETS table that indicates an in-progress log
%% catchup to the specified node.
-define(CATCHUP_RECORD(Catchup, Node), {Catchup, Node}).

%% Global key in persistent_term holding an atomic counters reference for
%% limiting the total number of concurrent catchup by bulk logs transfer.
-define(COUNTER_KEY, {?MODULE, counters}).
%% Index of the counter tracking the number of concurrent bulk logs transfer.
-define(COUNTER_CONCURRENT_CATCHUP, 1).
%% Total number of counters
-define(COUNTER_COUNT, 1).

-spec init_tables() -> term().
init_tables() ->
    persistent_term:put(?COUNTER_KEY, counters:new(?COUNTER_COUNT, [atomics])),
    ?MODULE = ets:new(?MODULE, [set, public, named_table, {read_concurrency, true}]).

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) -> supervisor:startlink_ret().
start_link(#raft_options{log_catchup_name = Name} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%% Submit a request to trigger log catchup for a particular follower starting at the index provided.
-spec start_catchup_request(Catchup :: atom(), Peer :: #raft_identity{}, FollowerLastIndex :: wa_raft_log:log_index(),
                            LeaderTerm :: wa_raft_log:log_term(), LeaderCommitIndex :: wa_raft_log:log_index()) -> ok.
start_catchup_request(Catchup, Peer, FollowerLastIndex, LeaderTerm, LeaderCommitIndex) ->
    ets:insert(Catchup, ?CATCHUP_REQUEST(Peer, FollowerLastIndex, LeaderTerm, LeaderCommitIndex)),
    ok.

%% Cancel a request to trigger log catchup for a particular follower.
-spec cancel_catchup_request(Catchup :: atom(), Peer :: #raft_identity{}) -> ok.
cancel_catchup_request(Catchup, Peer) ->
    ets:delete(Catchup, Peer),
    ok.

%% Returns whether or not there exists an in-progress log catchup to the
%% specified follower on the specified catchup server.
-spec is_catching_up(Catchup :: atom(), Peer :: #raft_identity{}) -> boolean().
is_catching_up(Catchup, Peer) ->
    case ets:lookup(?MODULE, Catchup) of
        [?CATCHUP_RECORD(_, Peer)] -> true;
        _                          -> false
    end.

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT log catchup server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    % elp:ignore W0023 (atoms_exhaustion) - Limit set of inputs
    list_to_atom("raft_log_catchup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT log catchup server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.log_catchup_name
    end.

%% RAFT log catchup server implementation
-spec init(Options :: #raft_options{}) -> {ok, #state{}, timeout()}.
init(#raft_options{application = Application, table = Table, partition = Partition, self = Self,
                   identifier = Identifier, distribution_module = DistributionModule,
                   log_name = LogName, log_module = LogModule,
                   log_catchup_name = Name, server_name = Server}) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Catchup[~0p] starting for partition ~0p/~0p",
        [Name, Table, Partition], #{domain => [whatsapp, wa_raft]}),

    Name = ets:new(Name, [set, public, named_table, {write_concurrency, true}]),
    Log = #raft_log{
        name = LogName,
        application = Application,
        table = Table,
        partition = Partition,
        provider = LogModule
    },
    State = #state{
        application = Application,
        name = Name,
        table = Table,
        partition = Partition,
        self = Self,
        identifier = Identifier,
        distribution_module = DistributionModule,
        log = Log,
        server_name = Server
    },

    {ok, State, ?CONTINUE_TIMEOUT}.

-spec handle_call(Request :: term(), From :: gen_server:from(), State :: #state{}) -> {noreply, #state{}, timeout()}.
handle_call(Request, From, #state{name = Name} = State) ->
    ?LOG_WARNING("Unexpected call ~0P from ~0p on ~0p", [Request, 30, From, Name], #{domain => [whatsapp, wa_raft]}),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, #state{}, timeout()}.
handle_cast(Request, #state{name = Name} = State) ->
    ?LOG_WARNING("Unexpected cast ~0P on ~0p", [Request, 30, Name], #{domain => [whatsapp, wa_raft]}),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec handle_info(Info :: timeout, State :: #state{}) -> {noreply, #state{}, timeout()}.
handle_info(timeout, #state{name = Name} = State) ->
    case ets:tab2list(Name) of
        [] ->
            {noreply, State, ?IDLE_TIMEOUT};
        Requests ->
            % Select a random log catchup request to process.
            ?CATCHUP_REQUEST(Peer, FollowerLastIndex, LeaderTerm, LeaderCommitIndex) = lists:nth(rand:uniform(length(Requests)), Requests),
            NewState = send_logs(Peer, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, State),
            % erlint-ignore garbage_collect
            erlang:garbage_collect(),
            {noreply, NewState, ?CONTINUE_TIMEOUT}
    end;
handle_info(Info, #state{name = Name} = State) ->
    ?LOG_WARNING("Unexpected info ~0P on ~0p", [Info, 30, Name], #{domain => [whatsapp, wa_raft]}),
    {noreply, State, ?CONTINUE_TIMEOUT}.

-spec terminate(Reason :: term(), State :: #state{}) -> term().
terminate(_Reason, #state{name = Name}) ->
    ets:delete(?MODULE, Name).

%% =======================================================================
%% Private functions - Send logs to follower
%%

-spec send_logs(#raft_identity{}, wa_raft_log:log_index(), wa_raft_log:log_term(), wa_raft_log:log_index(), #state{}) -> #state{}.
send_logs(Peer, NextLogIndex, LeaderTerm, LeaderCommitIndex, #state{name = Name, lockouts = Lockouts} = State) ->
    StartMillis = erlang:system_time(millisecond),
    LockoutMillis = maps:get(Peer, Lockouts, 0),
    NewState = case LockoutMillis =< StartMillis of
        true ->
            Counters = persistent_term:get(?COUNTER_KEY),
            case counters:get(Counters, ?COUNTER_CONCURRENT_CATCHUP) < ?RAFT_MAX_CONCURRENT_LOG_CATCHUP() of
                true ->
                    counters:add(Counters, ?COUNTER_CONCURRENT_CATCHUP, 1),
                    ets:insert(?MODULE, ?CATCHUP_RECORD(Name, Peer)),
                    try send_logs_impl(Peer, NextLogIndex, LeaderTerm, LeaderCommitIndex, State) catch
                        T:E:S ->
                            ?RAFT_COUNT('raft.catchup.error'),
                            ?LOG_ERROR("Catchup[~p, term ~p] bulk logs transfer to ~0p failed with ~0p ~0p at ~p",
                                [Name, LeaderTerm, Peer, T, E, S], #{domain => [whatsapp, wa_raft]})
                    after
                        counters:sub(persistent_term:get(?COUNTER_KEY), ?COUNTER_CONCURRENT_CATCHUP, 1)
                    end,
                    EndMillis = erlang:system_time(millisecond),
                    ?RAFT_GATHER('raft.leader.catchup.duration', (EndMillis - StartMillis) * 1000),
                    State#state{lockouts = Lockouts#{Peer => EndMillis + ?LOCKOUT_PERIOD}};
                false ->
                    State
            end;
        false ->
            ?LOG_NOTICE("Catchup[~p, term ~p] skipping bulk logs transfer to ~0p because follower is still under lockout.",
                [Name, LeaderTerm, Peer], #{domain => [whatsapp, wa_raft]}),
            State
    end,
    ets:delete(Name, Peer),
    ets:delete(?MODULE, Name),
    NewState.

-spec send_logs_impl(#raft_identity{}, wa_raft_log:log_index(), wa_raft_log:log_term(), wa_raft_log:log_index(), #state{}) -> term().
send_logs_impl(#raft_identity{node = PeerNode} = Peer, NextLogIndex, LeaderTerm, LeaderCommitIndex,
               #state{application = App, name = Name, self = Self, identifier = Identifier, distribution_module = DistributionModule, server_name = Server, log = Log} = State) ->
    PrevLogIndex = NextLogIndex - 1,
    {ok, PrevLogTerm} = wa_raft_log:term(Log, PrevLogIndex),

    LogBatchEntries = ?RAFT_CATCHUP_MAX_ENTRIES_PER_BATCH(App),
    LogBatchBytes = ?RAFT_CATCHUP_MAX_BYTES_PER_BATCH(App),
    {ok, Entries} = wa_raft_log:get(Log, NextLogIndex, LogBatchEntries, LogBatchBytes),

    case Entries of
        [] ->
            ?LOG_NOTICE("Catchup[~0p, term ~p] finishes bulk logs transfer to follower ~0p at ~0p.",
                [Name, LeaderTerm, Peer, NextLogIndex], #{domain => [whatsapp, wa_raft]});
        _ ->
            % Replicate the log entries to our peer.
            Dest = {Server, PeerNode},
            Command = wa_raft_server:make_rpc(Self, LeaderTerm, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, LeaderCommitIndex, 0)),
            Timeout = ?RAFT_CATCHUP_HEARTBEAT_TIMEOUT(),

            try wa_raft_server:parse_rpc(Self, DistributionModule:call(Dest, Identifier, Command, Timeout)) of
                {LeaderTerm, _, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, true, FollowerEndIndex)} ->
                    send_logs_impl(Peer, FollowerEndIndex + 1, LeaderTerm, LeaderCommitIndex, State);
                {LeaderTerm, _, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, false, _FollowerEndIndex)} ->
                    exit(append_failed);
                {LeaderTerm, _, Other} ->
                    exit({bad_response, Other});
                {NewTerm, _, _} ->
                    exit({new_term, NewTerm})
            catch
                % Suppress any `gen_server:call` regurgitation of the potentially large append payload into
                % an error report to avoid excessively large error reports.
                exit:{Reason, _} -> exit(Reason)
            end
    end.
