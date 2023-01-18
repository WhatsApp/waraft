%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module manages data catchup to followers.

-module(wa_raft_log_catchup).
-compile(warn_missing_spec).
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
    start_catchup_request/6,
    cancel_catchup_request/2,
    is_catching_up/2
]).

%% RAFT log catchup server state
-record(state, {
    name :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),

    server_name :: atom(),
    log_name :: wa_raft_log:log(),

    lockouts = #{} :: #{node() => non_neg_integer()}
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
-define(CATCHUP_REQUEST(FollowerId, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, Witness), {FollowerId, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, Witness}).

%% An entry in the catchup ETS table that indicates an in-progress log
%% catchup to the specified node.
-define(CATCHUP_RECORD(Catchup, Node), {Catchup, Node}).

-spec init_tables() -> term().
init_tables() ->
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
-spec start_catchup_request(Catchup :: atom(), FollowerId :: node(), FollowerLastIndex :: wa_raft_log:log_index(),
                            LeaderTerm :: wa_raft_log:log_term(), LeaderCommitIndex :: wa_raft_log:log_index(), Witness :: boolean()) -> ok.
start_catchup_request(Catchup, FollowerId, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, Witness) ->
    ets:insert(Catchup, ?CATCHUP_REQUEST(FollowerId, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, Witness)),
    ok.

%% Cancel a request to trigger log catchup for a particular follower.
-spec cancel_catchup_request(Catchup :: atom(), FollowerId :: node()) -> ok.
cancel_catchup_request(Catchup, FollowerId) ->
    ets:delete(Catchup, FollowerId),
    ok.

%% Returns whether or not there exists an in-progress log catchup to the
%% specified follower on the specified catchup server.
-spec is_catching_up(Catchup :: atom(), FollowerId :: node()) -> boolean().
is_catching_up(Catchup, FollowerId) ->
    case ets:lookup(?MODULE, Catchup) of
        [?CATCHUP_RECORD(_, FollowerId)] -> true;
        _                                -> false
    end.

%% RAFT log catchup server implementation
-spec init(Options :: #raft_options{}) -> {ok, #state{}, timeout()}.
init(#raft_options{table = Table, partition = Partition, log_name = Log, log_catchup_name = Name, server_name = Server}) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Catchup[~0p] starting for partition ~0p/~0p",
        [Name, Table, Partition], #{domain => [whatsapp, wa_raft]}),

    Name = ets:new(Name, [set, public, named_table, {write_concurrency, true}]),
    State = #state{
        name = Name,
        table = Table,
        partition = Partition,
        server_name = Server,
        log_name = Log
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
            ?CATCHUP_REQUEST(FollowerId, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, Witness) = lists:nth(rand:uniform(length(Requests)), Requests),
            {noreply, send_logs(FollowerId, FollowerLastIndex, LeaderTerm, LeaderCommitIndex, Witness, State), ?CONTINUE_TIMEOUT}
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

-spec send_logs(node(), wa_raft_log:log_index(), wa_raft_log:log_term(), wa_raft_log:log_index(), boolean(), #state{}) -> #state{}.
send_logs(FollowerId, NextLogIndex, LeaderTerm, LeaderCommitIndex, Witness, #state{name = Name, lockouts = Lockouts} = State) ->
    StartMillis = erlang:system_time(millisecond),
    LockoutMillis = maps:get(FollowerId, Lockouts, 0),
    NewState = case LockoutMillis =< StartMillis of
        true ->
            Counters = persistent_term:get(?RAFT_COUNTERS),
            case counters:get(Counters, ?RAFT_GLOBAL_COUNTER_LOG_CATCHUP) < ?RAFT_CONFIG(raft_max_log_catchup, 5) of
                true ->
                    counters:add(Counters, ?RAFT_GLOBAL_COUNTER_LOG_CATCHUP, 1),
                    ets:insert(?MODULE, ?CATCHUP_RECORD(Name, FollowerId)),
                    try send_logs_impl(FollowerId, NextLogIndex, LeaderTerm, LeaderCommitIndex, Witness, State) catch
                        T:E:S ->
                            ?RAFT_COUNT('raft.catchup.error'),
                            ?LOG_ERROR("Catchup[~p, term ~p] bulk logs transfer to ~0p failed with ~0p ~0P at ~p",
                                [Name, LeaderTerm, FollowerId, T, E, S], #{domain => [whatsapp, wa_raft]})
                    after
                        counters:sub(persistent_term:get(?RAFT_COUNTERS), ?RAFT_GLOBAL_COUNTER_LOG_CATCHUP, 1)
                    end,
                    EndMillis = erlang:system_time(millisecond),
                    ?RAFT_GATHER('raft.leader.catchup.duration', (EndMillis - StartMillis) * 1000),
                    State#state{lockouts = Lockouts#{FollowerId => EndMillis + ?LOCKOUT_PERIOD}};
                false ->
                    State
            end;
        false ->
            ?LOG_NOTICE("Catchup[~p, term ~p] skipping bulk logs transfer to ~0p because follower is still under lockout.",
                [Name, LeaderTerm, FollowerId], #{domain => [whatsapp, wa_raft]}),
            State
    end,
    ets:delete(Name, FollowerId),
    ets:delete(?MODULE, Name),
    NewState.

-spec send_logs_impl(node(), wa_raft_log:log_index(), wa_raft_log:log_term(), wa_raft_log:log_index(), boolean(), #state{}) -> term().
send_logs_impl(FollowerId, NextLogIndex, LeaderTerm, LeaderCommitIndex, Witness, #state{name = Name, server_name = Server, log_name = Log} = State) ->
    PrevLogIndex = NextLogIndex - 1,
    {ok, PrevLogTerm} = wa_raft_log:term(Log, PrevLogIndex),

    LogBatchEntries = ?RAFT_CONFIG(raft_catchup_log_batch_entries, 800),
    LogBatchBytes = ?RAFT_CONFIG(raft_catchup_log_batch_bytes, 4 * 1024 * 1024),
    Entries = case Witness of
        false ->
            {ok, E} = wa_raft_log:get(Log, NextLogIndex, LogBatchEntries, LogBatchBytes),
            E;
        true ->
            {ok, T} = wa_raft_log:get_terms(Log, NextLogIndex, LogBatchEntries, LogBatchBytes),
            [{Term, []} || Term <- T]
    end,

    case Entries of
        [] ->
            ?LOG_NOTICE("Catchup[~0p, term ~p] finishes bulk logs transfer to follower ~0p at ~0p.",
                [Name, LeaderTerm, FollowerId, NextLogIndex], #{domain => [whatsapp, wa_raft]});
        _ ->
            % Replicate the log entries to our peer.
            Dest = {Server, FollowerId},
            Command = ?LEGACY_APPEND_ENTRIES_RPC(LeaderTerm, node(), PrevLogIndex, PrevLogTerm, Entries, LeaderCommitIndex, 0),
            Timeout = ?RAFT_CONFIG(raft_catchup_rpc_timeout_ms, 5000),

            try ?RAFT_DISTRIBUTION_MODULE:call(Dest, Command, Timeout) of
                ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(LeaderTerm, FollowerId, PrevLogIndex, true, FollowerEndIndex) ->
                    send_logs_impl(FollowerId, FollowerEndIndex + 1, LeaderTerm, LeaderCommitIndex, Witness, State);
                ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(_Term, _FollowerId, PrevLogIndex, false, _FollowerEndIndex) ->
                    exit(append_failed);
                ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(NewTerm, _FollowerId, PrevLogIndex, _Success, _FollowerEndIndex) ->
                    exit({new_term, NewTerm});
                Other ->
                    exit({bad_response, Other})
            catch
                exit:{Reason, _} -> exit(Reason)
            end
    end.
