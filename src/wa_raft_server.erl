%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements RPC of raft consensus protocol. See raft spec
%%% on https://raft.github.io/. A wa_raft instance is a participant in
%%% a consensus group. Each participant plays a certain role (follower,
%%% leader or candidate). The mission of a consensus group is to
%%% implement a replicated state machine in a distributed cluster.

-module(wa_raft_server).
-compile(warn_missing_spec).
-behaviour(gen_statem).

%% OTP supervisor
-export([
    child_spec/1,
    start_link/1
]).

%% API
-export([
    make_config/1,
    make_config/2,
    status/1,
    status/2,
    membership/1,
    stop/1,
    commit/2,
    read/2
]).

%% gen_statem callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3,
    code_change/4,
    stalled/3,
    leader/3,
    follower/3,
    candidate/3,
    disabled/3
]).

%% Internal API
-export([
    snapshot_available/3,
    promote/2,
    promote/3,
    promote/4,
    resign/1,
    refresh_config/1,
    adjust_membership/3,
    handover/1,
    handover/2,
    handover_candidates/1,
    disable/2,
    enter_witness/1,
    enable/1,
    cast/3,
    witness/3
]).

%% Exports for CT tests
-ifdef(TEST).
-export([
    compute_quorum/3,
    config/1,
    max_index_to_apply/3
]).
-endif.

-export_type([
    state/0,
    peer/0,
    membership/0,
    config/0,
    status/0
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").
-include("wa_raft_rpc.hrl").

%% Section 5.2. Randomized election timeout for fast election and to avoid split votes
-define(ELECTION_TIMEOUT, {state_timeout, random_election_timeout(), election}).
-define(WITNESS_TIMEOUT, ?ELECTION_TIMEOUT).
%% Heartbeat interval in ms. Leader sends periodic heartbeats to maintain authority
-define(HEARTBEAT_TIMEOUT, {state_timeout, ?RAFT_CONFIG(raft_heartbeat_interval_ms, 120), heartbeat}).
-define(COMMIT_BATCH_TIMEOUT, {state_timeout, ?RAFT_CONFIG(raft_commit_batch_interval_ms, 2), batch_commit}).

%% Time before considering handover failed in ms.
-define(RAFT_HANDOVER_TIMEOUT_MS(), ?RAFT_CONFIG(raft_handover_timeout_ms, 600)).

-define(MAX_LOG_APPLY_BATCH_SIZE, ?RAFT_CONFIG(raft_apply_log_batch_size, 200)).

%% Maximum number of log entries to include in a Handover RPC to pass
%% leadership to another peer. A limit is enforced to prevent a handover
%% trying to send huge numbers of logs to catchup a peer during handover.
-define(RAFT_MAX_HANDOVER_LOG_ENTRIES(), ?RAFT_CONFIG(raft_max_handover_log_entries, 200)).
%% Maximum total byte size of log entries to include in a Handover RPC.
-define(RAFT_MAX_HANDOVER_LOG_SIZE(), ?RAFT_CONFIG(raft_max_handover_log_size, 50 * 1024 * 1024)).

-type state() ::
    stalled |
    leader |
    follower |
    candidate |
    disabled |
    witness.

-type peer() :: {Name :: atom(), Node :: node()}.
-type membership() :: [peer()].
-opaque config() ::
    #{
        version := integer(),
        membership => membership(),
        witness => membership()
    }.

-type status() :: [status_element()].
-type status_element() ::
      {state, state()}
    | {id, atom()}
    | {peers, [{atom(), {node(), atom()}}]}
    | {partition, wa_raft:partition()}
    | {data_dir, file:filename_all()}
    | {current_term, wa_raft_log:log_term()}
    | {voted_for, node()}
    | {commit_index, wa_raft_log:log_index()}
    | {last_applied, wa_raft_log:log_index()}
    | {leader_id, node()}
    | {next_index, wa_raft_log:log_index()}
    | {match_index, wa_raft_log:log_index()}
    | {log_module, module()}
    | {log_first, wa_raft_log:log_index()}
    | {log_last, wa_raft_log:log_index()}
    | {votes, #{node() => boolean()}}
    | {inflight_applies, non_neg_integer()}
    | {disable_reason, string()}
    | {witness, boolean()}
    | {config, config()}
    | {config_index, wa_raft_log:log_index()}.

-type event() :: rpc() | normalized_procedure_call() | command() | timeout_type().

-type rpc() :: rpc_id() | rpc_named().
-type rpc_id() :: ?RAFT_RPC(atom(), wa_raft_log:log_term(), node(), undefined | tuple()).
-type rpc_named() :: ?RAFT_NAMED_RPC(atom(), wa_raft_log:log_term(), atom(), node(), undefined | tuple()).

-type command() :: commit_command() | read_command() | status_command() | promote_command() | resign_command() | adjust_membership_command() | snapshot_available_command() |
                   handover_candidates_command() | handover_command() | enable_command() | disable_command() | witness_command().
-type commit_command()              :: ?COMMIT_COMMAND(wa_raft_acceptor:op()).
-type read_command()                :: ?READ_COMMAND(wa_raft_acceptor:read_op()).
-type status_command()              :: ?STATUS_COMMAND.
-type promote_command()             :: ?PROMOTE_COMMAND(wa_raft_log:log_term(), boolean(), config() | undefined).
-type resign_command()              :: ?RESIGN_COMMAND.
-type adjust_membership_command()   :: ?ADJUST_MEMBERSHIP_COMMAND(membership_action(), peer() | undefined).
-type snapshot_available_command()  :: ?SNAPSHOT_AVAILABLE_COMMAND(string(), wa_raft_log:log_pos()).
-type handover_candidates_command() :: ?HANDOVER_CANDIDATES_COMMAND.
-type handover_command()            :: ?HANDOVER_COMMAND(node()).
-type enable_command()              :: ?ENABLE_COMMAND.
-type disable_command()             :: ?DISABLE_COMMAND(term()).
-type witness_command()             :: ?WITNESS_COMMAND().

-type timeout_type() :: election | heartbeat.

-type membership_action() :: add | add_witness | remove | remove_witness | refresh.

%% ==================================================
%%  OTP Supervision Callbacks
%% ==================================================

-spec child_spec(RaftArgs :: wa_raft:args()) -> supervisor:child_spec().
child_spec(RaftArgs) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [RaftArgs]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link(RaftArgs :: wa_raft:args()) -> {ok, Pid :: pid()} | ignore | wa_raft:error().
start_link(#{table := Table, partition := Partition} = RaftArgs) ->
    gen_statem:start_link({local, ?RAFT_SERVER_NAME(Table, Partition)}, ?MODULE, RaftArgs, []).

%% ==================================================
%%  RAFT Server Internal API
%% ==================================================
-spec make_config(Membership :: membership()) -> config().
make_config(Membership) ->
    #{
        version => ?RAFT_CONFIG_CURRENT_VERSION,
        membership => Membership
    }.

-spec make_config(Membership :: membership(), Witness :: membership()) -> config().
make_config(Membership, Witness) ->
    #{
        version => ?RAFT_CONFIG_CURRENT_VERSION,
        membership => Membership,
        witness => Witness
    }.

%% Commit an op to the consensus group.
-spec commit(Pid :: atom() | pid(), Op :: wa_raft_acceptor:op()) -> ok | wa_raft:error().
commit(Pid, Op) ->
    gen_server:cast(Pid, ?COMMIT_COMMAND(Op)).

%% Strongly-consistent read
-spec read(Pid :: atom() | pid(), Op :: wa_raft_acceptor:op()) -> ok | wa_raft:error().
read(Pid, Op) ->
    gen_server:cast(Pid, ?READ_COMMAND(Op)).

-spec status(ServerRef :: gen_server:server_ref()) -> status().
status(ServerRef) ->
    gen_server:call(ServerRef, ?STATUS_COMMAND, ?RPC_CALL_TIMEOUT_MS).

-spec status
    (ServerRef :: gen_server:server_ref(), Key :: atom()) -> Value :: eqwalizer:dynamic();
    (ServerRef :: gen_server:server_ref(), Keys :: [atom()]) -> Value :: [eqwalizer:dynamic()].
status(ServerRef, Key) when is_atom(Key) ->
    hd(status(ServerRef, [Key]));
status(ServerRef, Keys) when is_list(Keys) ->
    case status(ServerRef) of
        [_|_] = Status ->
            FilterFun =
                fun({Key, Value}) ->
                    case lists:member(Key, Keys) of
                        true -> {true, Value};
                        false -> false
                    end
                end,
            lists:filtermap(FilterFun, Status);
        _ ->
            lists:duplicate(length(Keys), undefined)
    end.

-spec membership(Service :: pid() | atom() | {atom(), node()}) -> undefined | membership().
membership(Service) ->
    case status(Service, config) of
        #{membership := Membership} -> Membership;
        _                           -> undefined
    end.

-spec stop(Pid :: atom() | pid()) -> ok.
stop(Pid) ->
    gen_statem:stop(Pid).

% An API that uses storage timeout since it interacts with storage layer directly
-spec snapshot_available(Pid :: atom() | pid() | {atom(), atom()}, Root :: string(), Pos :: wa_raft_log:log_pos()) -> ok | wa_raft:error().
snapshot_available(Pid, Root, Pos) ->
    gen_server:call(Pid, ?SNAPSHOT_AVAILABLE_COMMAND(Root, Pos), ?STORAGE_CALL_TIMEOUT_MS).

%% TODO(hsun324): Update promote to enable setting a RAFT cluster membership
%%                in order to be able to bootstrap new RAFT clusters.
-spec promote(Pid :: atom() | pid(), Term :: pos_integer()) -> ok | wa_raft:error().
promote(Pid, Term) ->
    promote(Pid, Term, false).

-spec promote(Pid :: atom() | pid(), Term :: pos_integer(), Force :: boolean()) -> ok | wa_raft:error().
promote(Pid, Term, Force) ->
    promote(Pid, Term, Force, undefined).

-spec promote(Pid :: atom() | pid(), Term :: pos_integer(), Force :: boolean(), Config :: undefined | config()) -> ok | wa_raft:error().
promote(Pid, Term, Force, Config) ->
    gen_server:call(Pid, ?PROMOTE_COMMAND(Term, Force, Config), ?RPC_CALL_TIMEOUT_MS).

-spec resign(Pid :: atom() | pid()) -> ok | wa_raft:error().
resign(Pid) ->
    gen_server:call(Pid, ?RESIGN_COMMAND, ?RPC_CALL_TIMEOUT_MS).

-spec refresh_config(Name :: atom() | pid()) -> {ok, Pos :: wa_raft_log:log_pos()} | wa_raft:error().
refresh_config(Name) ->
    gen_server:call(Name, ?ADJUST_MEMBERSHIP_COMMAND(refresh, undefined), ?RPC_CALL_TIMEOUT_MS).

-spec adjust_membership(Name :: atom() | pid(), Action :: add | remove | add_witness | remove_witness, Peer :: peer()) -> {ok, Pos :: wa_raft_log:log_pos()} | wa_raft:error().
adjust_membership(Name, Action, Peer) ->
    gen_server:call(Name, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer), ?RPC_CALL_TIMEOUT_MS).

-spec handover_candidates(Name :: atom() | pid()) -> {ok, Candidates :: [node()]} | wa_raft:error().
handover_candidates(Name) ->
    gen_server:call(Name, ?HANDOVER_CANDIDATES_COMMAND, ?RPC_CALL_TIMEOUT_MS).

%% Instruct a RAFT leader to attempt a handover to a random handover candidate.
-spec handover(Name :: atom() | pid()) -> ok.
handover(Name) ->
    gen_server:cast(Name, ?HANDOVER_COMMAND(undefined)).

%% Instruct a RAFT leader to attempt a handover to the specified peer node.
%% If an `undefined` peer node is specified, then handover to a random handover candidate.
%% Returns which peer node the handover was sent to or otherwise an error.
-spec handover(Name :: atom() | pid(), Peer :: node() | undefined) -> {ok, Peer :: node()} | wa_raft:error().
handover(Name, Peer) ->
    gen_server:call(Name, ?HANDOVER_COMMAND(Peer), ?RPC_CALL_TIMEOUT_MS).

-spec disable(Name :: atom() | pid(), Reason :: term()) -> ok | {error, ErrorReason :: atom()}.
disable(Name, Reason) ->
    gen_server:cast(Name, ?DISABLE_COMMAND(Reason)).

-spec enter_witness(Name :: atom() | pid()) -> ok | {error, ErrorReason :: atom()}.
enter_witness(Name) ->
    gen_server:cast(Name, ?WITNESS_COMMAND()).

-spec enable(Name :: atom() | pid()) -> ok | {error, ErrorReason :: atom()}.
enable(Name) ->
    gen_server:call(Name, ?ENABLE_COMMAND, ?RPC_CALL_TIMEOUT_MS).

%% ==================================================
%%  gen_statem Callbacks
%% ==================================================

%% gen_statem callbacks
-spec init(wa_raft:args()) -> gen_statem:init_result(state()).
init(#{table := Table, partition := Partition} = RaftArgs) ->
    process_flag(trap_exit, true),

    Name = ?RAFT_SERVER_NAME(Table, Partition),
    Witness = maps:get(witness, RaftArgs, false),
    Storage = ?RAFT_STORAGE_NAME(Table, Partition),

    ?LOG_NOTICE("Server[~p] starting with options ~p", [Name, RaftArgs], #{domain => [whatsapp, wa_raft]}),

    % Open storage and the log
    {ok, Last} = wa_raft_storage:open(Storage),
    {ok, View} = wa_raft_log:open(?RAFT_LOG_NAME(Table, Partition), Last),

    DataDir = ?ROOT_DIR(Table, Partition),
    State0 = #raft_state{
        name = Name,
        table = Table,
        partition = Partition,
        data_dir = DataDir,
        log_view = View,
        storage = Storage,
        catchup = ?RAFT_LOG_CATCHUP(Table, Partition),
        current_term = Last#raft_log_pos.term,
        commit_index = Last#raft_log_pos.index,
        last_applied = Last#raft_log_pos.index,
        witness = Witness
    },

    State1 = load_config(State0),
    rand:seed(exsp, {erlang:monotonic_time(), erlang:time_offset(), erlang:unique_integer()}),
    % TODO(hsun324): When we have proper error handling for data corruption vs. stalled server
    %                then handle {error, Reason} type returns from load_state.
    State2 = case wa_raft_durable_state:load(State1) of
        {ok, NewState} -> NewState;
        _              -> State1
    end,
    % 1. Begin as disabled if a disable reason is set
    % 2. Begin as witness if configured
    % 3. Begin as stalled if there is no data
    % 4. Begin as follower otherwise
    case {State2#raft_state.last_applied, State2#raft_state.disable_reason, Witness} of
        {_, undefined, true} -> {ok, witness, State2};
        {0, undefined, _}    -> {ok, stalled, State2};
        {_, undefined, _}    -> {ok, follower, State2};
        {_, _, _}            -> {ok, disabled, State2}
    end.

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() ->
    [state_functions, state_enter].

%%-------------------------------------------------------------------
%% Procedure Call Marshalling
%%-------------------------------------------------------------------

%% A request to execute a particular procedure. This request could
%% have been issued locally or as a result of a remote procedure
%% call. The peer (if exists and could be oneself) that issued the
%% procedure call will be provided as the sender.
-define(PROCEDURE_CALL(Type, Term, Sender, Payload), {procedure, Type, Term, Sender, Payload}).
-define(APPEND_ENTRIES_RPC(Term, Sender, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex), ?PROCEDURE_CALL(?APPEND_ENTRIES, Term, Sender, {PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex})).
-define(APPEND_ENTRIES_RESPONSE_RPC(Term, Sender, PrevLogIndex, Success, LastIndex),                  ?PROCEDURE_CALL(?APPEND_ENTRIES_RESPONSE, Term, Sender, {PrevLogIndex, Success, LastIndex})).
-define(REQUEST_VOTE_RPC(Term, Sender, ElectionType, LastLogIndex, LastLogTerm),                      ?PROCEDURE_CALL(?REQUEST_VOTE, Term, Sender, {ElectionType, LastLogIndex, LastLogTerm})).
-define(VOTE_RPC(Term, Sender, Vote),                                                                 ?PROCEDURE_CALL(?VOTE, Term, Sender, {Vote})).
-define(HANDOVER_RPC(Term, Sender, Ref, PrevLogIndex, PrevLogTerm, Entries),                          ?PROCEDURE_CALL(?HANDOVER, Term, Sender, {Ref, PrevLogIndex, PrevLogTerm, Entries})).
-define(HANDOVER_FAILED_RPC(Term, Sender, Ref),                                                       ?PROCEDURE_CALL(?HANDOVER_FAILED, Term, Sender, {Ref})).
-define(NOTIFY_TERM_RPC(Term, SenderName, SenderNode),                                                ?PROCEDURE_CALL(?NOTIFY_TERM, Term, {SenderName, SenderNode}, {})).

%% TODO(hsun324): T112326686
%%   - Remove unnecessary term from procedure structure and rename without "RPC".
%%   - Switch to some identity record to unify node() and peer().
-type procedure_call() :: ?PROCEDURE_CALL(atom(), wa_raft_log:log_term(), peer() | node(), tuple()).
-type normalized_procedure_call() :: append_entries() | append_entries_response() | request_vote() | vote() | handover() | handover_failed() | notify_term().
-type append_entries()          :: ?APPEND_ENTRIES_RPC         (wa_raft_log:log_term(), node(), wa_raft_log:log_index(), wa_raft_log:log_term(), [wa_raft_log:log_entry()], wa_raft_log:log_index(), wa_raft_log:log_index()).
-type append_entries_response() :: ?APPEND_ENTRIES_RESPONSE_RPC(wa_raft_log:log_term(), node(), wa_raft_log:log_index(), boolean(), wa_raft_log:log_index()).
-type request_vote()            :: ?REQUEST_VOTE_RPC           (wa_raft_log:log_term(), node(), election_type(), wa_raft_log:log_index(), wa_raft_log:log_term()).
-type vote()                    :: ?VOTE_RPC                   (wa_raft_log:log_term(), node(), boolean()).
-type handover()                :: ?HANDOVER_RPC               (wa_raft_log:log_term(), node(), reference(), wa_raft_log:log_index(), wa_raft_log:log_term(), [wa_raft_log:log_entry()]).
-type handover_failed()         :: ?HANDOVER_FAILED_RPC        (wa_raft_log:log_term(), node(), reference()).
-type notify_term()             :: ?NOTIFY_TERM_RPC            (wa_raft_log:log_term(), atom(), node()).

-type election_type() :: normal | force | allowed.

-spec protocol() -> #{atom() => procedure_call()}.
protocol() ->
    #{
        ?APPEND_ENTRIES          => ?APPEND_ENTRIES_RPC(0, undefined, 0, 0, [], 0, 0),
        ?APPEND_ENTRIES_RESPONSE => ?APPEND_ENTRIES_RESPONSE_RPC(0, undefined, 0, false, 0),
        ?REQUEST_VOTE            => ?REQUEST_VOTE_RPC(0, undefined, normal, 0, 0),
        ?VOTE                    => ?VOTE_RPC(0, undefined, false),
        ?HANDOVER                => ?HANDOVER_RPC(0, undefined, undefined, 0, 0, []),
        ?HANDOVER_FAILED         => ?HANDOVER_FAILED_RPC(0, undefined, undefined)
    }.

-spec handle_rpc(Type :: gen_statem:event_type(), RPC :: rpc(), State :: state(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
handle_rpc(Type, ?RAFT_RPC(Procedure, Term, SenderId, Payload) = Event, State, #raft_state{name = Name} = Data) ->
    handle_rpc_impl(Type, Event, Procedure, Term, {Name, SenderId}, Payload, State, Data);
handle_rpc(Type, ?RAFT_NAMED_RPC(Procedure, Term, SenderName, SenderNode, Payload) = Event, State, Data) ->
    handle_rpc_impl(Type, Event, Procedure, Term, {SenderName, SenderNode}, Payload, State, Data);
handle_rpc(_Type, RPC, State, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?RAFT_COUNT({'raft', State, 'rpc.unrecognized'}),
    ?LOG_NOTICE("~0p[~0p, term ~0p] receives unknown RPC format ~P",
        [State, Name, CurrentTerm, RPC, 25], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

-spec handle_rpc_impl(Type :: gen_statem:event_type(), Event :: rpc(), Key :: atom(), Term :: wa_raft_log:log_term(), Sender :: peer(),
                      Payload :: undefined | tuple(), State :: state(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Protocol] Undefined payload should be treated as an empty tuple
handle_rpc_impl(Type, Event, Key, Term, Sender, undefined, State, Data) ->
    handle_rpc_impl(Type, Event, Key, Term, Sender, {}, State, Data);
% TODO(hsun324): T112326686 - remove special case for NotifyTerm with peer()
handle_rpc_impl(Type, _Event, ?NOTIFY_TERM, Term, {SenderName, SenderNode}, _Payload, _State, _Data) ->
    {keep_state_and_data, {next_event, Type, ?NOTIFY_TERM_RPC(Term, SenderName, SenderNode)}};
%% [Protocol] Convert any valid remote procedure call to the appropriate local procedure call.
handle_rpc_impl(Type, _Event, Key, Term, {_, SenderNode}, Payload, State, #raft_state{name = Name, current_term = CurrentTerm}) when is_tuple(Payload) ->
    case protocol() of
        #{Key := ?PROCEDURE_CALL(Procedure, _ZeroTerm, _NoIdentity, Defaults)} ->
            {keep_state_and_data, {next_event, Type, ?PROCEDURE_CALL(Procedure, Term, SenderNode, defaultize_payload(Defaults, Payload))}};
        #{} ->
            ?RAFT_COUNT({'raft', State, 'rpc.unknown'}),
            ?LOG_NOTICE("~0p[~0p, term ~0p] receives unknown RPC type ~0p with payload ~0P",
                [State, Name, CurrentTerm, Key, Payload, 25], #{domain => [whatsapp, wa_raft]}),
            keep_state_and_data
    end.

-spec defaultize_payload(tuple(), tuple()) -> tuple().
defaultize_payload(Defaults, Payload) ->
    defaultize_payload(Defaults, Payload, size(Defaults), size(Payload)).

-spec defaultize_payload(tuple(), tuple(), non_neg_integer(), non_neg_integer()) -> tuple().
defaultize_payload(_Defaults, Payload, N, N) ->
    Payload;
defaultize_payload(Defaults, Payload, N, M) when N > M ->
    defaultize_payload(Defaults, erlang:insert_element(M + 1, Payload, element(M + 1, Defaults)), N, M + 1);
defaultize_payload(Defaults, Payload, N, M) when N < M ->
    defaultize_payload(Defaults, erlang:delete_element(M, Payload), N, M - 1).

%% ==================================================
%%  RAFT Server state-specific event callbacks
%% ==================================================

%% [RAFT Extension]
%% Stalled - an extension of raft protocol to handle the situation that a node join after being wiped(tupperware preemption).
%%           This node should not participate any election. Otherwise it may vote yes to a lagging node and sabotage data integrity.
-spec stalled(Type :: enter, PreviousState :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
             (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
stalled(enter, _OldStateName, State) ->
    wa_raft_info:set_state(State#raft_state.table, State#raft_state.partition, ?FUNCTION_NAME),
    wa_raft_info:set_stale(true, State),
    State1 = reset_state(State),
    {keep_state, State1};

%% [Protocol] Handle any RPCs
stalled(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [RequestVote RPC] Discard normal RequestVote RPCs when we have an active leader.
stalled(Type, ?REQUEST_VOTE_RPC(_Term, _SenderNode, normal, _LastLogIndex, _LastLogTerm) = Event, State) ->
    filter_vote_request(?FUNCTION_NAME, Type, Event, State);

%% [General Rules] Discard any incoming RPCs with a term older than the current term
stalled(_Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload),
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term < CurrentTerm ->
    ?LOG_NOTICE("Stalled[~p, term ~p] received stale ~p from ~p with old term ~p. Dropping.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?NOTIFY_TERM_RPC(CurrentTerm, Name, node()), State),
    {keep_state, State};
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
stalled(Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload) = Event,
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term > CurrentTerm ->
    ?LOG_NOTICE("Stalled[~p, term ~p] received ~p from ~p with new term ~p. Advancing.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, advance_term(Term, State), {next_event, Type, Event}};

%% [AppendEntries] If we haven't discovered leader for this term, record it
stalled(_Type, ?APPEND_ENTRIES_RPC(CurrentTerm, LeaderId, PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex),
        #raft_state{current_term = CurrentTerm, leader_id = undefined} = State) ->
    NewState = State#raft_state{leader_id = LeaderId, leader_heartbeat_ts = erlang:system_time(millisecond)},
    notify_leader_change(NewState),
    send_rpc(LeaderId, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, false, 0), NewState),
    {keep_state, NewState};

%% [AppendEntries] Otherwise, stalled nodes always discard AppendEntries
stalled(_Type, ?APPEND_ENTRIES_RPC(CurrentTerm, LeaderId, PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex),
        #raft_state{current_term = CurrentTerm} = State) ->
    NewState = State#raft_state{leader_heartbeat_ts = erlang:system_time(millisecond)},
    send_rpc(LeaderId, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, false, 0), NewState),
    {keep_state, NewState};

%% [NotifyTerm] Drop NotifyTerm RPCs with matching term
stalled(_Type, ?NOTIFY_TERM_RPC(CurrentTerm, _SenderName, _SenderNode), #raft_state{current_term = CurrentTerm}) ->
    keep_state_and_data;

%% [Fallback] Node receives unrecognized RPC
stalled(_Type, ?RAFT_RPC(RPCType, CurrentTerm, SenderId, _Payload),
       #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Stalled[~p, term ~p] receives unrecognized RPC ~p from ~p.",
        [Name, CurrentTerm, RPCType, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

stalled({call, From}, ?SNAPSHOT_AVAILABLE_COMMAND(Root, #raft_log_pos{index = SnapshotIndex, term = SnapshotTerm} = SnapshotPos),
        #raft_state{name = Name, data_dir = DataDir, log_view = View0, storage = Storage,
                    current_term = CurrentTerm, last_applied = LastApplied} = State0) ->
    case SnapshotIndex > LastApplied orelse LastApplied =:= 0 of
        true ->
            Path = filename:join(DataDir, ?SNAPSHOT_NAME(SnapshotIndex, SnapshotTerm)),
            catch filelib:ensure_dir(Path),
            case prim_file:rename(Root, Path) of
                ok ->
                    ?LOG_NOTICE("Stalled[~p, term ~p] applying snapshot ~p:~p",
                        [Name, CurrentTerm, SnapshotIndex, SnapshotTerm], #{domain => [whatsapp, wa_raft]}),
                    ok = wa_raft_storage:open_snapshot(Storage, SnapshotPos),
                    {ok, View1} = wa_raft_log:reset(View0, SnapshotPos),
                    State1 = State0#raft_state{log_view = View1, last_applied = SnapshotIndex, commit_index = SnapshotIndex},
                    State2 = load_config(State1),
                    NewTerm = max(CurrentTerm, SnapshotTerm),
                    ?LOG_NOTICE("Stalled[~p, term ~p] switching to follower after installing snapshot at ~p:~p.",
                        [Name, CurrentTerm, SnapshotIndex, SnapshotTerm], #{domain => [whatsapp, wa_raft]}),
                    % At this point, we assume that we received some cluster membership configuration from
                    % our peer so it is safe to transition to an operational state.
                    {next_state, follower, advance_term(NewTerm, State2), {reply, From, ok}};
                {error, Reason} ->
                    ?LOG_WARNING("Stalled[~p, term ~p] failed to rename available snapshot ~p to ~p due to ~p",
                        [Name, CurrentTerm, Root, Path, Reason], #{domain => [whatsapp, wa_raft]}),
                    {keep_state_and_data, {reply, From, {error, Reason}}}
            end;
        false ->
            ?LOG_NOTICE("Stalled[~p, term ~p] ignoring available snapshot ~p:~p with index not past ours (~p)",
                [Name, CurrentTerm, SnapshotIndex, SnapshotTerm, LastApplied], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;

stalled(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

stalled(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Stalled[~p, term ~p] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, unsupported}),
    keep_state_and_data.

%% [RAFT Core]
%% Leader - In the RAFT cluster, the leader node is a node agreed upon by the cluster to be
%%          responsible for accepting and replicating commits and maintaining the integrity
%%          of the underlying state machine.
-spec leader(Type :: enter, PreviousState :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
            (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Leader] changing to leader
leader(enter, OldStateName,
       #raft_state{name = Name, current_term = CurrentTerm, storage = Storage, log_view = View} = State) ->
    ?LOG_NOTICE("~p becomes leader[term ~p, last log ~p]. Previous state is ~p.", [Name, CurrentTerm, wa_raft_log:last_index(View), OldStateName], #{domain => [whatsapp, wa_raft]}),
    ?RAFT_COUNT('raft.leader.elected'),
    wa_raft_info:set_state(State#raft_state.table, State#raft_state.partition, ?FUNCTION_NAME),
    wa_raft_info:set_stale(false, State),
    wa_raft_storage:cancel(Storage),
    State1 = reset_state(State),
    State2 = reset_leader_state(State1),
    State3 = append_entries_to_followers(State2),
    State4 = apply_single_node_cluster(State3), % apply immediately for single node cluster
    {keep_state, State4, ?HEARTBEAT_TIMEOUT};

%% [Protocol] Handle any RPCs
leader(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [RequestVote RPC] Discard normal RequestVote RPCs when we have an active leader.
leader(Type, ?REQUEST_VOTE_RPC(_Term, _SenderNode, normal, _LastLogIndex, _LastLogTerm) = Event, State) ->
    filter_vote_request(?FUNCTION_NAME, Type, Event, State);

%% [General Rules] Discard any incoming RPCs with a term older than the current term
leader(_Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload),
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term < CurrentTerm ->
    ?LOG_NOTICE("Leader[~p, term ~p] received stale ~p from ~p with old term ~p. Dropping.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?NOTIFY_TERM_RPC(CurrentTerm, Name, node()), State),
    {keep_state, State};
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
leader(Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload) = Event,
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term > CurrentTerm ->
    ?LOG_NOTICE("Leader[~p, term ~p] received ~p from ~p with new term ~p. Advancing and switching to follower.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, advance_term(Term, State), {next_event, Type, Event}};

%% [Leader] Handle AppendEntries RPC (5.1, 5.2)
%% [AppendEntries] We are leader for the current term, so we should never see an
%%                 AppendEntries RPC from another node for this term
leader(_Type, ?APPEND_ENTRIES_RPC(CurrentTerm, SenderId, PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex),
       #raft_state{current_term = CurrentTerm, name = Name, commit_index = CommitIndex} = State) ->
    ?LOG_ERROR("Leader[~p, term ~p] got invalid heartbeat from conflicting leader ~p.",
        [Name, CurrentTerm, SenderId], #{domain => [whatsapp, wa_raft]}),
    send_rpc(SenderId, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, false, CommitIndex), State),
    keep_state_and_data;

%% [Leader] Handle AppendEntries RPC responses (5.2, 5.3, 7).
%% Handle normal-case successes
leader(cast, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, FollowerId, _PrevIndex, true, FollowerEndIndex),
    #raft_state{
        name = Name,
        current_term = CurrentTerm,
        commit_index = CommitIndex0,
        next_index = NextIndex0,
        match_index = MatchIndex0,
        heartbeat_response_ts = HeartbeatResponse0,
        first_current_term_log_index = TermStartIndex} = State0) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("Leader[~p, term ~p] append ok on ~p. Follower end index ~p. Leader commitIndex ~p",
        [Name, CurrentTerm, FollowerId, FollowerEndIndex, CommitIndex0], #{domain => [whatsapp, wa_raft]}),
    HeartbeatResponse1 = HeartbeatResponse0#{FollowerId => erlang:system_time(millisecond)},
    State1 = State0#raft_state{heartbeat_response_ts = HeartbeatResponse1},

    case select_follower_replication_mode(FollowerEndIndex, State1) of
        bulk_logs -> request_bulk_logs_for_follower(FollowerId, FollowerEndIndex, State1);
        _         -> cancel_bulk_logs_for_follower(FollowerId, State1)
    end,

    % Here, the RAFT protocol expects that the MatchIndex for the follower be set to
    % the log index of the last log entry replicated by the AppendEntries RPC that
    % triggered this AppendEntriesResponse RPC. However, we do not have enough state
    % here to figure out that log index so instead assume that the follower's log
    % matches completely after a successful AppendEntries RPC.
    %
    % This is perfectly valid during normal operations after the leadership has been
    % stable for a while since all replication at that point occurs at the end of
    % the log, and so FollowerEndIndex =:= PrevLogIndex + length(Entries). However,
    % this may not be true at the start of a term.
    %
    % In our implementation of the RAFT protocol, the leader of a new term always
    % appends a new log entry created by itself (with the new term) to the end of
    % the log before starting replication (hereinafter the "initial log entry").
    % We store the index of the initial log entry in FirstCurrentTermLogIndex.
    % For all followers, NextIndex is initialized to FirstCurrentTermLogIndex so
    % replication for the new term always starts from the initial log entry. In
    % addition, the leader will refuse to commit any log entries until it finds
    % a quorum that contains at least the initial log entry has been established.
    %
    % Note that since the initial log entry is created by the RAFT leader at the
    % start of a new term, it is impossible for followers with a log at least as
    % long as the leader's to match. After the first round of AppendEntries, all
    % followers will either match the leader or will have a log whose last log
    % index is lesser than FirstCurrentTermLogIndex.
    %
    %  * For any followers whose log matches, the condition is trivial.
    %  * For any followers whose log does not match and whose log ends with a log
    %    entry with an index lesser than (FirstCurrentTermLogIndex - 1), the first
    %    AppendEntries will fail due to the the previous log entry being missing.
    %  * For any followers whose log does not match and whose log ends with a log
    %    entry with an index at least (FirstCurrentTermLogIndex - 1), the first
    %    AppendEntries RPC will contain the initial log entry, which is guaranteed
    %    to not match, resulting in the log being truncated to end with the log
    %    entry at (FirstCurrentTermLogIndex - 1). Subsequent mismatches of this
    %    type will be detected by mismatching PrevLogIndex and PrevLogTerm.
    %
    % By the liveness of the RAFT protocol, since the AppendEntries step always
    % results in a FollowerEndIndex that is less than FirstCurrentTermLogIndex
    % if the follower's log does not match the leader's, we can conclude that
    % once FollowerEndIndex reaches FirstCurrentTermLogIndex, the follower must
    % have a matching log. Thus, once the quorum MatchIndex reaches a log index
    % at least FirstCurrentTermLogIndex (a.k.a. the initial log entry), we can
    % be sure that a quorum has been formed even when setting MatchIndex to
    % FollowerEndIndex for all AppendEntries.

    MatchIndex1 = maps:put(FollowerId, FollowerEndIndex, MatchIndex0),
    OldNextIndex = maps:get(FollowerId, NextIndex0, TermStartIndex),
    NextIndex1 = maps:put(FollowerId, erlang:max(OldNextIndex, FollowerEndIndex + 1), NextIndex0),

    State2 = State1#raft_state{match_index = MatchIndex1, next_index = NextIndex1},
    State3 = maybe_apply(FollowerEndIndex, State2),
    ?RAFT_GATHER('raft.leader.apply.func', timer:now_diff(os:timestamp(), StartT)),
    {keep_state, maybe_heartbeat(State3), ?HEARTBEAT_TIMEOUT};

%% and failures.
leader(cast, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, FollowerId, PrevLogIndex, false, FollowerEndIndex),
       #raft_state{name = Name, current_term = CurrentTerm, next_index = NextIndex0, match_index = MatchIndex0} = State0) ->
    ?RAFT_COUNT('raft.leader.append.failure'),
    ?LOG_WARNING( "Leader[~p, term ~p] append failure for follower ~p. Follower reports local log ends at ~0p.",
        [Name, CurrentTerm, FollowerId, FollowerEndIndex], #{domain => [whatsapp, wa_raft]}),

    select_follower_replication_mode(FollowerEndIndex, State0) =:= snapshot andalso
        request_snapshot_for_follower(FollowerId, State0),
    cancel_bulk_logs_for_follower(FollowerId, State0),

    % See comment in successful branch of AppendEntriesResponse RPC handling for
    % reasoning as to why it is safe to set MatchIndex to FollowerEndIndex for this
    % RAFT implementation.
    MatchIndex1 = maps:put(FollowerId, FollowerEndIndex, MatchIndex0),
    % We must trust the follower's last log index here because the follower may have
    % applied a snapshot since the last successful heartbeat. In such case, we need
    % to fast-forward the follower's next index so that we resume replication at the
    % point after the snapshot.
    NextIndex1 = maps:put(FollowerId, FollowerEndIndex + 1, NextIndex0),
    State1 = State0#raft_state{next_index = NextIndex1, match_index = MatchIndex1},
    State2 = maybe_apply(min(PrevLogIndex, FollowerEndIndex), State1),
    {keep_state, maybe_heartbeat(State2), ?HEARTBEAT_TIMEOUT};

%% [RequestVote RPC] We are already leader for the current term, so always decline votes (5.1, 5.2)
leader(_Type, ?REQUEST_VOTE_RPC(CurrentTerm, CandidateId, _ElectionType, _LastLogIndex, _LastLogTerm),
       #raft_state{current_term = CurrentTerm} = State) ->
    send_rpc(CandidateId, ?VOTE_RPC(CurrentTerm, node(), false), State),
    keep_state_and_data;

%% [Vote RPC] We are already leader, so we don't need to consider any more votes (5.1)
leader(_Type, ?VOTE_RPC(CurrentTerm, VoterId, Vote), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_NOTICE("Leader[~p, term ~p] receives vote ~p from ~p after being elected. Ignore it.",
        [Name, CurrentTerm, Vote, VoterId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Handover RPC] We are already leader so ignore any handover requests.
leader(_Type, ?HANDOVER_RPC(CurrentTerm, NodeId, Ref, _PrevLogIndex, _PrevLogTerm, _LogEntries),
       #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_WARNING("Leader[~p, term ~p] got orphan handover request from ~p while leader.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    send_rpc(NodeId, ?HANDOVER_FAILED_RPC(CurrentTerm, node(), Ref), State),
    keep_state_and_data;

%% [Handover Failed RPC] Our handover failed, so clear the handover status.
leader(_Type, ?HANDOVER_FAILED_RPC(CurrentTerm, NodeId, Ref),
       #raft_state{name = Name, current_term = CurrentTerm, handover = {NodeId, Ref, _Timeout}} = State) ->
    ?LOG_NOTICE("Leader[~p, term ~p] resuming normal operations after failed handover to ~p.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    {keep_state, State#raft_state{handover = undefined}};

%% [Handover Failed RPC] Got a handover failed with an unknown ID. Ignore.
leader(_Type, ?HANDOVER_FAILED_RPC(CurrentTerm, NodeId, _Ref),
       #raft_state{name = Name, current_term = CurrentTerm, handover = Handover}) ->
    ?LOG_NOTICE("Leader[~p, term ~p] got handover failed RPC from ~p that does not match current handover ~p.",
        [Name, CurrentTerm, NodeId, Handover], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [NotifyTerm] Drop NotifyTerm RPCs with matching term
leader(_Type, ?NOTIFY_TERM_RPC(CurrentTerm, _SenderName, _SenderNode), #raft_state{current_term = CurrentTerm}) ->
    keep_state_and_data;

%% [Fallback] Node receives unrecognized RPC
leader(_Type, ?RAFT_RPC(RPCType, CurrentTerm, SenderId, _Payload),
       #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Leader[~p, term ~p] receives unrecognized RPC ~p from ~p.",
        [Name, CurrentTerm, RPCType, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% Periodical Heartbeat to followers
leader(state_timeout = Type, Event, #raft_state{name = Name, current_term = CurrentTerm, handover = {Peer, _Ref, Timeout}} = State) ->
    NowMillis = erlang:system_time(millisecond),
    case NowMillis > Timeout of
        true ->
            ?LOG_NOTICE("Leader[~p, term ~p] handover to ~p times out",
                [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
            {keep_state, State#raft_state{handover = undefined}, {next_event, Type, Event}};
        false ->
            {keep_state_and_data, ?HEARTBEAT_TIMEOUT}
    end;
leader(state_timeout, _, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    State1 = append_entries_to_followers(State0),
    case ?RAFT_CONFIG(raft_election_weight, ?RAFT_ELECTION_DEFAULT_WEIGHT) of
        0 ->
            ?LOG_NOTICE("Leader[~p, term ~p] weight is zero and gives up leader role",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            {next_state, follower, State1};
        _ ->
            check_leader_lagging(State1),
            {keep_state, State1, ?HEARTBEAT_TIMEOUT}
    end;

%% [Commit] If a handover is in progress, then try to redirect to handover target
leader(cast, ?COMMIT_COMMAND({Ref, _Op}), #raft_state{storage = Storage, handover = {Peer, _Ref, _Timeout}} = State) ->
    ?RAFT_COUNT('raft.commit.handover'),
    wa_raft_storage:fulfill_op(Storage, Ref, {error, {notify_redirect, Peer}}), % Optimistically redirect to handover peer
    {keep_state, State};
%% [Commit] Otherwise, add a new commit to the RAFT log
leader(cast, ?COMMIT_COMMAND(Op), #raft_state{current_term = CurrentTerm, log_view = View0, next_index = NextIndex} = State0) ->
    ?RAFT_COUNT('raft.commit'),
    {ok, View1} = wa_raft_log:submit(View0, {CurrentTerm, Op}),
    ExpectedLastIndex = wa_raft_log:last_index(View1) + wa_raft_log:pending(View1),
    State1 = apply_single_node_cluster(State0#raft_state{log_view = View1}), % apply immediately for single node cluster

    MaxNexIndex = maps:fold(fun(_NodeId, V, Acc) -> erlang:max(Acc, V) end, 0, NextIndex),
    case ?RAFT_CONFIG(raft_commit_batch_interval_ms, 2) > 0 andalso ExpectedLastIndex - MaxNexIndex < ?RAFT_CONFIG(raft_commit_batch_max, 15) of
        true ->
            ?RAFT_COUNT('raft.commit.batch.delay'),
            {keep_state, State1, ?COMMIT_BATCH_TIMEOUT};
        false ->
            State2 = append_entries_to_followers(State1),
            {keep_state, State2, ?HEARTBEAT_TIMEOUT}
    end;

%% [Strong Read] Leader is eligible to serve strong reads.
leader(cast, ?READ_COMMAND({From, Command}),
       #raft_state{name = Name, table = Table, partition = Partition,log_view = View0, current_term = CurrentTerm,
                   commit_index = CommitIndex, first_current_term_log_index = FirstLogIndex} = State0) ->
    ?LOG_DEBUG("Leader[~p, term ~p] receives strong read request", [Name, CurrentTerm]),
    ReadIndex = max(CommitIndex, FirstLogIndex),
    Pending = wa_raft_log:pending(View0),
    LastLogIndex = wa_raft_log:last_index(View0),
    ok = wa_raft_queue:submit_read(Table, Partition, ReadIndex + 1, From, Command),
    {ok, View1} = case ReadIndex < LastLogIndex orelse Pending > 0 of
        true  -> {ok, View0};
        % TODO(hsun324): Try to reuse the commit code to deal with placeholder ops so we
        % handle batching and timeout properly instead of relying on a previously set timeout.
        false -> wa_raft_log:submit(View0, {CurrentTerm, {?READ_OP, noop}})
    end,
    {keep_state, State0#raft_state{log_view = View1}};

%% [Resign] Leader resigns by switching to follower state.
leader(Type, ?RESIGN_COMMAND, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    ?LOG_NOTICE("Leader[~p, term ~p] resigns.", [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{leader_id = undefined},
    notify_leader_change(State1),
    reply(Type, ok),
    {next_state, follower, State1};

%% [Adjust Membership] Leader attempts to commit a single-node membership change.
leader(Type, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer),
       #raft_state{name = Name, log_view = View0, storage = Storage,
                   current_term = CurrentTerm, last_applied = LastApplied} = State0) ->
    % Try to adjust the configuration according to the current request.
    Config = config(State0),
    % eqwalizer:fixme - adjust_membership_command() is defined imprecisely
    case adjust_config({Action, Peer}, Config, State0) of
        {ok, NewConfig} ->
            % Ensure that we have committed (applied entries are committed) at least one log entry
            % from our current term before admitting any membership change operations.
            case wa_raft_log:term(View0, LastApplied) of
                {ok, CurrentTerm} ->
                    % Ensure that this leader has no other pending config not yet applied.
                    StorageConfigIndex = case wa_raft_storage:read_metadata(Storage, config) of
                        {ok, #raft_log_pos{index = SI}, _} -> SI;
                        undefined                          -> 0
                    end,
                    LogConfigIndex = case wa_raft_log:config(View0) of
                        {ok, LI, _} -> LI;
                        not_found   -> 0
                    end,
                    case LogConfigIndex > StorageConfigIndex of
                        true ->
                            ?LOG_NOTICE("Leader[~p, term ~p] rejecting request to ~p peer ~p because it has a pending reconfiguration (storage: ~p, log: ~p).",
                                [Name, CurrentTerm, Action, Peer, StorageConfigIndex, LogConfigIndex], #{domain => [whatsapp, wa_raft]}),
                            reply(Type, {error, rejected}),
                            {keep_state, State0};
                        false ->
                            % Now that we have completed all the required checks, the leader is free to
                            % attempt to change the config. We heartbeat immediately to make the change as
                            % soon as possible.
                            Op = {make_ref(), {config, NewConfig}},
                            {ok, LogIndex, View1} = wa_raft_log:append(View0, [{CurrentTerm, Op}]),
                            ?LOG_NOTICE("Leader[~p, term ~p] appended configuration change from ~0p to ~0p at log index ~p.",
                                [Name, CurrentTerm, Config, NewConfig, LogIndex], #{domain => [whatsapp, wa_raft]}),
                            State1 = State0#raft_state{log_view = View1},
                            State2 = apply_single_node_cluster(State1),
                            State3 = append_entries_to_followers(State2),
                            reply(Type, {ok, #raft_log_pos{index = LogIndex, term = CurrentTerm}}),
                            {keep_state, State3, ?HEARTBEAT_TIMEOUT}
                    end;
                {ok, _OtherTerm} ->
                    ?LOG_NOTICE("Leader[~p, term ~p] rejecting request to ~p peer ~p because it has not established current term commit quorum.",
                        [Name, CurrentTerm, Action, Peer], #{domain => [whatsapp, wa_raft]}),
                    reply(Type, {error, rejected}),
                    {keep_state, State0}
            end;
        {error, Reason} ->
            ?LOG_NOTICE("Leader[~p, term ~p] refusing invalid membership adjustment ~0p on configuration ~0p due to ~0p.",
                [Name, CurrentTerm, {Action, Peer}, Config, Reason], #{domain => [whatsapp, wa_raft]}),
            reply(Type, {error, invalid}),
            {keep_state, State0}
    end;

%% [Snapshot Available] It does not make sense for a leader to install a new snapshot.
leader({call, From}, ?SNAPSHOT_AVAILABLE_COMMAND(_Root, _Position), _State) ->
    {keep_state_and_data, {reply, From, {error, rejected}}};

%% [Handover Candidates] Return list of handover candidates (peers that are not lagging too much)
leader({call, From}, ?HANDOVER_CANDIDATES_COMMAND, State) ->
    {keep_state_and_data, {reply, From, {ok, compute_handover_candidates(State)}}};

%% [Handover] With peer 'undefined' randomly select a valid candidate to handover to
leader(Type, ?HANDOVER_COMMAND(undefined), #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    case compute_handover_candidates(State) of
        [] ->
            ?LOG_NOTICE("Leader[~p, term ~p] has no valid peer to handover to",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            reply(Type, {error, no_valid_peer}),
            keep_state_and_data;
        Candidates ->
            Peer = lists:nth(rand:uniform(length(Candidates)), Candidates),
            leader(Type, ?HANDOVER_COMMAND(Peer), State)
    end;

%% [Handover] Do not allow handover to self
leader(Type, ?HANDOVER_COMMAND(Peer), #raft_state{name = Name, current_term = CurrentTerm} = State) when Peer =:= node() ->
    ?LOG_WARNING("Leader[~p, term ~p] dropping handover to self.",
        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, badarg}),
    {keep_state, State};

%% [Handover] Attempt to start a handover to the specified peer
leader(Type, ?HANDOVER_COMMAND(Peer),
       #raft_state{name = Name, log_view = View,
                   current_term = CurrentTerm, handover = undefined} = State0) ->
    % TODO(hsun324): For the time being, assume that all members of the cluster use the same server name.
    case member({Name, Peer}, config(State0)) of
        false ->
            ?LOG_WARNING("Leader[~p, term ~p] dropping handover to unknown peer ~p.",
                [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
            reply(Type, {error, badarg}),
            keep_state_and_data;
        true ->
            PeerMatchIndex = maps:get(Peer, State0#raft_state.match_index, 0),
            FirstIndex = wa_raft_log:first_index(View),
            PeerSendIndex = max(PeerMatchIndex + 1, FirstIndex + 1),
            LastIndex = wa_raft_log:last_index(View),
            MaxHandoverBatchSize = ?RAFT_MAX_HANDOVER_LOG_ENTRIES(),
            MaxHandoverBytes = ?RAFT_MAX_HANDOVER_LOG_SIZE(),

            case LastIndex - PeerSendIndex =< MaxHandoverBatchSize of
                true ->
                    ?RAFT_COUNT('raft.leader.handover'),
                    ?LOG_NOTICE("Leader[~p, term ~p] starting handover to ~p.",
                        [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
                    Ref = make_ref(),
                    Timeout = erlang:system_time(millisecond) + ?RAFT_HANDOVER_TIMEOUT_MS(),
                    State1 = State0#raft_state{handover = {Peer, Ref, Timeout}},

                    PrevLogIndex = PeerSendIndex - 1,
                    {ok, PrevLogTerm} = wa_raft_log:term(View, PrevLogIndex),
                    {ok, LogEntries} = wa_raft_log:get(View, PeerSendIndex, MaxHandoverBatchSize, MaxHandoverBytes),

                    % The request to load the log may result in not all required log entries being loaded
                    % if we hit the byte size limit. Ensure that we have loaded all required log entries
                    % before initiating a handover.
                    case PrevLogIndex + length(LogEntries) of
                        LastIndex ->
                            send_rpc(Peer, ?HANDOVER_RPC(CurrentTerm, node(), Ref, PrevLogIndex, PrevLogTerm, LogEntries), State1),
                            reply(Type, {ok, Peer}),
                            {keep_state, State1};
                        _ ->
                            ?RAFT_COUNT('raft.leader.handover.oversize'),
                            ?LOG_WARNING("Leader[~p, term ~p] handover to peer ~p would require an oversized RPC.",
                                [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
                            reply(Type, {error, oversize}),
                            keep_state_and_data
                    end;
                false ->
                    ?RAFT_COUNT('raft.leader.handover.peer_lagging'),
                    ?LOG_WARNING("Leader[~p, term ~p] determines that peer ~p is not eligible for handover because it is ~p entries behind.",
                        [Name, CurrentTerm, Peer, LastIndex - PeerSendIndex], #{domain => [whatsapp, wa_raft]}),
                    reply(Type, {error, peer_lagging}),
                    keep_state_and_data
            end
    end;

%% [Handover] Reject starting a handover when a handover is still in progress
leader({call, From}, ?HANDOVER_COMMAND(Peer), #raft_state{name = Name, current_term = CurrentTerm, handover = {Node, _Ref, _Timeout}}) ->
    ?LOG_WARNING("Leader[~p, term ~p] rejecting duplicate handover to ~p with running handover to ~p.",
        [Name, CurrentTerm, Peer, Node], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, duplicate}}};

leader(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% Leader receives unknown event
leader(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Leader[~p, term ~p] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, unsupported}),
    keep_state_and_data.

-spec follower(Type :: enter, PreviousState :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
              (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Follower] changes from leader
follower(enter, leader,
         #raft_state{name = Name, current_term = CurrentTerm, match_index = MatchIndex, next_index = NextIndex,
                     last_heartbeat_ts = LastHeartbeatTs, storage = Storage} = State) ->
    ?LOG_NOTICE("Follower[~p, term ~p] changes from leader. Match ~p. Next ~p. LastHeartbeat ~p.",
        [Name, CurrentTerm, MatchIndex, NextIndex, LastHeartbeatTs], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_state(State#raft_state.table, State#raft_state.partition, ?FUNCTION_NAME),
    check_follower_stale(follower, State),
    State1 = reset_state(State),
    State2 = State1#raft_state{voted_for = undefined, next_index = maps:new(), match_index = maps:new()},
    wa_raft_storage:cancel(Storage),
    {keep_state, State2, ?ELECTION_TIMEOUT};
%% [Follower] State entry setup
follower(enter, OldStateName,
         #raft_state{name = Name, current_term = CurrentTerm, match_index = MatchIndex, next_index = NextIndex} = State) ->
    ?LOG_NOTICE("Follower[~p, term ~p] starts with old state ~p. Match ~100p. Next ~100p",
        [Name, CurrentTerm, OldStateName, MatchIndex, NextIndex], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_state(State#raft_state.table, State#raft_state.partition, ?FUNCTION_NAME),
    check_follower_stale(follower, State),
    State1 = reset_state(State),
    State2 = State1#raft_state{voted_for = undefined, next_index = maps:new(), match_index = maps:new()}, %% always reset leader on state change
    {keep_state, State2, ?ELECTION_TIMEOUT};

%% [Protocol] Handle any RPCs
follower(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [RequestVote RPC] Discard normal RequestVote RPCs when we have an active leader.
follower(Type, ?REQUEST_VOTE_RPC(_Term, _SenderNode, normal, _LastLogIndex, _LastLogTerm) = Event, State) ->
    filter_vote_request(?FUNCTION_NAME, Type, Event, State);

%% [General Rules] Discard any incoming RPCs with a term older than the current term
follower(_Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload),
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term < CurrentTerm ->
    ?LOG_NOTICE("Follower[~p, term ~p] received stale ~p from ~p with old term ~p. Dropping.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?NOTIFY_TERM_RPC(CurrentTerm, Name, node()), State),
    {keep_state, State};
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
follower(Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload) = Event,
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term > CurrentTerm ->
    ?LOG_NOTICE("Follower[~p, term ~p] received ~p from ~p with new term ~p. Advancing.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, advance_term(Term, State), {next_event, Type, Event}};

%% [Follower] Handle AppendEntries RPC (5.2, 5.3)
%% Follower receives AppendEntries from leader
follower(Type, ?APPEND_ENTRIES_RPC(CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommitIndex, LeaderTrimIndex),
         #raft_state{current_term = CurrentTerm} = State0) ->
    case append_entries(CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, State0) of
        {{disable, Reason}, State1} ->
            State2 = State1#raft_state{disable_reason = Reason},
            {next_state, disabled, State2};
        {Result, #raft_state{log_view = View} = State1} ->
            State2 = State1#raft_state{leader_heartbeat_ts = erlang:system_time(millisecond)},
            TrimIndex = case ?RAFT_CONFIG(use_trim_index, false) of
                true  -> LeaderTrimIndex;
                false -> infinity
            end,
            LastIndex = wa_raft_log:last_index(View),
            NewCommitIndex = erlang:min(LeaderCommitIndex, LastIndex),
            send_rpc(LeaderId, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, Result =:= ok, LastIndex), State2),
            reply(Type, ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, Result =:= ok, LastIndex)),
            State3 = case Result of
                ok -> apply_log(State2, NewCommitIndex, TrimIndex);
                _  -> State2
            end,
            check_follower_lagging(LeaderCommitIndex, State3),
            {keep_state, State3, ?ELECTION_TIMEOUT}
    end;

%% [Follower] Handle AppendEntries RPC response (5.2)
%% [AppendEntriesResponse] Followers do not send AppendEntries and so should not get responses
follower(_Type, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, SenderId, _PrevLogIndex, _Success, _LastLogIndex),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Follower[~p, term ~p] got conflicting response from ~p.",
        [Name, CurrentTerm, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Follower] Handle RequestVote RPC (5.2)
%% [RequestVote] Follower receives a vote request from another node
follower(_Type, ?REQUEST_VOTE_RPC(CurrentTerm, CandidateId, _ElectionType, LastLogIndex, LastLogTerm),
         #raft_state{current_term = CurrentTerm} = State) ->
    State1 = vote(CandidateId, LastLogIndex, LastLogTerm, State),
    {keep_state, State1};

%% [Follower] Handle RequestVote RPC response (5.2)
%% [Vote] A follower has already lost the election for a term, so drop any vote responses
follower(_Type, ?VOTE_RPC(CurrentTerm, VoterId, Voted),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Follower[~p, term ~p] got extra vote ~p from ~p.",
        [Name, CurrentTerm, Voted, VoterId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Handover Extension] Another leader is asking for this follower to take over
follower(_Type, ?HANDOVER_RPC(CurrentTerm, NodeId, Ref, PrevLogIndex, PrevLogTerm, LogEntries),
         #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    ?RAFT_COUNT('wa_raft.follower.handover'),
    ?LOG_NOTICE("Follower[~p, term ~p] evaluating handover RPC from ~p.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    case ?RAFT_CONFIG(raft_election_weight, ?RAFT_ELECTION_DEFAULT_WEIGHT) of
        0 ->
            ?LOG_NOTICE("Follower[~p, term ~p] not considering handover RPC due to election weight 0.",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            send_rpc(NodeId, ?HANDOVER_FAILED_RPC(CurrentTerm, node(), Ref), State0),
            {keep_state, State0};
        _ ->
            case append_entries(CurrentTerm, NodeId, PrevLogIndex, PrevLogTerm, LogEntries, State0) of
                {ok, State1} ->
                    ?LOG_NOTICE("Follower[~p, term ~p] immediately starting new election due to append success during handover RPC.",
                        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
                    {next_state, candidate, State1#raft_state{next_election_type = force}};
                {Error, State1} ->
                    ?RAFT_COUNT('wa.raft.follower.handover.failed'),
                    ?LOG_WARNING("Follower[~p, term ~p] failing handover request due to non-ok append result of ~p",
                        [Name, CurrentTerm, Error], #{domain => [whatsapp, wa_raft]}),
                    send_rpc(NodeId, ?HANDOVER_FAILED_RPC(CurrentTerm, node(), Ref), State1),
                    {keep_state, State1}
            end
    end;

%% [HandoverFailed Extension] Followers cannot initiate handovers
follower(_Type, ?HANDOVER_FAILED_RPC(CurrentTerm, NodeId, _Ref),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Follower[~p, term ~p] got conflicting handover failed from ~p.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [NotifyTerm] Drop NotifyTerm RPCs with matching term
follower(_Type, ?NOTIFY_TERM_RPC(CurrentTerm, _SenderName, _SenderNode), #raft_state{current_term = CurrentTerm}) ->
    keep_state_and_data;

%% [Fallback] Node receives unrecognized RPC
follower(_Type, ?RAFT_RPC(RPCType, CurrentTerm, SenderId, _Payload),
       #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Follower[~p, term ~p] receives unrecognized RPC ~p from ~p.",
        [Name, CurrentTerm, RPCType, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Follower] handle timeout
%% follower doesn't receive any heartbeat. starting a new election
follower(state_timeout, _,
         #raft_state{name = Name, current_term = CurrentTerm, leader_id = LeaderId,
                     log_view = View, leader_heartbeat_ts = HeartbeatTs} = State) ->
    WaitingMs = case HeartbeatTs of
        undefined -> undefined;
        _         -> erlang:system_time(millisecond) - HeartbeatTs
    end,
    ?LOG_NOTICE("Follower[~p, term ~p] times out after ~p ms. Last leader ~p. Max log index ~p. Promote to candidate and restart election.",
        [Name, CurrentTerm, WaitingMs, LeaderId, wa_raft_log:last_index(View)], #{domain => [whatsapp, wa_raft]}),
    ?RAFT_COUNT('raft.follower.timeout'),
    case ?RAFT_CONFIG(raft_election_weight, ?RAFT_ELECTION_DEFAULT_WEIGHT) of
        0 ->
            ?LOG_NOTICE("Follower[~p, term ~p] election weight is zero so not advancing.",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            {keep_state, State, ?ELECTION_TIMEOUT};
        _ ->
            {next_state, candidate, State}
    end;

follower(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

follower(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Follower[~p, term ~p] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, unsupported}),
    keep_state_and_data.

-spec candidate(Type :: enter, PreviousState :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
               (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Enter] Node starts a new election upon entering the candidate state.
candidate(enter, _PreviousState,
          #raft_state{name = Name, current_term = CurrentTerm,
                      log_view = View, next_election_type = ElectionType} = State0) ->
    ?RAFT_COUNT('raft.leader.election_started'),
    wa_raft_info:set_state(State0#raft_state.table, State0#raft_state.partition, ?FUNCTION_NAME),

    % Entering the candidate state means that we are starting a new election, thus
    % advance the term and set VotedFor to the current node. (Candidates always
    % implicitly vote for themselves.)
    State1 = advance_term(CurrentTerm + 1, node(), State0),

    LastLogIndex = wa_raft_log:last_index(View),
    {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),

    ?LOG_NOTICE("Candidate[~p, term ~p] moves to new term ~p and starts election with last log position ~p:~p.",
        [Name, CurrentTerm, CurrentTerm + 1, LastLogIndex, LastLogTerm], #{domain => [whatsapp, wa_raft]}),

    check_follower_stale(candidate, State1),
    State2 = reset_state(State1),
    State3 = State2#raft_state{election_start_ts = os:timestamp(), next_election_type = normal, votes = #{}},

    % Broadcast vote requests and also send a vote-for-self.
    % (Candidates always implicitly vote for themselves.)
    broadcast(?REQUEST_VOTE_RPC(CurrentTerm + 1, node(), ElectionType, LastLogIndex, LastLogTerm), State3),
    send_rpc(node(), ?VOTE_RPC(CurrentTerm + 1, node(), true), State3),

    {keep_state, State3, ?ELECTION_TIMEOUT};

%% [Protocol] Handle any RPCs
candidate(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [RequestVote RPC] Discard normal RequestVote RPCs when we have an active leader.
candidate(Type, ?REQUEST_VOTE_RPC(_Term, _SenderNode, normal, _LastLogIndex, _LastLogTerm) = Event, State) ->
    filter_vote_request(?FUNCTION_NAME, Type, Event, State);

%% [General Rules] Discard any incoming RPCs with a term older than the current term
candidate(_Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload),
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term < CurrentTerm ->
    ?LOG_NOTICE("Candidate[~p, term ~p] received stale ~p from ~p with old term ~p. Dropping.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?NOTIFY_TERM_RPC(CurrentTerm, Name, node()), State),
    {keep_state, State};
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
candidate(Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload) = Event,
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term > CurrentTerm ->
    ?LOG_NOTICE("Candidate[~p, term ~p] received ~p from ~p with new term ~p. Advancing and switching to follower.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, advance_term(Term, State), {next_event, Type, Event}};

%% [AppendEntries RPC] Switch to follower because current term now has a leader (5.2, 5.3)
candidate(Type, ?APPEND_ENTRIES_RPC(CurrentTerm, LeaderId, _PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex) = Event,
          #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Candidate[~p, term ~p] gets first heartbeat of the term from ~p. Switch to follower.",
        [Name, CurrentTerm, LeaderId], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, State, {next_event, Type, Event}};

%% [RequestVote RPC] Candidate has always voted for itself, so vote false on anyone else (5.2)
candidate(_Type, ?REQUEST_VOTE_RPC(CurrentTerm, CandidateId, _ElectionType, _LastLogIndex, _LastLogTerm),
          #raft_state{current_term = CurrentTerm} = State) ->
    send_rpc(CandidateId, ?VOTE_RPC(CurrentTerm, node(), false), State),
    keep_state_and_data;

%% [Vote RPC] Candidate receives an affirmative vote (5.2)
candidate(cast, ?VOTE_RPC(CurrentTerm, NodeId, true),
          #raft_state{
              name = Name,
              log_view = View,
              current_term = CurrentTerm,
              heartbeat_response_ts = HeartbeatResponse0,
              election_start_ts = StartT,
              votes = Votes0} = State0) ->
    HeartbeatResponse1 = HeartbeatResponse0#{NodeId => erlang:system_time(millisecond)},
    Votes1 = Votes0#{NodeId => true},
    State1 = State0#raft_state{heartbeat_response_ts = HeartbeatResponse1, votes = Votes1},
    case compute_quorum(Votes1, false, config(State1)) of
        true ->
            StartT =/= undefined orelse error(bad_state),
            Duration = timer:now_diff(os:timestamp(), StartT),
            LastIndex = wa_raft_log:last_index(View),
            {ok, LastTerm} = wa_raft_log:term(View, LastIndex),
            ?LOG_NOTICE("Candidate[~p, term ~p] is becoming leader with last log ~p:~p and votes ~w. Election takes ~p us",
                [Name, CurrentTerm, LastIndex, LastTerm, Votes1, Duration], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_GATHER('raft.candidate.election.duration', Duration),
            {next_state, leader, State1};
        false ->
            {keep_state, State1}
    end;

%% [Vote RPC] Candidate receives a negative vote (Candidate cannot become leader here. Losing
%%            an election does not need to convert candidate to follower.) (5.2)
candidate(cast, ?VOTE_RPC(CurrentTerm, NodeId, false),
          #raft_state{current_term = CurrentTerm, votes = Votes} = State) ->
    {keep_state, State#raft_state{votes = maps:put(NodeId, false, Votes)}};

%% [Handover RPC] Got a message from the leader of this term. Switch to follower
candidate(Type, ?HANDOVER_RPC(CurrentTerm, _NodeId, _Ref, _PrevLogIndex, _PrevLogTerm, _LogEntries) = Event,
          #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Candidate[~p, term ~p] switching to follower to handle handover.",
        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, State, {next_event, Type, Event}};

%% [HandoverFailed RPC] Candidates cannot initiate handovers
candidate(_Type, ?HANDOVER_FAILED_RPC(CurrentTerm, NodeId, _Ref),
          #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Candidate[~p, term ~p] got conflicting handover failed from ~p.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [NotifyTerm] Drop NotifyTerm RPCs with matching term
candidate(_Type, ?NOTIFY_TERM_RPC(CurrentTerm, _SenderName, _SenderNode), #raft_state{current_term = CurrentTerm}) ->
    keep_state_and_data;

%% [Fallback] Node receives unrecognized RPC
candidate(_Type, ?RAFT_RPC(RPCType, CurrentTerm, SenderId, _Payload),
       #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Candidate[~p, term ~p] receives unrecognized RPC ~p from ~p.",
        [Name, CurrentTerm, RPCType, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Candidate] Handle Election Timeout (5.2)
%% Candidate doesn't get enough votes after a period of time, restart election.
candidate(state_timeout, _,
          #raft_state{name = Name, current_term = CurrentTerm, votes = Votes} = State) ->
    ?LOG_NOTICE("Candidate[~p, term ~p] election timed out. Starting new election. Votes: ~w", [Name, CurrentTerm, Votes], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, State};

candidate(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

candidate(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Candidate[~p, term ~p] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, unsupported}),
    keep_state_and_data.

-spec disabled(Type :: enter, PreviousState :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
              (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
disabled(enter, FromState, #raft_state{disable_reason = undefined} = State) ->
    disabled(enter, FromState, State#raft_state{disable_reason = "No reason specified."});

disabled(enter, leader, #raft_state{name = Name, current_term = CurrentTerm, storage = Storage} = State0) ->
    ?LOG_NOTICE("Leader[~p, term ~p] is now disabled.", [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_state(State0#raft_state.table, State0#raft_state.partition, ?FUNCTION_NAME),
    wa_raft_info:set_stale(true, State0),
    State1 = reset_state(State0),
    State2 = State1#raft_state{leader_id = undefined, voted_for = undefined, next_index = #{}, match_index = #{}},
    notify_leader_change(State2),
    wa_raft_storage:cancel(Storage),
    wa_raft_durable_state:store(State2),
    {keep_state, State2};

disabled(enter, FromState, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    FromState =/= disabled andalso
        ?LOG_NOTICE("~p[~p, term ~p] is now disabled.", [FromState, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_state(State0#raft_state.table, State0#raft_state.partition, ?FUNCTION_NAME),
    wa_raft_info:set_stale(true, State0),
    State1 = reset_state(State0),
    State2 = State1#raft_state{voted_for = undefined, next_index = #{}, match_index = #{}},
    wa_raft_durable_state:store(State2),
    {keep_state, State2};

%% [Protocol] Handle any RPCs
disabled(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [RequestVote RPC] Disabled nodes drop all RequestVote RPCs unconditionally
disabled(_Type, ?REQUEST_VOTE_RPC(_Term, _SenderNode, normal, _LastLogIndex, _LastLogTerm), State) ->
    ?RAFT_COUNT('raft.server.request_vote.disabled'),
    {keep_state, State};

%% [General Rules] Discard any incoming RPCs with a term older than the current term
disabled(_Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload),
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term < CurrentTerm ->
    ?LOG_NOTICE("Disabled[~p, term ~p] received stale ~p from ~p with old term ~p. Dropping.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    % Do not notify term here because a disabled node should be invisible to the cluster.
    {keep_state, State};
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
disabled(Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload) = Event,
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term > CurrentTerm ->
    ?LOG_NOTICE("Disabled[~p, term ~p] received ~p from ~p with new term ~p. Advancing.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, advance_term(Term, State), {next_event, Type, Event}};

disabled(_Type, ?APPEND_ENTRIES_RPC(CurrentTerm, SenderId, _PrevLogIndex, _PrevLogTerm, _Entries, _CommitIndex, _TrimIndex),
         #raft_state{current_term = CurrentTerm, leader_id = undefined} = State0) ->
    %% When we detect the leader for the current term, then set cache so that we can handle redirects.
    State1 = State0#raft_state{leader_id = SenderId},
    notify_leader_change(State1),
    {keep_state, State1};

disabled(_Type, ?APPEND_ENTRIES_RPC(CurrentTerm, _SenderId, _PrevLogIndex, _PrevLogTerm, _Entries, _CommitIndex, _TrimIndex),
         #raft_state{current_term = CurrentTerm}) ->
    %% Ignore any other AppendEntries RPC calls because a disabled node should be invisible to the cluster.
    keep_state_and_data;

disabled(_Type, ?REQUEST_VOTE_RPC(CurrentTerm, _SenderId, _ElectionType, _LastLogIndex, _LastLogTerm),
         #raft_state{current_term = CurrentTerm}) ->
    %% Ignore any RequestVote RPC calls because a disabled node should be invisible to the cluster.
    keep_state_and_data;

%% [NotifyTerm] Drop NotifyTerm RPCs with matching term
disabled(_Type, ?NOTIFY_TERM_RPC(CurrentTerm, _SenderName, _SenderNode), #raft_state{current_term = CurrentTerm}) ->
    keep_state_and_data;

%% [Fallback] Node receives unrecognized RPC
disabled(_Type, ?RAFT_RPC(RPCType, CurrentTerm, SenderId, _Payload),
       #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Disabled[~p, term ~p] receives unrecognized RPC ~p from ~p.",
        [Name, CurrentTerm, RPCType, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

disabled({call, From}, ?PROMOTE_COMMAND(_Term, _Force, _Config), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Disabled[~p, term ~p] cannot be promoted.", [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, disabled}}};

disabled({call, From}, ?ENABLE_COMMAND, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    ?LOG_NOTICE("Disabled[~p, term ~p] re-enabling by request from ~p by moving to stalled state.",
        [Name, CurrentTerm, From], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{disable_reason = undefined},
    wa_raft_durable_state:store(State1),
    {next_state, stalled, State1, {reply, From, ok}};

disabled(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

disabled(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Disabled[~p, term ~p] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, unsupported}),
    keep_state_and_data.

%% [Witness] State functions
-spec witness(Type :: enter, PreviousState :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
             (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
witness(enter, FromState, #raft_state{witness = false} = State) ->
    witness(enter, FromState, State#raft_state{witness = true});

% [Witness] Enters witness state after being in any other state
witness(enter, FromState, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    FromState =/= witness andalso
        ?LOG_NOTICE("~p[~p, term ~p] is now witness.", [FromState, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_state(State0#raft_state.table, State0#raft_state.partition, ?FUNCTION_NAME),
    wa_raft_info:set_stale(true, State0),
    State1 = reset_state(State0),
    State2 = State1#raft_state{voted_for = undefined, next_index = #{}, match_index = #{}},
    wa_raft_durable_state:store(State2),
    {keep_state, State2, ?WITNESS_TIMEOUT};

%% [Protocol] Handle any RPCs
witness(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [Witness] handle timeout
%% Witness doesn't receive any heartbeat. Update staleness state.
witness(state_timeout, _,
         #raft_state{name = Name, current_term = CurrentTerm, leader_id = LeaderId,
                     log_view = View, leader_heartbeat_ts = HeartbeatTs} = State) ->
    WaitingMs = case HeartbeatTs of
        undefined -> undefined;
        _         -> erlang:system_time(millisecond) - HeartbeatTs
    end,
    ?LOG_NOTICE("Witness[~p, term ~p] times out after ~p ms. Last leader ~p. Max log index ~p.",
        [Name, CurrentTerm, WaitingMs, LeaderId, wa_raft_log:last_index(View)], #{domain => [whatsapp, wa_raft]}),
    ?RAFT_COUNT('raft.witness.timeout'),
    wa_raft_info:set_stale(true, State),
    {keep_state, State, ?WITNESS_TIMEOUT};

%% [RequestVote RPC] Discard normal RequestVote RPCs when we have an active leader.
witness(Type, ?REQUEST_VOTE_RPC(_Term, _SenderNode, normal, _LastLogIndex, _LastLogTerm) = Event, State) ->
    filter_vote_request(?FUNCTION_NAME, Type, Event, State);

witness(_Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload),
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term < CurrentTerm ->
    ?LOG_NOTICE("Witness[~p, term ~p] received stale ~p from ~p with old term ~p. Dropping.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?NOTIFY_TERM_RPC(CurrentTerm, Name, node()), State),
    {keep_state, State};

witness(Type, ?PROCEDURE_CALL(RPCType, Term, Sender, _Payload) = Event,
        #raft_state{name = Name, current_term = CurrentTerm} = State) when Term > CurrentTerm ->
    ?LOG_NOTICE("Witness[~p, term ~p] received ~p from ~p with new term ~p. Advancing.",
        [Name, CurrentTerm, RPCType, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, advance_term(Term, State), {next_event, Type, Event}};

%% [Witness] Handle AppendEntries RPC (5.2, 5.3)
%% Witness receives AppendEntries from leader
witness(Type, ?APPEND_ENTRIES_RPC(CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommitIndex, LeaderTrimIndex),
        #raft_state{current_term = CurrentTerm} = State0) ->
    case append_entries(CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, State0) of
        {{disable, Reason}, State1} ->
            State2 = State1#raft_state{disable_reason = Reason},
            {next_state, disabled, State2};
        {Result, #raft_state{log_view = View} = State1} ->
            State2 = State1#raft_state{leader_heartbeat_ts = erlang:system_time(millisecond)},
            TrimIndex = case ?RAFT_CONFIG(use_trim_index, false) of
                true  -> LeaderTrimIndex;
                false -> infinity
            end,
            LastIndex = wa_raft_log:last_index(View),
            NewCommitIndex = erlang:min(LeaderCommitIndex, LastIndex),
            send_rpc(LeaderId, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, Result =:= ok, LastIndex), State2),
            reply(Type, ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, Result =:= ok, LastIndex)),
            State3 = case Result of
                ok -> apply_log(State2, NewCommitIndex, TrimIndex);
                _  -> State2
            end,
            check_follower_lagging(LeaderCommitIndex, State3),
            {keep_state, State3, ?WITNESS_TIMEOUT}
    end;

%% [Witness] Handle AppendEntries RPC response (5.2)
%% [AppendEntriesResponse] Witnesses do not send AppendEntries and so should not get responses
witness(_Type, ?APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, SenderId, _PrevLogIndex, _Success, _LastLogIndex),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Follower[~p, term ~p] got conflicting response from ~p.",
        [Name, CurrentTerm, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

% [Witness] Witnesses ignore any handover requests.
witness(_Type, ?HANDOVER_RPC(CurrentTerm, NodeId, Ref, _PrevLogIndex, _PrevLogTerm, _LogEntries),
       #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_WARNING("Witness[~p, term ~p] got orphan handover request from ~p while witness.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    send_rpc(NodeId, ?HANDOVER_FAILED_RPC(CurrentTerm, node(), Ref), State),
    keep_state_and_data;

witness(_Type, ?NOTIFY_TERM_RPC(CurrentTerm, _SenderName, _SenderNode), #raft_state{current_term = CurrentTerm}) ->
    keep_state_and_data;

witness(_Type, ?HANDOVER_FAILED_RPC(CurrentTerm, NodeId, _Ref),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Witness[~p, term ~p] got conflicting handover failed from ~p.",
        [Name, CurrentTerm, NodeId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Fallback] Node receives unrecognized RPC
witness(_Type, ?RAFT_RPC(RPCType, CurrentTerm, SenderId, _Payload),
       #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Witness[~p, term ~p] receives unrecognized RPC ~p from ~p.",
        [Name, CurrentTerm, RPCType, SenderId], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

witness({call, From}, ?SNAPSHOT_AVAILABLE_COMMAND(_, #raft_log_pos{index = SnapshotIndex, term = SnapshotTerm} = SnapshotPos),
        #raft_state{log_view = View0, name = Name, current_term = CurrentTerm, last_applied = LastApplied} = State0) ->
    case SnapshotIndex > LastApplied orelse LastApplied =:= 0 of
        true ->
            ?LOG_NOTICE("Witness[~p, term ~p] accepting snapshot ~p:~p but not loading",
                [Name, CurrentTerm, SnapshotIndex, SnapshotTerm], #{domain => [whatsapp, wa_raft]}),
            {ok, View1} = wa_raft_log:reset(View0, SnapshotPos),
            State1 = State0#raft_state{log_view = View1, last_applied = SnapshotIndex, commit_index = SnapshotIndex},
            State2 = load_config(State1),
            NewTerm = max(CurrentTerm, SnapshotTerm),
            {next_state, witness, advance_term(NewTerm, State2), {reply, From, ok}};
        false ->
            ?LOG_NOTICE("Witness[~p, term ~p] ignoring available snapshot ~p:~p with index not past ours (~p)",
                [Name, CurrentTerm, SnapshotIndex, SnapshotTerm, LastApplied], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;

%% [Witness] Witness receives RAFT command
witness(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Witness] Witness receives unknown event
witness(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Witness[~p, term ~p] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, unsupported}),
    keep_state_and_data.


-spec terminate(Reason :: term(), StateName :: state(), State :: #raft_state{}) -> ok.
terminate(Reason, StateName, #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("~p[~p, term ~p] terminating due to ~p.",
        [StateName, Name, CurrentTerm, Reason], #{domain => [whatsapp, wa_raft]}),
    wa_raft_durable_state:sync(State),
    wa_raft_info:delete_state(State#raft_state.table, State#raft_state.partition),
    wa_raft_info:set_stale(true, State),
    ok.

-spec code_change(_OldVsn :: term(), StateName :: state(), State :: #raft_state{}, Extra :: term())
        -> {ok, StateName :: state(), State :: #raft_state{}}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Fallbacks for command calls to the RAFT server for when there is no state-specific
%% handler for a particular command defined for a particular RAFT server FSM state.
-spec command(state(), gen_statem:event_type(), command(), #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Commit] Non-leader nodes should fail commits with {error, not_leader}.
command(StateName, Type, ?COMMIT_COMMAND({Ref, _}),
        #raft_state{name = Name, current_term = CurrentTerm, storage = Storage, leader_id = LeaderId}) when StateName =/= leader ->
    ?LOG_WARNING("~p[~p, term ~p] commit ~p fails. Leader is ~p.",
        [StateName, Name, CurrentTerm, Ref, LeaderId], #{domain => [whatsapp, wa_raft]}),
    wa_raft_storage:fulfill_op(Storage, Ref, {error, not_leader}),
    reply(Type, {error, not_leader}),
    keep_state_and_data;
%% [Strong Read] Non-leader nodes are not eligible for strong reads.
command(StateName, cast, ?READ_COMMAND({From, _}),
        #raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm, leader_id = LeaderId}) when StateName =/= leader ->
    ?LOG_WARNING("~p[~p, term ~p] strong read fails. Leader is ~p.",
        [StateName, Name, CurrentTerm, LeaderId], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_read_early(Table, Partition, From, {error, not_leader}),
    keep_state_and_data;
%% [Status] Get status of node.
command(StateName, {call, From}, ?STATUS_COMMAND, State) ->
    Status = [
        {state, StateName},
        {id, node()},
        {table, State#raft_state.table},
        {partition, State#raft_state.partition},
        {data_dir, State#raft_state.data_dir},
        {current_term, State#raft_state.current_term},
        {voted_for, State#raft_state.voted_for},
        {commit_index, State#raft_state.commit_index},
        {last_applied, State#raft_state.last_applied},
        {leader_id, State#raft_state.leader_id},
        {next_index, State#raft_state.next_index},
        {match_index, State#raft_state.match_index},
        {log_module, wa_raft_log:provider(State#raft_state.log_view)},
        {log_first, wa_raft_log:first_index(State#raft_state.log_view)},
        {log_last, wa_raft_log:last_index(State#raft_state.log_view)},
        {votes, State#raft_state.votes},
        {inflight_applies, wa_raft_queue:apply_queue_size(State#raft_state.table, State#raft_state.partition)},
        {disable_reason, State#raft_state.disable_reason},
        {config, config(State)},
        {config_index, config_index(State)},
        {witness, State#raft_state.witness}
    ],
    {keep_state_and_data, {reply, From, Status}};

%% [Promote] Non-disabled nodes check if eligible to promote and then promote to leader.
command(StateName, {call, From}, ?PROMOTE_COMMAND(Term, Force, Config),
        #raft_state{name = Name, log_view = View0, current_term = CurrentTerm, leader_heartbeat_ts = HeartbeatTs} = State0) when StateName =/= disabled ->
    ElectionWeight = ?RAFT_CONFIG(raft_election_weight, ?RAFT_ELECTION_DEFAULT_WEIGHT),
    SavedConfig = config(State0),
    Allowed = if
        Term =< CurrentTerm ->
            ?LOG_ERROR("~p[~p, term ~p] cannot attempt promotion to invalid term ~p.",
                [StateName, Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
            false;
        ElectionWeight =:= 0 ->
            ?LOG_ERROR("~p[~p, term ~p] node election weight is zero and cannot be promoted as leader.",
                [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            false;
        % Prevent promotions to any operational state when there is no cluster membership configuration.
        (not is_map_key(membership, SavedConfig)) andalso (Config =:= undefined orelse not is_map_key(membership, Config)) ->
            ?LOG_ERROR("~p[~p, term ~p] cannot promote with neither existing nor forced config having configured membership.",
                [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            false;
        StateName =:= witness ->
            ?LOG_ERROR("~p[~p, term ~p] cannot promote a witness node.",
                [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            false;
        Force ->
            true;
        Config =/= undefined ->
            ?LOG_ERROR("~p[~p, term ~p] cannot forcibly apply a configuration when doing a non-forced promotion.",
                [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            false;
        StateName =:= stalled ->
            ?LOG_ERROR("~p[~p, term ~p] cannot promote a stalled node.",
                [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            false;
        HeartbeatTs =:= undefined ->
            true;
        true ->
            erlang:system_time(millisecond) - HeartbeatTs >= ?RAFT_CONFIG(raft_promotion_grace_period_secs, 60) * 1000
    end,
    case Allowed of
        true ->
            NewStateName = case Force of
                true  -> leader;
                false -> candidate
            end,
            ?LOG_NOTICE("~p[~p, term ~p] promoted to ~p of term ~p.",
                [StateName, Name, CurrentTerm, NewStateName, Term], #{domain => [whatsapp, wa_raft]}),

            % Advance to the term requested for promotion to
            State1 = advance_term(Term, State0),
            State2 = case Config of
                undefined ->
                    State1;
                _ ->
                    % If this promotion is part of the bootstrapping operation, then we must append
                    % the new configuration to the log before we can transition to leader; otherwise,
                    % the server will not know who to replicate to as leader.
                    Op = {make_ref(), {config, Config}},
                    {ok, _, View1} = wa_raft_log:append(View0, [{State1#raft_state.current_term, Op}]),
                    State1#raft_state{log_view = View1}
            end,
            case StateName =:= NewStateName of
                true  -> {repeat_state, State2, {reply, From, ok}};
                false -> {next_state, NewStateName, State2, {reply, From, ok}}
            end;
        false ->
            ?LOG_NOTICE("~p[~p, term ~p] rejected leader promotion of term ~p.",
                [StateName, Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;
%% [Resign] Non-leader nodes cannot resign.
command(StateName, {call, From}, ?RESIGN_COMMAND, #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= leader ->
    ?LOG_NOTICE("~p[~p, term ~p] not resigning because we are not leader.",
        [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, not_leader}}};
%% [AdjustMembership] Non-leader nodes cannot adjust their config.
command(StateName, Type, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer), #raft_state{name = Name, current_term = CurrentTerm} = State) when StateName =/= leader ->
    ?LOG_NOTICE("~p[~p, term ~p] cannot ~p peer ~p because we are not leader.",
        [StateName, Name, CurrentTerm, Action, Peer], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, not_leader}),
    {keep_state, State};
%% [Snapshot Available] Follower and candidate nodes might switch to stalled to install snapshot.
command(StateName, {call, From} = Type, ?SNAPSHOT_AVAILABLE_COMMAND(_Root, #raft_log_pos{index = SnapshotIndex}) = Event,
        #raft_state{name = Name, current_term = CurrentTerm, last_applied = LastAppliedIndex} = State)
            when StateName =:= follower orelse StateName =:= candidate ->
    case SnapshotIndex > LastAppliedIndex of
        true ->
            ?LOG_NOTICE("~p[~p, term ~p] got snapshot with newer index ~p compared to currently applied index ~p",
                [StateName, Name, CurrentTerm, SnapshotIndex, LastAppliedIndex], #{domain => [whatsapp, wa_raft]}),
            {next_state, stalled, State, {next_event, Type, Event}};
        false ->
            ?LOG_NOTICE("~p[~p, term ~p] ignoring snapshot with index ~p compared to currently applied index ~p",
                [StateName, Name, CurrentTerm, SnapshotIndex, LastAppliedIndex], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;
%% [Handover Candidates] Non-leader nodes cannot serve handovers.
command(StateName, {call, From}, ?HANDOVER_CANDIDATES_COMMAND,
        #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= leader ->
    ?LOG_NOTICE("~p[~p, term ~p] dropping handover candidates request due to not being leader.",
        [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, not_leader}}};
%% [Handover] Non-leader nodes cannot serve handovers.
command(StateName, Type, ?HANDOVER_COMMAND(_Peer),
        #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= leader ->
    ?LOG_NOTICE("~p[~p, term ~p] rejecting handover request because we are not leader.",
        [StateName, Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, not_leader}),
    keep_state_and_data;
%% [Enable] Non-disabled nodes are already enabled.
command(StateName, {call, From}, ?ENABLE_COMMAND, #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= disabled ->
    ?LOG_NOTICE("~p[~p, term ~p] got enable request from ~p while enabled.",
        [StateName, Name, CurrentTerm, From], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, already_enabled}}};
%% [Disable] All nodes should disable by setting RAFT state disable_reason.
command(StateName, cast, ?DISABLE_COMMAND(Reason), #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    ?LOG_NOTICE("~p[~p, term ~p] disabling due to reason ~p.",
        [StateName, Name, CurrentTerm, Reason], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{disable_reason = Reason},
    wa_raft_durable_state:store(State1),
    {next_state, disabled, State1};
%% [Fallback] Drop unknown command calls.
command(StateName, Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_NOTICE("~p[~p, term ~p] dropping unhandled command ~p event ~p",
        [StateName, Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

%% ==================================================
%%  RAFT Configuration Change Helpers
%% ==================================================

%% Determine if the specified peer is a member of the current cluster configuration,
%% returning false if the specified peer is not a member or there is no current cluster
%% configuration.
-spec member(Peer :: peer(), Config :: config()) -> boolean().
member(Peer, Config) ->
    member(Peer, Config, false).

-spec member(Peer :: peer(), Config :: config(), Default :: boolean()) -> boolean().
member(Peer, #{membership := Membership}, _Default) ->
    lists:member(Peer, Membership);
member(_Peer, _Config, Default) ->
    Default.

%% Helper function to get the membership list from a config with unknown version.
-spec config_membership(Config :: config()) -> Membership :: [peer()].
config_membership(#{membership := Membership}) ->
    Membership;
config_membership(_Config) ->
    error(membership_not_set).

-spec config_witnesses(Config :: config()) -> Witnesses :: [peer()].
config_witnesses(#{witness := Witnesses}) ->
    Witnesses;
config_witnesses(_Config) ->
    [].

%% Migration helper function for bridging RAFT state peer-based cluster membership
%% and RAFT dynamic cluster membership.
%% TODO(hsun324): Begin migrating uses of this function to use config directly as applicable.
-spec config_peers(Config :: config(), State :: #raft_state{}) -> Peers :: [{atom(), {atom(), node()}}].
config_peers(#{membership := Members}, _State) ->
    % Currently, we use the node name as the RAFT id.
    [{Node, {Name, Node}} || {Name, Node} <- Members, Node =/= node()];
config_peers(_Config, _State) ->
    error(membership_not_set).

%% Returns the current effective RAFT configuration. This is the most recent configuration
%% stored in either the RAFT log or the RAFT storage.
-spec config(State :: #raft_state{}) -> Config :: config().
config(#raft_state{log_view = View, cached_config = {ConfigIndex, Config}}) ->
    case wa_raft_log:config(View) of
        {ok, LogConfigIndex, LogConfig} when LogConfig =/= undefined, LogConfigIndex > ConfigIndex ->
            maybe_upgrade_config(LogConfig);
        {ok, _LogConfigIndex, _LogConfig} ->
            % This case will normally only occur when the log has leftover log entries from
            % previous incarnations of the RAFT server that have been applied but not yet
            % trimmed this incarnation.
            Config;
        not_found ->
            Config
    end;
config(#raft_state{cached_config = undefined} = State) ->
    config(State#raft_state{cached_config = {0, default_config(State)}}).

-spec default_config(State :: #raft_state{}) -> Config :: config().
default_config(#raft_state{}) ->
    #{version => ?RAFT_CONFIG_CURRENT_VERSION}.

-spec config_index(State :: #raft_state{}) -> ConfigIndex :: wa_raft_log:log_index().
config_index(#raft_state{log_view = View, cached_config = {ConfigIndex, _Config}}) ->
    case wa_raft_log:config(View) of
        {ok, LogConfigIndex, LogConfig} when LogConfig =/= undefined, LogConfigIndex > ConfigIndex ->
            LogConfigIndex;
        {ok, _LogConfigIndex, _LogConfig} ->
            % This case will normally only occur when the log has leftover log entries from
            % previous incarnations of the RAFT server that have been applied but not yet
            % trimmed this incarnation.
            ConfigIndex;
        not_found ->
            ConfigIndex
    end;
config_index(#raft_state{cached_config = undefined} = State) ->
    config_index(State#raft_state{cached_config = {0, default_config(State)}}).

%% Loads and caches the current configuration stored in the RAFT storage.
%% This configuration is used whenever there is no newer configuration
%% available in the RAFT log and so needs to be kept in sync with what
%% the RAFT server expects is in storage.
-spec load_config(State :: #raft_state{}) -> NewState :: #raft_state{}.
load_config(#raft_state{storage = Storage} = State) ->
    case wa_raft_storage:read_metadata(Storage, config) of
        {ok, #raft_log_pos{index = ConfigIndex}, Config} ->
            State#raft_state{cached_config = {ConfigIndex, maybe_upgrade_config(Config)}};
        undefined ->
            State#raft_state{cached_config = undefined};
        {error, Reason} ->
            error({could_not_load_config, Reason})
    end.

%% Maybe upgrade an old configuration to a new configuration compatibility
%% version.
-spec maybe_upgrade_config(RawConfig :: map()) -> Config :: config().
maybe_upgrade_config(#{version := ?RAFT_CONFIG_CURRENT_VERSION} = Config) ->
    Config.

%% After an apply is sent to storage, check to see if it is a new configuration
%% being applied. If it is, then update the cached configuration.
-spec maybe_update_config(Index :: wa_raft_log:log_index(), Term :: wa_raft_log:log_term(),
                          Op :: wa_raft_acceptor:op() | [] | undefined, State :: #raft_state{}) -> NewState :: #raft_state{}.
maybe_update_config(Index, _Term, {_Ref, {config, Config}}, State) ->
    State#raft_state{cached_config = {Index, Config}};
maybe_update_config(_Index, _Term, _Op, State) ->
    State.

%%
%% Private functions
%%
-spec random_election_timeout() -> non_neg_integer().
random_election_timeout() ->
    Max = ?RAFT_ELECTION_TIMEOUT_MS_MAX(),
    Min = ?RAFT_ELECTION_TIMEOUT_MS(),
    Timeout =
        case Max > Min of
            true -> Min + rand:uniform(Max - Min);
            false -> Min
        end,
    case ?RAFT_CONFIG(raft_election_weight, ?RAFT_ELECTION_DEFAULT_WEIGHT) of
        Weight when Weight > 0 andalso Weight =< ?RAFT_ELECTION_MAX_WEIGHT ->
            % higher weight, shorter timeout so it has more chances to initiate an leader election
            round(Timeout * ?RAFT_ELECTION_MAX_WEIGHT div Weight);
        _ ->
            Timeout * ?RAFT_ELECTION_DEFAULT_WEIGHT
    end.

%% [RequestVote RPC]
%% RAFT servers should completely ignore normal RequestVote RPCs that it receives
%% from any peer if it knows about a currently active leader. An active leader is
%% one that is replicating to a quorum so we can check if we have gotten a
%% heartbeat recently.
-spec filter_vote_request(StateName :: state(), Type :: gen_statem:event_type(),
                          Event :: request_vote(), State :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
filter_vote_request(StateName, Type, ?REQUEST_VOTE_RPC(Term, SenderNode, normal, LastLogIndex, LastLogTerm),
                    #raft_state{name = Name, current_term = CurrentTerm, leader_heartbeat_ts = LeaderHeartbeatTs} = State) ->
    AllowedDelay = ?RAFT_ELECTION_TIMEOUT_MS() div 2,
    Delay = case LeaderHeartbeatTs of
        undefined -> infinity;
        _         -> erlang:system_time(millisecond) - LeaderHeartbeatTs
    end,
    case Delay > AllowedDelay of
        true ->
            % We have not gotten a heartbeat from the leader recently so allow this vote request
            % to go through by reraising it with the special 'allowed' type.
            {keep_state, State, {next_event, Type, ?REQUEST_VOTE_RPC(Term, SenderNode, allowed, LastLogIndex, LastLogTerm)}};
        false ->
            % We have gotten a heartbeat recently so drop this vote request.
            % Log this at debug level because we may end up with alot of these when we have
            % removed a server from the cluster but not yet shut it down.
            ?RAFT_COUNT('raft.server.request_vote.drop'),
            ?LOG_DEBUG("~p[~p, term ~p] dropping normal vote request from ~p because leader was still active ~p ms ago (allowed ~p ms).",
                [StateName, Name, CurrentTerm, SenderNode, Delay, AllowedDelay], #{domain => [whatsapp, wa_raft]}),
            {keep_state, State}
    end.

-spec apply_single_node_cluster(State0 :: #raft_state{}) -> State1 :: #raft_state{}.
apply_single_node_cluster(#raft_state{name = Name, log_view = View0} = State0) ->
    % TODO(hsun324) T112326686: Review after RAFT RPC id changes.
    case config_membership(config(State0)) of
        [{Name, Node}] when Node =:= node() ->
            View1 = case wa_raft_log:sync(View0) of
                {ok, L} -> L;
                _       -> View0
            end,
            maybe_apply(infinity, State0#raft_state{log_view = View1});
        _ ->
            State0
    end.

%% Leader - check quorum and apply logs if necessary
-spec maybe_apply(EndIndex :: infinity | wa_raft_log:log_index(), State0 :: #raft_state{}) -> State1 :: #raft_state{}.
maybe_apply(EndIndex, #raft_state{name = Name, log_view = View, current_term = CurrentTerm,
                                  match_index = MatchIndex, commit_index = LastCommitIndex, last_applied = LastAppliedIndex} = State0) when EndIndex > LastCommitIndex ->
    % Raft paper section 5.4.3 - Only log entries from the leader’s current term are committed
    % by counting replicas; once an entry from the current term has been committed in this way,
    % then all prior entries are committed indirectly because of the View Matching Property
    % NOTE: See comment in successful branch of AppendEntriesResponse RPC handling for leader for
    %       precautions about changing match index handling.
    Config = config(State0),
    case max_index_to_apply(MatchIndex, wa_raft_log:last_index(View), Config) of
        CommitIndex when CommitIndex > LastCommitIndex ->
            case wa_raft_log:term(View, CommitIndex) of
                {ok, CurrentTerm} ->
                    % log entry is same term of current leader node
                    TrimIndex = lists:min(to_member_list(MatchIndex#{node() => LastAppliedIndex}, 0, Config)),
                    apply_log(State0, CommitIndex, TrimIndex);
                {ok, Term} when Term < CurrentTerm ->
                    % Raft paper section 5.4.3 - as a leader, don't commit entries from previous term if no log entry of current term has applied yet
                    ?RAFT_COUNT('raft.apply.delay.old'),
                    ?LOG_WARNING( "[~p] delays commit ~p in term ~p. Current term ~p", [Name, EndIndex, Term, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
                    State0;
                _ ->
                    State0
            end;
        _ ->
            State0 %% no quorum yet
    end;
maybe_apply(_EndIndex, State) ->
    State.

% Return the max index to potentially apply on the leader. This is the latest log index that
% has achieved replication on at least a quorum of nodes in the current RAFT cluster.
% NOTE: We do not need to enforce that the leader should not commit entries from previous
%       terms here (5.4.2) because we only update the CommitIndex broadcast by the leader
%       when we actually apply the log entry on the leader. (See `apply_log` for information
%       about how this rule is enforced there.)
-spec max_index_to_apply(#{node() => wa_raft_log:log_index()}, wa_raft_log:log_index(), Config :: config()) -> wa_raft_log:log_index().
max_index_to_apply(MatchIndex, LastIndex, Config) ->
    compute_quorum(MatchIndex#{node() => LastIndex}, 0, Config).

%% Create a new list with exactly one element for each member in the membership
%% defined in the provided configuration taking the value mapped to each member in
%% the provided map or a provided default if a pairing is not available.
-spec to_member_list(Mapping :: #{node() => Value}, Default :: Value, Config :: config()) -> Normalized :: [Value].
to_member_list(Mapping, Default, Config) ->
    [maps:get(Node, Mapping, Default) || {_, Node} <- config_membership(Config)].

%% Compute the quorum maximum value for the current membership given a config for
%% the values represented by the given a mapping of peers (see note on config about
%% RAFT RPC ids) to values assuming a default value for peers who are not represented
%% in the mapping.
-spec compute_quorum(Mapping :: #{node() => Value}, Default :: Value, Config :: config()) -> Quorum :: Value.
compute_quorum(Mapping, Default, Config) ->
    compute_quorum(to_member_list(Mapping, Default, Config)).

%% Given a set of values $V$, compute the greatest $Q$ s.t. there exists
%% at least a quorum of values $v_i \in V$ for which $v_i \ge Q$.
-spec compute_quorum(Values :: [Value]) -> Quorum :: Value.
compute_quorum([_|_] = Values) ->
    %% When taking element $k$ from a sorted list $|V| = n$, we know that all elements
    %% of the sorted list $v_k ... v_n$ will be at least $v_k$. With $k = ceil(|V| / 2)$,
    %% we can compute the quorum.
    Index = (length(Values) + 1) div 2,
    lists:nth(Index, lists:sort(Values)).

-spec apply_log(State0 :: #raft_state{}, CommitIndex :: wa_raft_log:log_index(), TrimIndex :: wa_raft_log:log_index() | infinity) -> State1 :: #raft_state{}.
apply_log(#raft_state{witness = true} = State0, CommitIndex, _) ->
    % Update CommitIndex and LastAppliedIndex, but don't apply new log entries
    State0#raft_state{last_applied = CommitIndex, commit_index = CommitIndex};
apply_log(#raft_state{name = Name, table = Table, partition = Partition, log_view = View,
                      last_applied = LastApplied} = State0, CommitIndex, TrimIndex) when CommitIndex > LastApplied ->
    StartT = os:timestamp(),
    case wa_raft_queue:apply_queue_full(Table, Partition) of
        false ->
            % Apply a limited number of log entries (both count and total byte size limited)
            LimitedIndex = erlang:min(CommitIndex, LastApplied + ?MAX_LOG_APPLY_BATCH_SIZE),
            LimitBytes = ?RAFT_CONFIG(raft_apply_batch_max_bytes, 200 * 4 * 1024),
            {ok, {_, #raft_state{log_view = View1} = State1}} = wa_raft_log:fold(View, LastApplied + 1, LimitedIndex, LimitBytes,
                fun (Index, Entry, {Index, State}) ->
                    wa_raft_queue:reserve_apply(Table, Partition),
                    {Index + 1, apply_op(Index, Entry, State)}
                end, {LastApplied + 1, State0}),

            % Perform log trimming since we've now applied some log entries, only keeping
            % at maximum MaxRotateDelay log entries.
            MaxRotateDelay = ?RAFT_CONFIG(max_log_rotate_delay, 1500000),
            RotateIndex = max(LimitedIndex - MaxRotateDelay, min(State1#raft_state.last_applied, TrimIndex)),
            RotateIndex =/= infinity orelse error(bad_state),
            {ok, View2} = wa_raft_log:rotate(View1, RotateIndex),
            State2 = State1#raft_state{log_view = View2},
            ?RAFT_GATHER('raft.apply_log.latency_us', timer:now_diff(os:timestamp(), StartT)),
            State2;
        true ->
            ApplyQueueSize = wa_raft_queue:apply_queue_size(Table, Partition),
            ?RAFT_COUNT('raft.apply.delay'),
            ?RAFT_GATHER('raft.apply.queue', ApplyQueueSize),
            LastApplied rem 10 =:= 0 andalso
                ?LOG_WARNING("[~p] delays applying for long queue ~p. last applied ~p",
                    [Name, ApplyQueueSize, LastApplied], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_GATHER('raft.apply_log.latency_us', timer:now_diff(os:timestamp(), StartT)),
            State0
    end;
apply_log(State, _CommitIndex, _TrimIndex) ->
    State.

-spec apply_op(wa_raft_log:log_index(), wa_raft_log:log_entry(), #raft_state{}) -> #raft_state{}.
apply_op(LogIndex, _Entry, #raft_state{name = Name, last_applied = LastAppliedIndex} = State) when LogIndex =< LastAppliedIndex ->
    ?LOG_WARNING("Skip apply_op because log ~p already applied on ~p", [LogIndex, Name], #{domain => [whatsapp, wa_raft]}),
    State;
apply_op(LogIndex, {Term, Op}, #raft_state{current_term = CurrentTerm, storage = Storage} = State0) ->
    wa_raft_storage:apply_op(Storage, {LogIndex, {Term, Op}}, CurrentTerm),
    maybe_update_config(LogIndex, Term, Op, State0#raft_state{last_applied = LogIndex, commit_index = LogIndex});
apply_op(LogIndex, undefined, #raft_state{name = Name, log_view = View}) ->
    ?RAFT_COUNT('raft.server.missing.log.entry'),
    ?LOG_ERROR("~p: fail to apply because of log rotation. Current pos ~p was rotated before applied. Log view: ~p", [Name, LogIndex, View], #{domain => [whatsapp, wa_raft]}),
    exit({invalid_op, LogIndex});
apply_op(LogIndex, Entry, #raft_state{name = Name}) ->
    ?RAFT_COUNT('raft.server.corrupted.log.entry'),
    ?LOG_ERROR("~p: fail to apply unrecognized entry ~p at ~p", [Name, Entry, LogIndex], #{domain => [whatsapp, wa_raft]}),
    exit({invalid_op, LogIndex, Entry}).

-spec append_entries_to_followers(State0 :: #raft_state{}) -> State1 :: #raft_state{}.
append_entries_to_followers(#raft_state{name = Name, log_view = View0, storage = Storage} = State0) ->
    State1 = case wa_raft_log:sync(View0) of
        {ok, View1} ->
            State0#raft_state{log_view = View1};
        skipped ->
            {ok, Pending, View1} = wa_raft_log:cancel(View0),
            ?RAFT_COUNT('raft.server.sync.skipped'),
            ?LOG_WARNING("~p: skipping pre-heartbeat sync for ~p log entr(ies)", [Name, length(Pending)], #{domain => [whatsapp, wa_raft]}),
            lists:foreach(
                fun ({_Term, {Ref, _Op}}) -> wa_raft_storage:fulfill_op(Storage, Ref, {error, commit_stalled});
                    (_)                   -> ok
                end, Pending),
            State0#raft_state{log_view = View1};
        {error, Error} ->
            ?RAFT_COUNT({'raft.server.sync', Error}),
            ?LOG_ERROR("~p: sync failed due to ~p", [Name, Error], #{domain => [whatsapp, wa_raft]}),
            error(Error)
    end,
    %% TODO(hsun324): Fix assumption about name when we support multi-partition cluster
    lists:foldl(
        fun ({_Name, FollowerId}, StateN) when FollowerId =:= node() -> StateN;
            ({_Name, FollowerId}, StateN)                            -> heartbeat(FollowerId, StateN)
        end, State1, config_membership(config(State1))).

%% Determines the current node's vote for the provided candidate based on
%% the election requirements and log restriction (5.2, 5.4.1). Cases:
-spec determine_vote(node(), wa_raft_log:log_index(), wa_raft_log:log_term(), #raft_state{}) -> boolean().
%%  1. current node already voted for another node -> deny vote
determine_vote(CandidateId, _LastLogIndex, _LastLogTerm, #raft_state{voted_for = VotedFor}) when VotedFor =/= undefined andalso VotedFor =/= CandidateId ->
    false;
%%  2. current node has not yet voted for already voted for candidate -> vote if log is at least up-to-date
determine_vote(_CandidateId, LastLogIndex, LastLogTerm, #raft_state{log_view = View}) ->
    MyLastIndex = wa_raft_log:last_index(View),
    {ok, MyLastTerm} = wa_raft_log:term(View, MyLastIndex),
    %% Raft spec 5.4.1 election rule
    % - If the logs have last entries with different terms, then the log with the later term is more up-to-date
    % - If the logs end with the same term, then whichever log is longer is more up-to-date
    LastLogTerm > MyLastTerm orelse (LastLogTerm =:= MyLastTerm andalso LastLogIndex >= MyLastIndex).

%% Casts a vote for the provided candidate according to `determine_vote`.
-spec vote(node(), wa_raft_log:log_index(), wa_raft_log:log_term(), #raft_state{}) -> #raft_state{}.
vote(CandidateId, LastLogIndex, LastLogTerm, State) ->
    Vote = determine_vote(CandidateId, LastLogIndex, LastLogTerm, State),
    % We persist state before responding to request vote RPC.
    NewState = case Vote of
        true  -> State#raft_state{voted_for = CandidateId};
        false -> State
    end,
    wa_raft_durable_state:store(NewState),
    cast_vote(CandidateId, Vote, State),
    NewState.

-spec cast_vote(node(), boolean(), #raft_state{}) -> ok.
cast_vote(CandidateId, Vote, #raft_state{name = Name, current_term = CurrentTerm, log_view = View} = State) ->
    MyLastIndex = wa_raft_log:last_index(View),
    {ok, MyLastTerm} = wa_raft_log:term(View, MyLastIndex),
    ?LOG_NOTICE("~p vote ~p to candidate ~p in term ~p. Current last log ~p:~p",
        [Name, Vote, CandidateId, CurrentTerm, MyLastIndex, MyLastTerm], #{domain => [whatsapp, wa_raft]}),
    send_rpc(CandidateId, ?VOTE_RPC(CurrentTerm, node(), Vote), State),
    ok.

-spec reset_state(#raft_state{}) -> #raft_state{}.
reset_state(#raft_state{log_view = View0} = State) ->
    %% drop all pending log entries
    {ok, _Pending, View1} = wa_raft_log:cancel(View0),
    %% reset common state
    State#raft_state{log_view = View1, handover = undefined}.

-spec advance_term(wa_raft_log:log_term(), #raft_state{}) -> #raft_state{}.
advance_term(CurrentTerm, #raft_state{current_term = CurrentTerm} = State) ->
    % no-op when the term is *unchanged*
    State;
advance_term(NewTerm, #raft_state{name = Name, current_term = CurrentTerm} = State) when NewTerm < CurrentTerm ->
    ?LOG_WARNING("~p attempts to *advance* to invalid term ~p when current term is ~p.", [Name, NewTerm, CurrentTerm]),
    State;
advance_term(NewTerm, State) ->
    advance_term(NewTerm, undefined, State).

-spec advance_term(wa_raft_log:log_term(), node(), #raft_state{}) -> #raft_state{}.
advance_term(NewTerm, VotedFor, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    ?RAFT_COUNT('raft.server.advance_term'),
    ?LOG_NOTICE("~p advances from term ~p to term ~p. (voted_for = ~p)",
                [Name, CurrentTerm, NewTerm, VotedFor], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{
        leader_id = undefined,
        current_term = NewTerm,
        voted_for = VotedFor},
    notify_leader_change(State1),
    wa_raft_durable_state:store(State1),
    State1.

%% Leader private functions
-spec reset_leader_state(#raft_state{}) -> #raft_state{}.
reset_leader_state(#raft_state{current_term = CurrentTerm, log_view = View0} = State0) ->
    % Reset state
    State1 = State0#raft_state{next_index = #{}, match_index = #{}, leader_id = node()},
    notify_leader_change(State1),

    % During promotion, we may add a config update operation to the end of the log.
    % If there is such an operation and it has the same term number as the current term,
    % then make sure to set the start of the current term so that it gets replicated
    % immediately.
    LastLogIndex = wa_raft_log:last_index(View0),
    case wa_raft_log:get(View0, LastLogIndex) of
        {ok, {CurrentTerm, {_Ref, {config, _NewConfig}}}} ->
            State1#raft_state{first_current_term_log_index = LastLogIndex};
        {ok, _} ->
            % Otherwise, the server should add a new log entry to start the current term.
            % This will flush pending commits, clear out and log mismatches on replicas,
            % and establish a quorum as soon as possible.
            {ok, NewLastLogIndex, View1} = wa_raft_log:append(View0, [{CurrentTerm, {make_ref(), noop}}]),
            State1#raft_state{log_view = View1, first_current_term_log_index = NewLastLogIndex}
    end.

-spec heartbeat(node(), #raft_state{}) -> #raft_state{}.
heartbeat(FollowerId, #raft_state{name = Name, log_view = View, catchup = Catchup, current_term = CurrentTerm,
                                  commit_index = CommitIndex, next_index = NextIndex0, match_index = MatchIndex,
                                  last_heartbeat_ts = LastHeartbeatTs, first_current_term_log_index = TermStartIndex} = State0) ->
    FollowerNextIndex = maps:get(FollowerId, NextIndex0, TermStartIndex),
    PrevLogIndex = FollowerNextIndex - 1,
    PrevLogTermRes = wa_raft_log:term(View, PrevLogIndex),
    FollowerMatchIndex = maps:get(FollowerId, MatchIndex, 0),
    FollowerMatchIndex =/= 0 andalso
        ?RAFT_GATHER('raft.leader.follower.lag', CommitIndex - FollowerMatchIndex),
    IsCatchingUp = wa_raft_log_catchup:is_catching_up(Catchup, FollowerId),
    NowTs = erlang:system_time(millisecond),
    LastFollowerHeartbeatTs = maps:get(FollowerId, LastHeartbeatTs, undefined),
    State1 = State0#raft_state{last_heartbeat_ts = LastHeartbeatTs#{FollowerId => NowTs}, leader_heartbeat_ts = NowTs},
    LastIndex = wa_raft_log:last_index(View),
    Witnesses = config_witnesses(config(State0)),
    case PrevLogTermRes =:= not_found orelse IsCatchingUp of %% catching up, or prep
        true ->
            {ok, LastTerm} = wa_raft_log:term(View, LastIndex),
            ?LOG_DEBUG("Leader[~p term ~p last log ~p] send empty heartbeat to follower ~p(prev ~p, catching-up ~p)",
                [Name, CurrentTerm, LastIndex, FollowerId, PrevLogIndex, IsCatchingUp], #{domain => [whatsapp, wa_raft]}),
            % Send append entries request.
            send_rpc(FollowerId, ?APPEND_ENTRIES_RPC(CurrentTerm, node(), LastIndex, LastTerm, [], CommitIndex, 0), State1),
            LastFollowerHeartbeatTs =/= undefined andalso ?RAFT_GATHER('raft.leader.heartbeat.interval_ms', erlang:system_time(millisecond) - LastFollowerHeartbeatTs),
            State1;
        false ->
            MaxHeartbeatSize = ?RAFT_CONFIG(raft_max_heartbeat_size, 1 * 1024 * 1024),
            Entries =
                case lists:member({Name, FollowerId}, Witnesses) of
                    true ->
                        MaxWitnessLogEntries = ?RAFT_CONFIG(raft_max_witness_log_entries_per_heartbeat, 250),
                        {ok, Terms} = wa_raft_log:get_terms(View, FollowerNextIndex, MaxWitnessLogEntries, MaxHeartbeatSize),
                        [{Term, []} || Term <- Terms];
                    _ ->
                        MaxLogEntries = ?RAFT_CONFIG(raft_max_log_entries_per_heartbeat, 15),
                        {ok, Ret} = wa_raft_log:get(View, FollowerNextIndex, MaxLogEntries, MaxHeartbeatSize),
                        Ret
                    end,
            {ok, PrevLogTerm} = PrevLogTermRes,
            ?RAFT_GATHER('raft.leader.heartbeat.size', length(Entries)),
            ?LOG_DEBUG("Leader[~p, term ~p] heartbeat to follower ~p from ~p(~p entries). Commit index ~p",
                [Name, CurrentTerm, FollowerId, FollowerNextIndex, length(Entries), CommitIndex], #{domain => [whatsapp, wa_raft]}),
            % Compute trim index.
            TrimIndex = lists:min(to_member_list(MatchIndex#{node() => LastIndex}, 0, config(State1))),
            % Send append entries request.
            CastResult = send_rpc(FollowerId, ?APPEND_ENTRIES_RPC(CurrentTerm, node(), PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex), State1),
            NextIndex1 =
                case CastResult of
                    ok ->
                        % pipelining - move NextIndex after sending out logs. If a packet is lost, follower's AppendEntriesResponse
                        % will return send back its correct index
                        maps:put(FollowerId, PrevLogIndex + length(Entries) + 1, NextIndex0);
                    _ ->
                        NextIndex0
                end,
            LastFollowerHeartbeatTs =/= undefined andalso ?RAFT_GATHER('raft.leader.heartbeat.interval_ms', erlang:system_time(millisecond) - LastFollowerHeartbeatTs),
            State1#raft_state{next_index = NextIndex1}
    end.

-spec compute_handover_candidates(State :: #raft_state{}) -> [node()].
compute_handover_candidates(#raft_state{log_view = View, match_index = MatchIndex}) ->
    LastLogIndex = wa_raft_log:last_index(View),
    MaxHandoverLogEntries = ?RAFT_MAX_HANDOVER_LOG_ENTRIES(),
    [Peer || {Peer, Match} <- maps:to_list(MatchIndex), LastLogIndex - Match =< MaxHandoverLogEntries].

-spec adjust_config(Action :: {add, peer()} | {remove, peer()} | {add_witness, peer()} | {remove_witness, peer()} | {refresh, undefined},
                    Config :: config(), State :: #raft_state{}) -> {ok, NewConfig :: config()} | {error, Reason :: atom()}.
adjust_config(Action, Config, #raft_state{name = Name}) ->
    Node = node(),
    Membership = config_membership(Config),
    Witness = config_witnesses(Config),
    case Action of
        % The 'refresh' action is used to commit the current effective configuration to storage in the
        % case of upgrading from adhoc to stored configuration or to materialize changes to the format
        % of stored configurations.
        {refresh, undefined} ->
            {ok, Config};
        {add, Peer} ->
            case lists:member(Peer, Membership) of
                true  -> {error, already_member};
                false -> {ok, Config#{membership => [Peer | Membership]}}
            end;
        {add_witness, Peer} ->
            case {lists:member(Peer, Witness), lists:member(Peer, Membership)} of
                {true, true}  -> {error, already_witness};
                {true, _} -> {error, already_member};
                {false, _} -> {ok, Config#{membership => [Peer | Membership], witness => [Peer | Witness]}}
            end;
        {remove, Peer} ->
            case {Peer, lists:member(Peer, Membership)} of
                {{Name, Node}, _} -> {error, cannot_remove_self};
                {_, false}        -> {error, not_a_member};
                {_, true}         -> {ok, Config#{membership => lists:delete(Peer, Membership), witness => lists:delete(Peer, Witness)}}
            end;
        {remove_witness, Peer} ->
            case {Peer, lists:member(Peer, Witness)} of
                {{Name, Node}, _} -> {error, cannot_remove_self};
                {_, false}        -> {error, not_a_witness};
                {_, true}         -> {ok, Config#{membership => lists:delete(Peer, Membership), witness => lists:delete(Peer, Witness)}}
            end
    end.


%% follower private functions

%% [5.3] AppendEntries RPC implementation for followers

-spec append_entries(Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, State) -> {Result, NewState} when
    Term :: wa_raft_log:log_term(),
    LeaderId :: node(),
    PrevLogIndex :: wa_raft_log:log_index(),
    PrevLogTerm :: wa_raft_log:log_term(),
    Entries :: [wa_raft_log:log_entry()],
    State :: #raft_state{},
    Result ::
          ok                 % the append was successful
        | invalid            % the request was malformed and no update should occur on leader
        | failed             % the request failed due to expected reasons
        | {disable, Reason}, % an unrecoverable error occurred
    Reason :: term(),
    NewState :: #raft_state{}.

%% [5.1] Drop stale RPCs
append_entries(Term, _LeaderId, _PrevLogIndex, _PrevLogTerm, _Entries,
               #raft_state{name = Name, current_term = CurrentTerm} = State) when Term =/= CurrentTerm ->
    ?LOG_ERROR("Follower[~p, term ~p] dropping AppendEntries RPC with different term ~p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    {failed, State};

%% Handle first heartbeat from a leader
append_entries(Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries,
               #raft_state{name = Name, current_term = CurrentTerm, leader_id = PrevLeaderId} = State0) when PrevLeaderId =:= undefined orelse PrevLeaderId =/= LeaderId ->
    ?LOG_NOTICE("Follower[~p, term ~p] receives first heartbeat from leader ~p. Previous leader is ~p", [Name, CurrentTerm, LeaderId, PrevLeaderId], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{leader_id = LeaderId},
    notify_leader_change(State1),
    append_entries(Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, State1);

%% Normal operation
append_entries(_Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries,
               #raft_state{current_term = CurrentTerm, log_view = View, name = Name, last_applied = LastApplied} = State) ->
    ?LOG_DEBUG("Follower[~p, term ~p] appending ~p log entries starting from ~p",
        [Name, CurrentTerm, length(Entries), PrevLogIndex + 1], #{domain => [whatsapp, wa_raft]}),
    ?RAFT_GATHER('raft.follower.heartbeat.size', length(Entries)),
    case wa_raft_log:term(View, PrevLogIndex) of
        {ok, PrevLogTerm} -> % PrevLogIndex term matches PrevLogTerm
            {ok, _LogLastIndex, View1} = wa_raft_log:append(View, PrevLogIndex + 1, Entries),
            {ok, State#raft_state{log_view = View1}};
        {ok, ReadPrevLogTerm} -> % PrevLogIndex term doesn't match PrevLogTerm
            % [5.3] If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
            %       However, if we are trying to delete log entries that have already been applied then we might be facing data corruption.
            ?RAFT_COUNT('raft.follower.heartbeat.skip.mismatch_prev_log_term'),
            ?LOG_WARNING(
                "Follower[~p, term ~p]: skip log appending from ~p. Prev log ~p term ~p doesn't match mine ~p. last applied ~p. Truncate it",
                [Name, CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, ReadPrevLogTerm, LastApplied], #{domain => [whatsapp, wa_raft]}),
            case PrevLogIndex < LastApplied of
                true ->
                    % We are trying to delete already applied log entries so disable this partition due to
                    % critical invariant failure (LastApplied <= CommitIndex <= LastLogIndex).
                    ?RAFT_COUNT('raft.follower.error.corrupted'),
                    ?LOG_WARNING("Follower[~p, term ~p] prev log ~p < last applied ~p. Request new snapshot from leader ~p",
                        [Name, CurrentTerm, PrevLogIndex, LastApplied, LeaderId], #{domain => [whatsapp, wa_raft]}),
                    Reason = io_lib:format("Mismatched term found for already applied log index ~p replicated from from ~0p:~0p (last applied was ~0p).",
                                           [PrevLogIndex, LeaderId, CurrentTerm, LastApplied]),
                    {{disable, Reason}, State};
                false ->
                    % We are not deleting already applied log entries, so proceed with truncation.
                    {ok, View1} = wa_raft_log:truncate(View, PrevLogIndex),
                    {failed, State#raft_state{log_view = View1}}
            end;
        not_found ->
            ?RAFT_COUNT('raft.follower.heartbeat.skip.no_prev_log'),
            ?LOG_WARNING("Follower[~p, term ~p]: skip log appending from ~p. Prev log ~p doesn't exist",
                [Name, CurrentTerm, LeaderId, PrevLogIndex], #{domain => [whatsapp, wa_raft]}),
            {failed, State};
        {error, Reason} ->
            ?RAFT_COUNT('raft.follower.heartbeat.skip.read_error_prev_log_term'),
            ?LOG_WARNING("Follower[~p, term ~p]: skip log appending from ~p. Prev log ~p read error: ~p",
                [Name, CurrentTerm, LeaderId, PrevLogIndex, Reason], #{domain => [whatsapp, wa_raft]}),
            {failed, State}
    end.

-spec notify_leader_change(#raft_state{}) -> term().
notify_leader_change(#raft_state{table = Table, partition = Partition, leader_id = undefined}) ->
    ?LOG_NOTICE("No leader for ~p:~p", [Table, Partition], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_leader(Table, Partition, undefined);
notify_leader_change(#raft_state{table = Table, partition = Partition, leader_id = LeaderId}) ->
    ?LOG_NOTICE("Change leader to ~p for ~p:~p", [LeaderId, Table, Partition], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_leader(Table, Partition, LeaderId).

%% Generic reply function that operates based on event type.
-spec reply(Type :: enter | gen_statem:event_type(), Message :: term()) -> ok | wa_raft:error().
reply(cast, _Message) ->
    ok;
reply({call, From}, Message) ->
    gen_statem:reply(From, Message);
reply(Type, Message) ->
    ?LOG_WARNING("Attempted to reply to non-reply event type ~p with message ~0P.",
        [Type, Message, 100], #{domain => [whatsapp, wa_raft]}),
    ok.

-spec send_rpc(node() | peer(), normalized_procedure_call(), #raft_state{}) -> term().
send_rpc(NodeOrPeer, ?NOTIFY_TERM_RPC(_Term, Name, Node), #raft_state{current_term = Term} = State) ->
    ?MODULE:cast(NodeOrPeer, ?RAFT_NAMED_RPC(?NOTIFY_TERM, Term, Name, Node, undefined), State);
send_rpc(NodeOrPeer, ?PROCEDURE_CALL(Procedure, _Term, Node, Payload), #raft_state{current_term = Term} = State) ->
    ?MODULE:cast(NodeOrPeer, ?RAFT_RPC(Procedure, Term, Node, Payload), State).

-spec cast(node() | peer(), rpc(), #raft_state{}) -> ok | {error, term()}.
cast(DestIdOrPeer, Message, #raft_state{name = Name}) ->
    % TODO(hsun324): T112326686 - remove compatability with just node after all RPCs migrated
    DestPeer = case DestIdOrPeer of
        {_, _} -> DestIdOrPeer;
        _      -> {Name, DestIdOrPeer}
    end,
    try
        % TODO(hsun324): T112326686 - upgrade clause to add assumed name to RPCs without SenderName
        AdjustedMessage =
            case {?RAFT_CONFIG(upgrade_rpc_with_name, false), Message} of
                {true, ?RAFT_RPC(Type, Term, SenderId, Payload)} -> ?RAFT_NAMED_RPC(Type, Term, Name, SenderId, Payload);
                _                                                -> Message
            end,

        % TODO(hsun324): For the time being, assume that all members of the cluster use the same server name.
        ok = ?RAFT_DISTRIBUTION_MODULE:cast(DestPeer, AdjustedMessage)
    catch
        _:E ->
            ?RAFT_COUNT({'raft.server.cast.error', E}),
            ?LOG_DEBUG("Cast to ~p error ~100p", [DestPeer, E], #{domain => [whatsapp, wa_raft]}),
            {error, E}
    end.

-spec broadcast(normalized_procedure_call(), #raft_state{}) -> ok.
broadcast(Message, State) ->
    Peers = config_peers(config(State), State),
    lists:foreach(fun({Id, _Addr}) -> catch ok = send_rpc(Id, Message, State) end, Peers).

-spec maybe_heartbeat(#raft_state{}) -> #raft_state{}.
maybe_heartbeat(State) ->
    case should_heartbeat(State) of
        true ->
            ?RAFT_COUNT('raft.leader.heartbeat'),
            append_entries_to_followers(State);
        false ->
            State
    end.

-spec should_heartbeat(#raft_state{}) -> boolean().
should_heartbeat(#raft_state{handover = Handover}) when Handover =/= undefined ->
    false;
should_heartbeat(#raft_state{last_heartbeat_ts = LastHeartbeatTs}) ->
    Latest = lists:max(maps:values(LastHeartbeatTs)),
    Current = erlang:system_time(millisecond),
    Current - Latest > ?RAFT_CONFIG(raft_heartbeat_interval_ms, 120).

%% Check follower/candidate staleness due to heartbeat delay
-spec check_follower_stale(state(), #raft_state{}) -> term().
check_follower_stale(FSMState, #raft_state{name = Name, table = Table, partition = Partition,
                                           current_term = CurrentTerm, leader_heartbeat_ts = LeaderHeartbeatTs} = State) ->
    HeartbeatMs = case LeaderHeartbeatTs of
        undefined -> infinity;
        _         -> erlang:system_time(millisecond) - LeaderHeartbeatTs
    end,
    Stale = HeartbeatMs > ?RAFT_CONFIG(raft_follower_heartbeat_stale_ms, 10000),
    case wa_raft_info:get_stale(Table, Partition) of
        Stale ->
            ok;
        _ ->
            ?LOG_NOTICE("~p[~p, term ~p] adjusts stale to ~p due to heartbeat delay of ~p ms.",
                [FSMState, Name, CurrentTerm, Stale, HeartbeatMs], #{domain => [whatsapp, wa_raft]}),
            wa_raft_info:set_stale(Stale, State)
    end.

%% Check follower/witness state due to log entry lag and change stale flag if needed
-spec check_follower_lagging(pos_integer(), #raft_state{}) -> ok.
check_follower_lagging(LeaderCommit, #raft_state{table = Table, partition = Partition, last_applied = LastApplied} = State) ->
    Lagging = LeaderCommit - LastApplied,
    ?RAFT_GATHER('raft.follower.lagging', Lagging),
    case Lagging < ?RAFT_CONFIG(raft_follower_max_lagging, 5000) of
        true ->
            wa_raft_info:get_stale(Table, Partition) =/= false andalso begin
                ?LOG_NOTICE("[~p:~p] Follower catches up.", [Table, Partition], #{domain => [whatsapp, wa_raft]}),
                wa_raft_info:set_stale(false, State)
            end;
        false ->
            wa_raft_info:get_stale(Table, Partition) =/= true andalso begin
                ?LOG_NOTICE("[~p:~p] Follower is far behind ~p(leader ~p, follower ~p)",
                        [Table, Partition, Lagging, LeaderCommit, LastApplied], #{domain => [whatsapp, wa_raft]}),
                wa_raft_info:set_stale(true, State)
            end
    end,
    ok.

%% Check leader state and set stale if needed
-spec check_leader_lagging(#raft_state{}) -> term().
check_leader_lagging(#raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm,
                                 heartbeat_response_ts = HeartbeatResponse} = State) ->
    NowTs = erlang:system_time(millisecond),
    QuorumTs = compute_quorum(HeartbeatResponse#{node() => NowTs}, 0, config(State)),

    Stale = wa_raft_info:get_stale(Table, Partition),
    QuorumAge = NowTs - QuorumTs,
    MaxAge = ?RAFT_CONFIG(raft_max_heartbeat_age_msecs, 180 * 1000),

    case QuorumAge >= MaxAge of
        Stale ->
            ok;
        true ->
            ?LOG_NOTICE("Leader[~p, term ~p] is now stale due to last heartbeat quorum age being ~p ms >= ~p ms max",
                [Name, CurrentTerm, QuorumAge, MaxAge], #{domain => [whatsapp, wa_raft]}),
            wa_raft_info:set_stale(true, State);
        false ->
            ?LOG_NOTICE("Leader[~p, term ~p] is no longer stale after heartbeat quorum age drops to ~p ms < ~p ms max",
                [Name, CurrentTerm, QuorumAge, MaxAge], #{domain => [whatsapp, wa_raft]}),
            wa_raft_info:set_stale(false, State)
    end.

%% Based on information that the leader has available as a result of heartbeat replies, attempt
%% to discern what the best subsequent replication mode would be for this follower.
-spec select_follower_replication_mode(wa_raft_log:log_index(), #raft_state{}) -> snapshot | bulk_logs | logs.
select_follower_replication_mode(FollowerLastIndex, #raft_state{log_view = View, last_applied = LastAppliedIndex}) ->
    CatchupEnabled = ?RAFT_CONFIG(catchup_enabled, true) =:= true,
    BulkLogThreshold = ?RAFT_CONFIG(catchup_max_follower_lag, 50000),
    LeaderFirstIndex = wa_raft_log:first_index(View),
    if
        % If catchup modes are not enabled, then always replicate using logs.
        not CatchupEnabled                                      -> logs;
        % Snapshot is required if the follower is stalled or we are missing
        % the logs required for incremental replication.
        FollowerLastIndex =:= 0                                 -> snapshot;
        LeaderFirstIndex > FollowerLastIndex                    -> snapshot;
        % Past a certain threshold, we should try to use bulk log catchup
        % to quickly bring the follower back up to date.
        LastAppliedIndex - FollowerLastIndex > BulkLogThreshold -> bulk_logs;
        % Otherwise, replicate normally.
        true                                                    -> logs
    end.

%% Try to start a snapshot transport to a follower if the snapshot transport
%% service is available. If the follower is a witness or too many snapshot
%% transports have been started then no transport is created. This function
%% always performs this request asynchronously.
-spec request_snapshot_for_follower(node(), #raft_state{}) -> term().
request_snapshot_for_follower(FollowerId, #raft_state{name = Name, table = Table, partition = Partition, data_dir = DataDir, log_view = View} = State) ->
    case lists:member({Name, FollowerId}, config_witnesses(config(State))) of
        true  ->
            % If node is a witness, we can bypass the transport process since we don't have to
            % send the full log.  Thus, we can run snapshot_available() here directly
            LastLogIndex = wa_raft_log:last_index(View),
            {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),
            LastLogPos = #raft_log_pos{index = LastLogIndex, term = LastLogTerm},
            wa_raft_server:snapshot_available({Name, FollowerId}, DataDir, LastLogPos);
        false ->
            wa_raft_snapshot_catchup:request_snapshot_transport(FollowerId, Table, Partition)
    end.

-spec request_bulk_logs_for_follower(node(), wa_raft_log:log_index(), #raft_state{}) -> ok.
request_bulk_logs_for_follower(FollowerId, FollowerEndIndex, #raft_state{name = Name, catchup = Catchup, current_term = CurrentTerm, commit_index = CommitIndex} = State) ->
    ?LOG_DEBUG("Leader[~p, term ~p] requesting bulk logs catchup for follower ~0p.",
        [Name, CurrentTerm, FollowerId], #{domain => [whatsapp, wa_raft]}),
    Witness = lists:member({Name, FollowerId}, config_witnesses(config(State))),
    wa_raft_log_catchup:start_catchup_request(Catchup, FollowerId, FollowerEndIndex, CurrentTerm, CommitIndex, Witness).

-spec cancel_bulk_logs_for_follower(node(), #raft_state{}) -> ok.
cancel_bulk_logs_for_follower(FollowerId, #raft_state{catchup = Catchup}) ->
    wa_raft_log_catchup:cancel_catchup_request(Catchup, FollowerId).
