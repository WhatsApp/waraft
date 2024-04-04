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
-compile(warn_missing_spec_all).
-behaviour(gen_statem).

%% OTP supervisor
-export([
    child_spec/1,
    start_link/1
]).

%% RAFT Cluster Config API
-export([
    make_config/1,
    make_config/2,
    get_config_members/1,
    get_config_witnesses/1,
    set_config_members/2,
    set_config_members/3
]).

%% API
-export([
    status/1,
    status/2,
    membership/1,
    stop/1,
    commit/2,
    read/2
]).

%% Internal API
-export([
    default_name/2,
    registered_name/2
]).

%% Internal API
-export([
    make_rpc/3,
    parse_rpc/2
]).

%% gen_statem callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3,
    stalled/3,
    leader/3,
    follower/3,
    candidate/3,
    disabled/3
]).

%% Internal RAFT Server API
-export([
    snapshot_available/3,
    promote/2,
    promote/3,
    promote/4,
    resign/1,
    refresh_config/1,
    adjust_membership/3,
    adjust_membership/4,
    handover/1,
    handover/2,
    handover_candidates/1,
    disable/2,
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
    config/0,
    membership/0,
    status/0
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").
-include("wa_raft_rpc.hrl").

%% Section 5.2. Randomized election timeout for fast election and to avoid split votes
-define(ELECTION_TIMEOUT(State), {state_timeout, random_election_timeout(State), election}).

%% Timeout in milliseconds before the next heartbeat is to be sent by a RAFT leader with no pending log entries
-define(HEARTBEAT_TIMEOUT(State),    {state_timeout, ?RAFT_HEARTBEAT_INTERVAL(State#raft_state.application), heartbeat}).
%% Timeout in milliseconds before the next heartbeat is to be sent by a RAFT leader with pending log entries
-define(COMMIT_BATCH_TIMEOUT(State), {state_timeout, ?RAFT_COMMIT_BATCH_INTERVAL(State#raft_state.application), batch_commit}).

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

-type event() :: rpc() | remote(normalized_procedure()) | command() | internal_event() | timeout_type().

-type rpc() :: rpc_named() | legacy_rpc().
-type legacy_rpc() :: ?LEGACY_RAFT_RPC(atom(), wa_raft_log:log_term(), node(), undefined | tuple()).
-type rpc_named() :: ?RAFT_NAMED_RPC(atom(), wa_raft_log:log_term(), atom(), node(), undefined | tuple()).

-type command() :: commit_command() | read_command() | status_command() | promote_command() | resign_command() | adjust_membership_command() | snapshot_available_command() |
                   handover_candidates_command() | handover_command() | enable_command() | disable_command().
-type commit_command()              :: ?COMMIT_COMMAND(wa_raft_acceptor:op()).
-type read_command()                :: ?READ_COMMAND(wa_raft_acceptor:read_op()).
-type status_command()              :: ?STATUS_COMMAND.
-type promote_command()             :: ?PROMOTE_COMMAND(wa_raft_log:log_term(), boolean(), config() | undefined).
-type resign_command()              :: ?RESIGN_COMMAND.
-type adjust_membership_command()   :: ?ADJUST_MEMBERSHIP_COMMAND(membership_action(), peer() | undefined, wa_raft_log:log_index() | undefined).
-type snapshot_available_command()  :: ?SNAPSHOT_AVAILABLE_COMMAND(string(), wa_raft_log:log_pos()).
-type handover_candidates_command() :: ?HANDOVER_CANDIDATES_COMMAND.
-type handover_command()            :: ?HANDOVER_COMMAND(node()).
-type enable_command()              :: ?ENABLE_COMMAND.
-type disable_command()             :: ?DISABLE_COMMAND(term()).

-type internal_event() :: advance_term_event() | force_election_event().
-type advance_term_event() :: ?ADVANCE_TERM(wa_raft_log:log_term()).
-type force_election_event() :: ?FORCE_ELECTION(wa_raft_log:log_term()).

-type timeout_type() :: election | heartbeat.

-type membership_action() :: add | add_witness | remove | remove_witness | refresh.

%% ==================================================
%%  OTP Supervision Callbacks
%% ==================================================

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => transient,
        shutdown => 30000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) -> {ok, Pid :: pid()} | ignore | wa_raft:error().
start_link(#raft_options{server_name = Name} = Options) ->
    gen_statem:start_link({local, Name}, ?MODULE, Options, []).

%% ==================================================
%%  RAFT Server - Cluster Config API
%% ==================================================

-spec get_config_members(Config :: config()) -> undefined | [#raft_identity{}].
get_config_members(#{version := ?RAFT_CONFIG_CURRENT_VERSION, membership := Members}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Members];
get_config_members(_Config) ->
    undefined.

-spec get_config_witnesses(Config :: config()) -> undefined | [#raft_identity{}].
get_config_witnesses(#{version := ?RAFT_CONFIG_CURRENT_VERSION, witness := Witnesses}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Witnesses];
get_config_witnesses(_Config) ->
    undefined.

-spec make_config(Members :: [#raft_identity{}]) -> config().
make_config(Members) ->
    set_config_members(Members, #{version => ?RAFT_CONFIG_CURRENT_VERSION}).

-spec make_config(Members :: [#raft_identity{}], Witnesses :: [#raft_identity{}]) -> config().
make_config(Members, Witnesses) ->
    set_config_members(Members, Witnesses, #{version => ?RAFT_CONFIG_CURRENT_VERSION}).

-spec set_config_members(Members :: [#raft_identity{}], Config :: config()) -> AdjustedConfig :: config().
set_config_members(Members, #{version := ?RAFT_CONFIG_CURRENT_VERSION} = Config) ->
    Config#{membership => [{Name, Node} || #raft_identity{name = Name, node = Node} <- Members]}.

-spec set_config_members(Members :: [#raft_identity{}], Witnesses :: [#raft_identity{}], Config :: config()) -> AdjustedConfig :: config().
set_config_members(Members, Witnesses, #{version := ?RAFT_CONFIG_CURRENT_VERSION} = Config) ->
    (set_config_members(Members, Config))#{witness => [{Name, Node} || #raft_identity{name = Name, node = Node} <- Witnesses]}.

%% ==================================================
%%  RAFT Server Internal API
%% ==================================================

%% Commit an op to the consensus group.
-spec commit(Pid :: atom() | pid(), Op :: wa_raft_acceptor:op()) -> ok | wa_raft:error().
commit(Pid, Op) ->
    gen_statem:cast(Pid, ?COMMIT_COMMAND(Op)).

%% Strongly-consistent read
-spec read(Pid :: atom() | pid(), Op :: wa_raft_acceptor:op()) -> ok | wa_raft:error().
read(Pid, Op) ->
    gen_statem:cast(Pid, ?READ_COMMAND(Op)).

-spec status(ServerRef :: gen_statem:server_ref()) -> status().
status(ServerRef) ->
    gen_statem:call(ServerRef, ?STATUS_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

-spec status
    (ServerRef :: gen_statem:server_ref(), Key :: atom()) -> Value :: dynamic();
    (ServerRef :: gen_statem:server_ref(), Keys :: [atom()]) -> Value :: [dynamic()].
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

-spec membership(Service :: pid() | atom() | {atom(), node()}) -> undefined | [#raft_identity{}].
membership(Service) ->
    case proplists:get_value(config, status(Service), undefined) of
        undefined -> undefined;
        Config    -> get_config_members(Config)
    end.

-spec stop(Pid :: atom() | pid()) -> ok.
stop(Pid) ->
    gen_statem:stop(Pid).

% An API that uses storage timeout since it interacts with storage layer directly
-spec snapshot_available(Pid :: atom() | pid() | {atom(), atom()}, Root :: string(), Pos :: wa_raft_log:log_pos()) -> ok | wa_raft:error().
snapshot_available(Pid, Root, Pos) ->
    gen_statem:call(Pid, ?SNAPSHOT_AVAILABLE_COMMAND(Root, Pos), ?RAFT_STORAGE_CALL_TIMEOUT()).

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
    gen_statem:call(Pid, ?PROMOTE_COMMAND(Term, Force, Config), ?RAFT_RPC_CALL_TIMEOUT()).

-spec resign(Pid :: atom() | pid()) -> ok | wa_raft:error().
resign(Pid) ->
    gen_statem:call(Pid, ?RESIGN_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

-spec refresh_config(Name :: atom() | pid()) -> {ok, Pos :: wa_raft_log:log_pos()} | wa_raft:error().
refresh_config(Name) ->
    gen_statem:call(Name, ?ADJUST_MEMBERSHIP_COMMAND(refresh, undefined, undefined), ?RAFT_RPC_CALL_TIMEOUT()).

-spec adjust_membership(
    Name :: atom() | pid(),
    Action :: add | remove | add_witness | remove_witness,
    Peer :: peer()
) ->
    {ok, Pos :: wa_raft_log:log_pos()} | wa_raft:error().
adjust_membership(Name, Action, Peer) ->
    adjust_membership(Name, Action, Peer, undefined).

-spec adjust_membership(
    Name :: atom() | pid(),
    Action :: add | remove | add_witness | remove_witness,
    Peer :: peer(),
    ConfigIndex :: wa_raft_log:log_index() | undefined
) -> {ok, Pos :: wa_raft_log:log_pos()} | wa_raft:error().
adjust_membership(Name, Action, Peer, ConfigIndex) ->
    gen_statem:call(Name, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer, ConfigIndex), ?RAFT_RPC_CALL_TIMEOUT()).

-spec handover_candidates(Name :: atom() | pid()) -> {ok, Candidates :: [node()]} | wa_raft:error().
handover_candidates(Name) ->
    gen_statem:call(Name, ?HANDOVER_CANDIDATES_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

%% Instruct a RAFT leader to attempt a handover to a random handover candidate.
-spec handover(Name :: atom() | pid()) -> ok.
handover(Name) ->
    gen_statem:cast(Name, ?HANDOVER_COMMAND(undefined)).

%% Instruct a RAFT leader to attempt a handover to the specified peer node.
%% If an `undefined` peer node is specified, then handover to a random handover candidate.
%% Returns which peer node the handover was sent to or otherwise an error.
-spec handover(Name :: atom() | pid(), Peer :: node() | undefined) -> {ok, Peer :: node()} | wa_raft:error().
handover(Name, Peer) ->
    gen_statem:call(Name, ?HANDOVER_COMMAND(Peer), ?RAFT_RPC_CALL_TIMEOUT()).

-spec disable(Name :: atom() | pid(), Reason :: term()) -> ok | {error, ErrorReason :: atom()}.
disable(Name, Reason) ->
    gen_statem:cast(Name, ?DISABLE_COMMAND(Reason)).

-spec enable(Name :: atom() | pid()) -> ok | {error, ErrorReason :: atom()}.
enable(Name) ->
    gen_statem:call(Name, ?ENABLE_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT server associated with the provided
%% RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_server_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT server associated with the provided
%% RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.server_name
    end.

-spec make_rpc(Self :: #raft_identity{}, Term :: wa_raft_log:log_term(), Procedure :: normalized_procedure()) -> rpc().
make_rpc(#raft_identity{name = Name, node = Node}, Term, ?PROCEDURE(Procedure, Payload)) ->
    % For compatibility with legacy versions that expect RPCs sent with no arguments to have payload 'undefined' instead of {}.
    PayloadOrUndefined = case Payload of
        {} -> undefined;
        _  -> Payload
    end,
    ?RAFT_NAMED_RPC(Procedure, Term, Name, Node, PayloadOrUndefined).

-spec parse_rpc(Self :: #raft_identity{}, RPC :: rpc()) -> {Term :: wa_raft_log:log_term(), Sender :: #raft_identity{}, Procedure :: procedure()}.
parse_rpc(_Self, ?RAFT_NAMED_RPC(Key, Term, SenderName, SenderNode, PayloadOrUndefined)) ->
    Payload = case PayloadOrUndefined of
        undefined -> {};
        _         -> PayloadOrUndefined
    end,
    #{Key := ?PROCEDURE(Procedure, Defaults)} = protocol(),
    {Term, #raft_identity{name = SenderName, node = SenderNode}, ?PROCEDURE(Procedure, defaultize_payload(Defaults, Payload))};
parse_rpc(#raft_identity{name = Name} = Self, ?LEGACY_RAFT_RPC(Procedure, Term, SenderId, Payload)) ->
    parse_rpc(Self, ?RAFT_NAMED_RPC(Procedure, Term, Name, SenderId, Payload)).

%% ==================================================
%%  gen_statem Callbacks
%% ==================================================

%% gen_statem callbacks
-spec init(Options :: #raft_options{}) -> gen_statem:init_result(state()).
init(#raft_options{application = Application, table = Table, partition = Partition, witness = Witness, self = Self, identifier = Identifier, database = DataDir,
                   distribution_module = DistributionModule, log_name = Log, log_catchup_name = Catchup, server_name = Name, storage_name = Storage} = Options) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Server[~0p] starting with options ~0p", [Name, Options], #{domain => [whatsapp, wa_raft]}),

    % Open storage and the log
    {ok, Last} = wa_raft_storage:open(Storage),
    {ok, View} = wa_raft_log:open(Log, Last),

    Now = erlang:monotonic_time(millisecond),
    State0 = #raft_state{
        application = Application,
        name = Name,
        self = Self,
        identifier = Identifier,
        table = Table,
        partition = Partition,
        data_dir = DataDir,
        log_view = View,
        distribution_module = DistributionModule,
        storage = Storage,
        catchup = Catchup,
        commit_index = Last#raft_log_pos.index,
        last_applied = Last#raft_log_pos.index,
        current_term = Last#raft_log_pos.term,
        state_start_ts = Now,
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
    true = wa_raft_info:set_current_term_and_leader(Table, Partition, State2#raft_state.current_term, undefined),
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

%% A macro that destructures the identity record indicating that the
%% relevant procedure should be refactored to treat identities
%% opaquely.
-define(IDENTITY_REQUIRES_MIGRATION(Name, Node), #raft_identity{name = Name, node = Node}).

-type remote(Call) :: ?REMOTE(#raft_identity{}, Call).
-type procedure()  :: ?PROCEDURE(atom(), tuple()).

-type normalized_procedure() :: append_entries() | append_entries_response() | request_vote() | vote() | handover() | handover_failed() | notify_term().
-type append_entries()          :: ?APPEND_ENTRIES         (wa_raft_log:log_index(), wa_raft_log:log_term(), [wa_raft_log:log_entry()], wa_raft_log:log_index(), wa_raft_log:log_index()).
-type append_entries_response() :: ?APPEND_ENTRIES_RESPONSE(wa_raft_log:log_index(), boolean(), wa_raft_log:log_index()).
-type request_vote()            :: ?REQUEST_VOTE           (election_type(), wa_raft_log:log_index(), wa_raft_log:log_term()).
-type vote()                    :: ?VOTE                   (boolean()).
-type handover()                :: ?HANDOVER               (reference(), wa_raft_log:log_index(), wa_raft_log:log_term(), [wa_raft_log:log_entry()]).
-type handover_failed()         :: ?HANDOVER_FAILED        (reference()).
-type notify_term()             :: ?NOTIFY_TERM            ().

-type election_type() :: normal | force | allowed.

-spec protocol() -> #{atom() => procedure()}.
protocol() ->
    #{
        ?APPEND_ENTRIES          => ?APPEND_ENTRIES(0, 0, [], 0, 0),
        ?APPEND_ENTRIES_RESPONSE => ?APPEND_ENTRIES_RESPONSE(0, false, 0),
        ?REQUEST_VOTE            => ?REQUEST_VOTE(normal, 0, 0),
        ?VOTE                    => ?VOTE(false),
        ?HANDOVER                => ?HANDOVER(undefined, 0, 0, []),
        ?HANDOVER_FAILED         => ?HANDOVER_FAILED(undefined)
    }.

-spec handle_rpc(Type :: gen_statem:event_type(), RPC :: rpc(), State :: state(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
handle_rpc(Type, ?RAFT_NAMED_RPC(Procedure, Term, SenderName, SenderNode, Payload) = Event, State, Data) ->
    handle_rpc_impl(Type, Event, Procedure, Term, #raft_identity{name = SenderName, node = SenderNode}, Payload, State, Data);
handle_rpc(Type, ?LEGACY_RAFT_RPC(Procedure, Term, SenderId, Payload) = Event, State, #raft_state{name = Name} = Data) ->
    handle_rpc_impl(Type, Event, Procedure, Term, #raft_identity{name = Name, node = SenderId}, Payload, State, Data);
handle_rpc(_Type, RPC, State, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?RAFT_COUNT({'raft', State, 'rpc.unrecognized'}),
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] receives unknown RPC format ~P",
        [Name, CurrentTerm, State, RPC, 25], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

-spec handle_rpc_impl(Type :: gen_statem:event_type(), Event :: rpc(), Key :: atom(), Term :: wa_raft_log:log_term(), Sender :: #raft_identity{},
                      Payload :: undefined | tuple(), State :: state(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Protocol] Undefined payload should be treated as an empty tuple
handle_rpc_impl(Type, Event, Key, Term, Sender, undefined, State, Data) ->
    handle_rpc_impl(Type, Event, Key, Term, Sender, {}, State, Data);
%% [General Rules] Discard any incoming RPCs with a term older than the current term
handle_rpc_impl(_Type, _Event, Key, Term, Sender, _Payload, State, #raft_state{name = Name, current_term = CurrentTerm} = Data) when Term < CurrentTerm ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] received stale ~0p from ~0p with old term ~0p. Dropping.",
        [Name, CurrentTerm, State, Key, Sender, Term], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?NOTIFY_TERM(), Data),
    keep_state_and_data;
%% [RequestVote RPC]
%% RAFT servers should completely ignore normal RequestVote RPCs that it receives
%% from any peer if it knows about a currently active leader. An active leader is
%% one that is replicating to a quorum so we can check if we have gotten a
%% heartbeat recently.
handle_rpc_impl(Type, Event, ?REQUEST_VOTE, Term, Sender, Payload, State,
                #raft_state{application = App, name = Name, current_term = CurrentTerm, leader_heartbeat_ts = LeaderHeartbeatTs} = Data) when is_tuple(Payload), element(1, Payload) =:= normal ->
    AllowedDelay = ?RAFT_ELECTION_TIMEOUT_MIN(App) div 2,
    Delay = case LeaderHeartbeatTs of
        undefined -> infinity;
        _         -> erlang:monotonic_time(millisecond) - LeaderHeartbeatTs
    end,
    case Delay > AllowedDelay of
        true ->
            % We have not gotten a heartbeat from the leader recently so allow this vote request
            % to go through by reraising it with the special 'allowed' election type.
            handle_rpc_impl(Type, Event, ?REQUEST_VOTE, Term, Sender, setelement(1, Payload, allowed), State, Data);
        false ->
            % We have gotten a heartbeat recently so drop this vote request.
            % Log this at debug level because we may end up with alot of these when we have
            % removed a server from the cluster but not yet shut it down.
            ?RAFT_COUNT('raft.server.request_vote.drop'),
            ?LOG_DEBUG("Server[~0p, term ~0p, ~0p] dropping normal vote request from ~p because leader was still active ~p ms ago (allowed ~p ms).",
                [Name, CurrentTerm, State, Sender, Delay, AllowedDelay], #{domain => [whatsapp, wa_raft]}),
            keep_state_and_data
    end;
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
handle_rpc_impl(Type, Event, _Key, Term, _Sender, _Payload, _State, #raft_state{current_term = CurrentTerm}) when Term > CurrentTerm ->
    {keep_state_and_data, [{next_event, internal, ?ADVANCE_TERM(Term)}, {next_event, Type, Event}]};
%% [NotifyTerm] Drop NotifyTerm RPCs with matching term
handle_rpc_impl(_Type, _Event, ?NOTIFY_TERM, _Term, _Sender, _Payload, _State, _Data) ->
    keep_state_and_data;
%% [Protocol] Convert any valid remote procedure call to the appropriate local procedure call.
handle_rpc_impl(Type, _Event, Key, _Term, Sender, Payload, State, #raft_state{name = Name, current_term = CurrentTerm} = Data) when is_tuple(Payload) ->
    case protocol() of
        #{Key := ?PROCEDURE(Procedure, Defaults)} ->
            handle_procedure(Type, ?REMOTE(Sender, ?PROCEDURE(Procedure, defaultize_payload(Defaults, Payload))), State, Data);
        #{} ->
            ?RAFT_COUNT({'raft', State, 'rpc.unknown'}),
            ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] receives unknown RPC type ~0p with payload ~0P",
                [Name, CurrentTerm, State, Key, Payload, 25], #{domain => [whatsapp, wa_raft]}),
            keep_state_and_data
    end.

-spec handle_procedure(Type :: gen_statem:event_type(), ProcedureCall :: remote(procedure()), State :: state(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [AppendEntries] If we haven't discovered leader for this term, record it
handle_procedure(Type, ?REMOTE(Sender, ?APPEND_ENTRIES(_, _, _, _, _)) = Procedure, State, #raft_state{leader_id = undefined} = Data) ->
    {keep_state, set_leader(State, Sender, Data), {next_event, Type, Procedure}};
%% [Handover] If we haven't discovered leader for this term, record it
handle_procedure(Type, ?REMOTE(Sender, ?HANDOVER(_, _, _, _)) = Procedure, State, #raft_state{leader_id = undefined} = Data) ->
    {keep_state, set_leader(State, Sender, Data), {next_event, Type, Procedure}};
handle_procedure(Type, Procedure, _State, _Data) ->
    {keep_state_and_data, {next_event, Type, Procedure}}.

-spec defaultize_payload(tuple(), tuple()) -> tuple().
defaultize_payload(Defaults, Payload) ->
    defaultize_payload(Defaults, Payload, tuple_size(Defaults), tuple_size(Payload)).

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
-spec stalled(Type :: enter, PreviousStateName :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
             (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
stalled(enter, PreviousStateName, #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?RAFT_COUNT('raft.stalled.enter'),
    ?LOG_NOTICE("Server[~0p, term ~0p, stalled] becomes stalled from state ~0p.",
        [Name, CurrentTerm, PreviousStateName], #{domain => [whatsapp, wa_raft]}),
    {keep_state, enter_state(?FUNCTION_NAME, State)};

%% [AdvanceTerm] Advance to newer term when requested
stalled(internal, ?ADVANCE_TERM(NewerTerm), #raft_state{name = Name, current_term = CurrentTerm} = State) when NewerTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.stalled.advance_term'),
    ?LOG_NOTICE("Server[~0p, term ~0p, stalled] advancing to new term ~p.",
        [Name, CurrentTerm, NewerTerm], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, advance_term(?FUNCTION_NAME, NewerTerm, undefined, State)};

%% [AdvanceTerm] Ignore attempts to advance to an older or current term
stalled(internal, ?ADVANCE_TERM(Term), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, stalled] ignoring attempt to advance to older or current term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Protocol] Handle any RPCs
stalled(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    % eqwalizer:fixme T169659719
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries] Stalled nodes always discard AppendEntries
stalled(_Type, ?REMOTE(Sender, ?APPEND_ENTRIES(PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex)), State) ->
    NewState = State#raft_state{leader_heartbeat_ts = erlang:monotonic_time(millisecond)},
    send_rpc(Sender, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, false, 0), NewState),
    {keep_state, NewState};

stalled({call, From}, ?SNAPSHOT_AVAILABLE_COMMAND(Root, #raft_log_pos{index = SnapshotIndex, term = SnapshotTerm} = SnapshotPos),
        #raft_state{name = Name, data_dir = DataDir, log_view = View0, storage = Storage,
                    current_term = CurrentTerm, last_applied = LastApplied} = State0) ->
    case SnapshotIndex > LastApplied orelse LastApplied =:= 0 of
        true ->
            Path = filename:join(DataDir, ?SNAPSHOT_NAME(SnapshotIndex, SnapshotTerm)),
            catch filelib:ensure_dir(Path),
            case prim_file:rename(Root, Path) of
                ok ->
                    ?LOG_NOTICE("Server[~0p, term ~0p, stalled] applying snapshot ~p:~p",
                        [Name, CurrentTerm, SnapshotIndex, SnapshotTerm], #{domain => [whatsapp, wa_raft]}),
                    ok = wa_raft_storage:open_snapshot(Storage, SnapshotPos),
                    {ok, View1} = wa_raft_log:reset(View0, SnapshotPos),
                    State1 = State0#raft_state{log_view = View1, last_applied = SnapshotIndex, commit_index = SnapshotIndex},
                    State2 = load_config(State1),
                    ?LOG_NOTICE("Server[~0p, term ~0p, stalled] switching to follower after installing snapshot at ~p:~p.",
                        [Name, CurrentTerm, SnapshotIndex, SnapshotTerm], #{domain => [whatsapp, wa_raft]}),
                    % At this point, we assume that we received some cluster membership configuration from
                    % our peer so it is safe to transition to an operational state.
                    {next_state, follower, State2, {reply, From, ok}};
                {error, Reason} ->
                    ?LOG_WARNING("Server[~0p, term ~0p, stalled] failed to rename available snapshot ~p to ~p due to ~p",
                        [Name, CurrentTerm, Root, Path, Reason], #{domain => [whatsapp, wa_raft]}),
                    {keep_state_and_data, {reply, From, {error, Reason}}}
            end;
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, stalled] ignoring available snapshot ~p:~p with index not past ours (~p)",
                [Name, CurrentTerm, SnapshotIndex, SnapshotTerm, LastApplied], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;

stalled(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

stalled(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, stalled] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

%% [RAFT Core]
%% Leader - In the RAFT cluster, the leader node is a node agreed upon by the cluster to be
%%          responsible for accepting and replicating commits and maintaining the integrity
%%          of the underlying state machine.
-spec leader(Type :: enter, PreviousStateName :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
            (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Leader] changing to leader
leader(enter, PreviousStateName, #raft_state{name = Name, self = Self, current_term = CurrentTerm, log_view = View0} = State0) ->
    ?RAFT_COUNT('raft.leader.enter'),
    ?RAFT_COUNT('raft.leader.elected'),
    ?LOG_NOTICE("Server[~0p, term ~0p, leader] becomes leader from state ~0p.",
        [Name, CurrentTerm, PreviousStateName], #{domain => [whatsapp, wa_raft]}),

    % Setup leader state and announce leadership
    State1 = enter_state(?FUNCTION_NAME, State0),
    State2 = set_leader(?FUNCTION_NAME, Self, State1),

    % During promotion, we may add a config update operation to the end of the log.
    % If there is such an operation and it has the same term number as the current term,
    % then make sure to set the start of the current term so that it gets replicated
    % immediately.
    LastLogIndex = wa_raft_log:last_index(View0),
    State3 = case wa_raft_log:get(View0, LastLogIndex) of
        {ok, {CurrentTerm, {_Ref, {config, _NewConfig}}}} ->
            State2#raft_state{first_current_term_log_index = LastLogIndex};
        {ok, _} ->
            % Otherwise, the server should add a new log entry to start the current term.
            % This will flush pending commits, clear out and log mismatches on replicas,
            % and establish a quorum as soon as possible.
            {ok, NewLastLogIndex, View1} = wa_raft_log:append(View0, [{CurrentTerm, {make_ref(), noop}}]),
            State2#raft_state{log_view = View1, first_current_term_log_index = NewLastLogIndex}
    end,

    % Perform initial heartbeat and log entry resolution
    State4 = append_entries_to_followers(State3),
    State5 = apply_single_node_cluster(State4), % apply immediately for single node cluster
    {keep_state, State5, ?HEARTBEAT_TIMEOUT(State5)};

%% [AdvanceTerm] Advance to newer term when requested
leader(internal, ?ADVANCE_TERM(NewerTerm), #raft_state{name = Name, log_view = View, current_term = CurrentTerm} = State) when NewerTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.leader.advance_term'),
    ?LOG_NOTICE("Server[~0p, term ~0p, leader] advancing to new term ~p.",
        [Name, CurrentTerm, NewerTerm], #{domain => [whatsapp, wa_raft]}),
    %% Drop any pending log entries queued in the log view before advancing
    {ok, _Pending, NewView} = wa_raft_log:cancel(View),
    {next_state, follower, advance_term(?FUNCTION_NAME, NewerTerm, undefined, State#raft_state{log_view = NewView})};

%% [AdvanceTerm] Ignore attempts to advance to an older or current term
leader(internal, ?ADVANCE_TERM(Term), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, leader] ignoring attempt to advance to older or current term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Protocol] Handle any RPCs
leader(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    % eqwalizer:fixme T169659719
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [Leader] Handle AppendEntries RPC (5.1, 5.2)
%% [AppendEntries] We are leader for the current term, so we should never see an
%%                 AppendEntries RPC from another node for this term
leader(_Type, ?REMOTE(Sender, ?APPEND_ENTRIES(PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex)),
       #raft_state{current_term = CurrentTerm, name = Name, commit_index = CommitIndex} = State) ->
    ?LOG_ERROR("Server[~0p, term ~0p, leader] got invalid heartbeat from conflicting leader ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, false, CommitIndex), State),
    keep_state_and_data;

%% [Leader] Handle AppendEntries RPC responses (5.2, 5.3, 7).
%% Handle normal-case successes
leader(cast, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, FollowerId) = Sender, ?APPEND_ENTRIES_RESPONSE(_PrevIndex, true, FollowerEndIndex)),
    #raft_state{
        name = Name,
        current_term = CurrentTerm,
        commit_index = CommitIndex0,
        next_index = NextIndex0,
        match_index = MatchIndex0,
        heartbeat_response_ts = HeartbeatResponse0,
        first_current_term_log_index = TermStartIndex} = State0) ->
    StartT = os:timestamp(),
    ?LOG_DEBUG("Server[~0p, term ~0p, leader] append ok on ~p. Follower end index ~p. Leader commitIndex ~p",
        [Name, CurrentTerm, Sender, FollowerEndIndex, CommitIndex0], #{domain => [whatsapp, wa_raft]}),
    HeartbeatResponse1 = HeartbeatResponse0#{FollowerId => erlang:monotonic_time(millisecond)},
    State1 = State0#raft_state{heartbeat_response_ts = HeartbeatResponse1},

    case select_follower_replication_mode(FollowerEndIndex, State1) of
        bulk_logs -> request_bulk_logs_for_follower(Sender, FollowerEndIndex, State1);
        _         -> cancel_bulk_logs_for_follower(Sender, State1)
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
    {keep_state, maybe_heartbeat(State3), ?HEARTBEAT_TIMEOUT(State3)};

%% and failures.
leader(cast, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, FollowerId) = Sender, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, false, FollowerEndIndex)),
       #raft_state{name = Name, current_term = CurrentTerm, next_index = NextIndex0, match_index = MatchIndex0} = State0) ->
    ?RAFT_COUNT('raft.leader.append.failure'),
    ?LOG_DEBUG("Server[~0p, term ~0p, leader] append failure for follower ~p. Follower reports local log ends at ~0p.",
        [Name, CurrentTerm, Sender, FollowerEndIndex], #{domain => [whatsapp, wa_raft]}),

    select_follower_replication_mode(FollowerEndIndex, State0) =:= snapshot andalso
        request_snapshot_for_follower(FollowerId, State0),
    cancel_bulk_logs_for_follower(Sender, State0),

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
    {keep_state, maybe_heartbeat(State2), ?HEARTBEAT_TIMEOUT(State2)};

%% [RequestVote RPC] We are already leader for the current term, so always decline votes (5.1, 5.2)
leader(_Type, ?REMOTE(Sender, ?REQUEST_VOTE(_ElectionType, _LastLogIndex, _LastLogTerm)),
       #raft_state{} = State) ->
    send_rpc(Sender, ?VOTE(false), State),
    keep_state_and_data;

%% [Vote RPC] We are already leader, so we don't need to consider any more votes (5.1)
leader(_Type, ?REMOTE(Sender, ?VOTE(Vote)), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, leader] receives vote ~p from ~p after being elected. Ignore it.",
        [Name, CurrentTerm, Vote, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Handover RPC] We are already leader so ignore any handover requests.
leader(_Type, ?REMOTE(Sender, ?HANDOVER(Ref, _PrevLogIndex, _PrevLogTerm, _LogEntries)),
       #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_WARNING("Server[~0p, term ~0p, leader] got orphan handover request from ~p while leader.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?HANDOVER_FAILED(Ref), State),
    keep_state_and_data;

%% [Handover Failed RPC] Our handover failed, so clear the handover status.
leader(_Type, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, NodeId) = Sender, ?HANDOVER_FAILED(Ref)),
       #raft_state{name = Name, current_term = CurrentTerm, handover = {NodeId, Ref, _Timeout}} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, leader] resuming normal operations after failed handover to ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    {keep_state, State#raft_state{handover = undefined}};

%% [Handover Failed RPC] Got a handover failed with an unknown ID. Ignore.
leader(_Type, ?REMOTE(Sender, ?HANDOVER_FAILED(_Ref)),
       #raft_state{name = Name, current_term = CurrentTerm, handover = Handover}) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, leader] got handover failed RPC from ~p that does not match current handover ~p.",
        [Name, CurrentTerm, Sender, Handover], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Timeout] Suspend periodic heartbeat to followers while handover is active
leader(state_timeout = Type, Event, #raft_state{name = Name, current_term = CurrentTerm, handover = {Peer, _Ref, Timeout}} = State) ->
    NowMillis = erlang:monotonic_time(millisecond),
    case NowMillis > Timeout of
        true ->
            ?LOG_NOTICE("Server[~0p, term ~0p, leader] handover to ~p times out",
                [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
            {keep_state, State#raft_state{handover = undefined}, {next_event, Type, Event}};
        false ->
            {keep_state_and_data, ?HEARTBEAT_TIMEOUT(State)}
    end;

%% [Timeout] Periodic heartbeat to followers
leader(state_timeout, _, #raft_state{application = App, name = Name, log_view = View, current_term = CurrentTerm} = State0) ->
    case ?RAFT_LEADER_ELIGIBLE(App) andalso ?RAFT_ELECTION_WEIGHT(App) =/= 0 of
        true ->
            State1 = append_entries_to_followers(State0),
            State2 = apply_single_node_cluster(State1),
            check_leader_lagging(State2),
            {keep_state, State1, ?HEARTBEAT_TIMEOUT(State2)};
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, leader] resigns from leadership because this node is ineligible or election weight is zero.",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            %% Drop any pending log entries queued in the log view before resigning
            {ok, _Pending, NewView} = wa_raft_log:cancel(View),
            {next_state, follower, clear_leader(?FUNCTION_NAME, State0#raft_state{log_view = NewView})}
    end;

%% [Commit] If a handover is in progress, then try to redirect to handover target
leader(cast, ?COMMIT_COMMAND({Reference, _Op}), #raft_state{table = Table, partition = Partition, handover = {Peer, _, _}} = State) ->
    ?RAFT_COUNT('raft.commit.handover'),
    wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Reference, {error, {notify_redirect, Peer}}), % Optimistically redirect to handover peer
    {keep_state, State};
%% [Commit] Otherwise, add a new commit to the RAFT log
leader(cast, ?COMMIT_COMMAND(Op), #raft_state{application = App, current_term = CurrentTerm, log_view = View0, next_index = NextIndex} = State0) ->
    ?RAFT_COUNT('raft.commit'),
    {ok, View1} = wa_raft_log:submit(View0, {CurrentTerm, Op}),
    ExpectedLastIndex = wa_raft_log:last_index(View1) + wa_raft_log:pending(View1),
    State1 = apply_single_node_cluster(State0#raft_state{log_view = View1}), % apply immediately for single node cluster

    MaxNexIndex = maps:fold(fun(_NodeId, V, Acc) -> erlang:max(Acc, V) end, 0, NextIndex),
    case ?RAFT_COMMIT_BATCH_INTERVAL(App) > 0 andalso ExpectedLastIndex - MaxNexIndex < ?RAFT_COMMIT_BATCH_MAX_ENTRIES(App) of
        true ->
            ?RAFT_COUNT('raft.commit.batch.delay'),
            {keep_state, State1, ?COMMIT_BATCH_TIMEOUT(State1)};
        false ->
            State2 = append_entries_to_followers(State1),
            {keep_state, State2, ?HEARTBEAT_TIMEOUT(State2)}
    end;

%% [Strong Read] If a handover is in progress, then try to redirect to handover target
leader(cast, ?READ_COMMAND({From, _Command}), #raft_state{table = Table, partition = Partition, handover = {Peer, _Ref, _Timeout}} = State) ->
    ?RAFT_COUNT('raft.read.handover'),
    wa_raft_queue:fulfill_incomplete_read(Table, Partition, From, {error, {notify_redirect, Peer}}), % Optimistically redirect to handover peer
    {keep_state, State};
%% [Strong Read] Leader is eligible to serve strong reads.
leader(cast, ?READ_COMMAND({From, Command}),
       #raft_state{name = Name, table = Table, partition = Partition, log_view = View0, storage = Storage,
                   current_term = CurrentTerm, commit_index = CommitIndex, last_applied = LastApplied, first_current_term_log_index = FirstLogIndex} = State0) ->
    ?LOG_DEBUG("Server[~0p, term ~0p, leader] receives strong read request", [Name, CurrentTerm]),
    LastLogIndex = wa_raft_log:last_index(View0),
    Pending = wa_raft_log:pending(View0),
    ReadIndex = max(CommitIndex, FirstLogIndex),
    case config_membership(config(State0)) of
        % If we are a single node cluster and we are fully-applied, then immediately dispatch.
        [{Name, Node}] when Node =:= node(), Pending =:= 0, ReadIndex =:= LastApplied ->
            wa_raft_storage:read(Storage, From, Command),
            {keep_state, State0};
        _ ->
            ok = wa_raft_queue:submit_read(Table, Partition, ReadIndex + 1, From, Command),
            {ok, View1} = case ReadIndex < LastLogIndex orelse Pending > 0 of
                true  -> {ok, View0};
                % TODO(hsun324): Try to reuse the commit code to deal with placeholder ops so we
                % handle batching and timeout properly instead of relying on a previously set timeout.
                false -> wa_raft_log:submit(View0, {CurrentTerm, {?READ_OP, noop}})
            end,
            {keep_state, State0#raft_state{log_view = View1}}
    end;

%% [Resign] Leader resigns by switching to follower state.
leader({call, From}, ?RESIGN_COMMAND, #raft_state{name = Name, log_view = View, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, leader] resigns.", [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),

    %% Drop any pending log entries queued in the log view before resigning
    {ok, _Pending, NewView} = wa_raft_log:cancel(View),
    {next_state, follower, clear_leader(?FUNCTION_NAME, State#raft_state{log_view = NewView}), {reply, From, ok}};

%% [Adjust Membership] Leader attempts to commit a single-node membership change.
leader(Type, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer, ExpectedConfigIndex),
       #raft_state{name = Name, log_view = View0, storage = Storage,
                   current_term = CurrentTerm, last_applied = LastApplied} = State0) ->
    % Try to adjust the configuration according to the current request.
    Config = config(State0),
    % eqwalizer:ignore Peer can be undefined
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
                    ConfigIndex = max(StorageConfigIndex, LogConfigIndex),
                    case LogConfigIndex > StorageConfigIndex of
                        true ->
                            ?LOG_NOTICE("Server[~0p, term ~0p, leader] rejecting request to ~p peer ~p because it has a pending reconfiguration (storage: ~p, log: ~p).",
                                [Name, CurrentTerm, Action, Peer, StorageConfigIndex, LogConfigIndex], #{domain => [whatsapp, wa_raft]}),
                            reply(Type, {error, rejected}),
                            {keep_state, State0};
                        false ->
                            case ExpectedConfigIndex =/= undefined andalso ExpectedConfigIndex =/= ConfigIndex of
                                true ->
                                    ?LOG_NOTICE("Server[~0p, term ~0p, leader] rejecting request to ~p peer ~p because it has a different config index than expected (storage: ~p, log: ~p expected: ~p).",
                                        [Name, CurrentTerm, Action, Peer, StorageConfigIndex, LogConfigIndex, ExpectedConfigIndex], #{domain => [whatsapp, wa_raft]}),
                                    reply(Type, {error, rejected}),
                                    {keep_state, State0};
                                false ->
                                    % Now that we have completed all the required checks, the leader is free to
                                    % attempt to change the config. We heartbeat immediately to make the change as
                                    % soon as possible.
                                    Op = {make_ref(), {config, NewConfig}},
                                    {ok, LogIndex, View1} = wa_raft_log:append(View0, [{CurrentTerm, Op}]),
                                    ?LOG_NOTICE("Server[~0p, term ~0p, leader] appended configuration change from ~0p to ~0p at log index ~p.",
                                        [Name, CurrentTerm, Config, NewConfig, LogIndex], #{domain => [whatsapp, wa_raft]}),
                                    State1 = State0#raft_state{log_view = View1},
                                    State2 = apply_single_node_cluster(State1),
                                    State3 = append_entries_to_followers(State2),
                                    reply(Type, {ok, #raft_log_pos{index = LogIndex, term = CurrentTerm}}),
                                    {keep_state, State3, ?HEARTBEAT_TIMEOUT(State3)}
                            end
                    end;
                {ok, _OtherTerm} ->
                    ?LOG_NOTICE("Server[~0p, term ~0p, leader] rejecting request to ~p peer ~p because it has not established current term commit quorum.",
                        [Name, CurrentTerm, Action, Peer], #{domain => [whatsapp, wa_raft]}),
                    reply(Type, {error, rejected}),
                    {keep_state, State0}
            end;
        {error, Reason} ->
            ?LOG_NOTICE("Server[~0p, term ~0p, leader] refusing invalid membership adjustment ~0p on configuration ~0p due to ~0p.",
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
            ?LOG_NOTICE("Server[~0p, term ~0p, leader] has no valid peer to handover to",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            reply(Type, {error, no_valid_peer}),
            keep_state_and_data;
        Candidates ->
            Peer = lists:nth(rand:uniform(length(Candidates)), Candidates),
            leader(Type, ?HANDOVER_COMMAND(Peer), State)
    end;

%% [Handover] Do not allow handover to self
leader(Type, ?HANDOVER_COMMAND(Peer), #raft_state{name = Name, current_term = CurrentTerm} = State) when Peer =:= node() ->
    ?LOG_WARNING("Server[~0p, term ~0p, leader] dropping handover to self.",
        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, badarg}),
    {keep_state, State};

%% [Handover] Attempt to start a handover to the specified peer
leader(Type, ?HANDOVER_COMMAND(Peer),
       #raft_state{application = App, name = Name, log_view = View,
                   current_term = CurrentTerm, handover = undefined} = State0) ->
    % TODO(hsun324): For the time being, assume that all members of the cluster use the same server name.
    case member({Name, Peer}, config(State0)) of
        false ->
            ?LOG_WARNING("Server[~0p, term ~0p, leader] dropping handover to unknown peer ~p.",
                [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
            reply(Type, {error, badarg}),
            keep_state_and_data;
        true ->
            PeerMatchIndex = maps:get(Peer, State0#raft_state.match_index, 0),
            FirstIndex = wa_raft_log:first_index(View),
            PeerSendIndex = max(PeerMatchIndex + 1, FirstIndex + 1),
            LastIndex = wa_raft_log:last_index(View),
            MaxHandoverBatchSize = ?RAFT_HANDOVER_MAX_ENTRIES(App),
            MaxHandoverBytes = ?RAFT_HANDOVER_MAX_BYTES(App),

            case LastIndex - PeerSendIndex =< MaxHandoverBatchSize of
                true ->
                    ?RAFT_COUNT('raft.leader.handover'),
                    ?LOG_NOTICE("Server[~0p, term ~0p, leader] starting handover to ~p.",
                        [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
                    Ref = make_ref(),
                    Timeout = erlang:monotonic_time(millisecond) + ?RAFT_HANDOVER_TIMEOUT(App),
                    State1 = State0#raft_state{handover = {Peer, Ref, Timeout}},

                    PrevLogIndex = PeerSendIndex - 1,
                    {ok, PrevLogTerm} = wa_raft_log:term(View, PrevLogIndex),
                    {ok, LogEntries} = wa_raft_log:get(View, PeerSendIndex, MaxHandoverBatchSize, MaxHandoverBytes),

                    % The request to load the log may result in not all required log entries being loaded
                    % if we hit the byte size limit. Ensure that we have loaded all required log entries
                    % before initiating a handover.
                    case PrevLogIndex + length(LogEntries) of
                        LastIndex ->
                            send_rpc(?IDENTITY_REQUIRES_MIGRATION(Name, Peer), ?HANDOVER(Ref, PrevLogIndex, PrevLogTerm, LogEntries), State1),
                            reply(Type, {ok, Peer}),
                            {keep_state, State1};
                        _ ->
                            ?RAFT_COUNT('raft.leader.handover.oversize'),
                            ?LOG_WARNING("Server[~0p, term ~0p, leader] handover to peer ~p would require an oversized RPC.",
                                [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
                            reply(Type, {error, oversize}),
                            keep_state_and_data
                    end;
                false ->
                    ?RAFT_COUNT('raft.leader.handover.peer_lagging'),
                    ?LOG_WARNING("Server[~0p, term ~0p, leader] determines that peer ~p is not eligible for handover because it is ~p entries behind.",
                        [Name, CurrentTerm, Peer, LastIndex - PeerSendIndex], #{domain => [whatsapp, wa_raft]}),
                    reply(Type, {error, peer_lagging}),
                    keep_state_and_data
            end
    end;

%% [Handover] Reject starting a handover when a handover is still in progress
leader({call, From}, ?HANDOVER_COMMAND(Peer), #raft_state{name = Name, current_term = CurrentTerm, handover = {Node, _Ref, _Timeout}}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, leader] rejecting duplicate handover to ~p with running handover to ~p.",
        [Name, CurrentTerm, Peer, Node], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, duplicate}}};

leader(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% Leader receives unknown event
leader(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, leader] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

-spec follower(Type :: enter, PreviousStateName :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
              (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
follower(enter, PreviousStateName, #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?RAFT_COUNT('raft.follower.enter'),
    ?LOG_NOTICE("Server[~0p, term ~0p, follower] becomes follower from state ~0p.",
        [Name, CurrentTerm, PreviousStateName], #{domain => [whatsapp, wa_raft]}),
    {keep_state, enter_state(?FUNCTION_NAME, State), ?ELECTION_TIMEOUT(State)};

%% [AdvanceTerm] Advance to newer term when requested
follower(internal, ?ADVANCE_TERM(NewerTerm), #raft_state{name = Name, current_term = CurrentTerm} = State) when NewerTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.follower.advance_term'),
    ?LOG_NOTICE("Server[~0p, term ~0p, follower] advancing to new term ~p.",
        [Name, CurrentTerm, NewerTerm], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, advance_term(?FUNCTION_NAME, NewerTerm, undefined, State)};

%% [AdvanceTerm] Ignore attempts to advance to an older or current term
follower(internal, ?ADVANCE_TERM(Term), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, follower] ignoring attempt to advance to older or current term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Protocol] Handle any RPCs
follower(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    % eqwalizer:fixme T169659719
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [Follower] Handle AppendEntries RPC (5.2, 5.3)
%% Follower receives AppendEntries from leader
follower(Type, ?REMOTE(Leader, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex)), State) ->
    handle_heartbeat(?FUNCTION_NAME, Type, Leader, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex, State);

%% [Follower] Handle AppendEntries RPC response (5.2)
%% [AppendEntriesResponse] Followers do not send AppendEntries and so should not get responses
follower(_Type, ?REMOTE(Sender, ?APPEND_ENTRIES_RESPONSE(_PrevLogIndex, _Success, _LastLogIndex)),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, follower] got conflicting response from ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Follower] Handle RequestVote RPCs (5.2)
%% [RequestVote] A follower with an unallocated vote should decide if the requesting candidate is eligible to receive
%%               its vote for the current term and affirm or reject accordingly.
follower(_Type, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, CandidateId) = Candidate, ?REQUEST_VOTE(_ElectionType, CandidateIndex, CandidateTerm)),
         #raft_state{name = Name, log_view = View, current_term = CurrentTerm, voted_for = undefined} = State) ->
    Index = wa_raft_log:last_index(View),
    {ok, Term} = wa_raft_log:term(View, Index),
    % Followers should only vote for candidates whose logs are at least as up-to-date as the local log.
    % Logs are ordered in up-to-dateness by the lexicographic order of the {Term, Index} pair of their latest entry. (5.4.1)
    case {CandidateTerm, CandidateIndex} >= {Term, Index} of
        true ->
            ?LOG_NOTICE("Server[~0p, term ~0p, follower] decides to vote for candidate ~0p with up-to-date log at ~0p:~0p versus local log at ~0p:~0p.",
                [Name, CurrentTerm, Candidate, CandidateIndex, CandidateTerm, Index, Term], #{domain => [whatsapp, wa_raft]}),
            NewState = State#raft_state{voted_for = CandidateId},
            % Persist the vote to stable storage before responding to the vote request. (Fig. 2)
            wa_raft_durable_state:store(NewState),
            send_rpc(Candidate, ?VOTE(true), State),
            {keep_state, NewState};
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, follower] refuses to vote for candidate ~0p with outdated log at ~0p:~0p versus local log at ~0p:~0p.",
                [Name, CurrentTerm, Candidate, CandidateIndex, CandidateTerm, Index, Term], #{domain => [whatsapp, wa_raft]}),
            send_rpc(Candidate, ?VOTE(false), State),
            keep_state_and_data
    end;
%% [RequestVote] A follower should affirm any vote requests for the candidate it already voted for in the current term.
follower(_Type, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, CandidateId) = Candidate, ?REQUEST_VOTE(_ElectionType, _CandidateIndex, _CandidateTerm)),
         #raft_state{name = Name, current_term = CurrentTerm, voted_for = CandidateId} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, follower] repeating prior vote for candidate ~0p.",
        [Name, CurrentTerm, Candidate], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Candidate, ?VOTE(true), State),
    keep_state_and_data;
%% [RequestVote] A follower should reject any vote requests for the candidate it did not vote for in the current term.
follower(_Type, ?REMOTE(Candidate, ?REQUEST_VOTE(_ElectionType, _CandidateIndex, _CandidateTerm)),
         #raft_state{name = Name, current_term = CurrentTerm, voted_for = VotedFor} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, follower] refusing to vote for candidate ~0p after previously voting for candidate ~0p in the current term.",
        [Name, CurrentTerm, Candidate, VotedFor], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Candidate, ?VOTE(false), State),
    keep_state_and_data;

%% [Follower] Handle responses to RequestVote RPCs (5.2)
%% [Vote] A follower should ignore any votes before it never initiated or has already lost the election for the current term.
follower(_Type, ?REMOTE(Sender, ?VOTE(Voted)), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, follower] got extra vote ~p from ~p.",
        [Name, CurrentTerm, Voted, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Handover Extension] Another leader is asking for this follower to take over
follower(_Type, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, LeaderId) = Sender, ?HANDOVER(Ref, PrevLogIndex, PrevLogTerm, LogEntries)),
         #raft_state{application = App, name = Name, current_term = CurrentTerm, leader_id = LeaderId} = State0) ->
    ?RAFT_COUNT('wa_raft.follower.handover'),
    ?LOG_NOTICE("Server[~0p, term ~0p, follower] evaluating handover RPC from ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    case ?RAFT_LEADER_ELIGIBLE(App) andalso ?RAFT_ELECTION_WEIGHT(App) =/= 0 of
        true ->
            case append_entries(?FUNCTION_NAME, PrevLogIndex, PrevLogTerm, LogEntries, length(LogEntries), State0) of
                {ok, true, _NewLastIndex, State1} ->
                    ?LOG_NOTICE("Server[~0p, term ~0p, follower] immediately starting new election due to append success during handover RPC.",
                        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
                    {next_state, candidate, State1, {next_event, internal, ?FORCE_ELECTION(CurrentTerm)}};
                {ok, false, _NewLastIndex, State1} ->
                    ?RAFT_COUNT('raft.follower.handover.rejected'),
                    ?LOG_WARNING("Server[~0p, term ~0p, follower] failing handover request because append was rejected.",
                        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
                    send_rpc(Sender, ?HANDOVER_FAILED(Ref), State1),
                    {keep_state, State1};
                {fatal, Reason} ->
                    ?RAFT_COUNT('raft.follower.handover.fatal'),
                    ?LOG_WARNING("Server[~0p, term ~0p, follower] failing handover request because append was fatal due to ~0P.",
                        [Name, CurrentTerm, Reason, 30], #{domain => [whatsapp, wa_raft]}),
                    send_rpc(Sender, ?HANDOVER_FAILED(Ref), State0),
                    {next_state, disabled, State0#raft_state{disable_reason = Reason}}
            end;
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, follower] not considering handover RPC due to being inelgibile or having zero election weight.",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            send_rpc(Sender, ?HANDOVER_FAILED(Ref), State0),
            {keep_state, State0}
    end;

%% [HandoverFailed Extension] Followers cannot initiate handovers
follower(_Type, ?REMOTE(Sender, ?HANDOVER_FAILED(_Ref)),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, follower] got conflicting handover failed from ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Follower] handle timeout
%% follower doesn't receive any heartbeat. starting a new election
follower(state_timeout, _,
         #raft_state{application = App, name = Name, current_term = CurrentTerm, leader_id = LeaderId,
                     log_view = View, leader_heartbeat_ts = HeartbeatTs} = State) ->
    WaitingMs = case HeartbeatTs of
        undefined -> undefined;
        _         -> erlang:monotonic_time(millisecond) - HeartbeatTs
    end,
    ?LOG_NOTICE("Server[~0p, term ~0p, follower] times out after ~p ms. Last leader ~p. Max log index ~p. Promote to candidate and restart election.",
        [Name, CurrentTerm, WaitingMs, LeaderId, wa_raft_log:last_index(View)], #{domain => [whatsapp, wa_raft]}),
    ?RAFT_COUNT('raft.follower.timeout'),
    case ?RAFT_LEADER_ELIGIBLE(App) andalso ?RAFT_ELECTION_WEIGHT(App) =/= 0 of
        true ->
            {next_state, candidate, State};
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, follower] not advancing to next term after heartbeat timeout due to being ineligible or having zero election weight.",
                [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
            {repeat_state, State}
    end;

follower(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

follower(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, follower] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

-spec candidate(Type :: enter, PreviousStateName :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
               (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Enter] Node starts a new election upon entering the candidate state.
candidate(enter, PreviousStateName, #raft_state{name = Name, self = Self, current_term = CurrentTerm, log_view = View} = State0) ->
    ?RAFT_COUNT('raft.leader.election_started'),
    ?RAFT_COUNT('raft.candidate.enter'),
    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] becomes candidate from state ~0p.",
        [Name, CurrentTerm, PreviousStateName], #{domain => [whatsapp, wa_raft]}),

    % Entering the candidate state means that we are starting a new election, thus
    % advance the term and set VotedFor to the current node. (Candidates always
    % implicitly vote for themselves.)
    State1 = enter_state(?FUNCTION_NAME, State0),
    State2 = advance_term(?FUNCTION_NAME, CurrentTerm + 1, node(), State1),

    % Determine the log index and term at which the election will occur.
    LastLogIndex = wa_raft_log:last_index(View),
    {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),

    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] moves to new term ~p and starts election with last log position ~p:~p.",
        [Name, CurrentTerm, CurrentTerm + 1, LastLogIndex, LastLogTerm], #{domain => [whatsapp, wa_raft]}),

    % Broadcast vote requests and also send a vote-for-self.
    % (Candidates always implicitly vote for themselves.)
    broadcast_rpc(?REQUEST_VOTE(normal, LastLogIndex, LastLogTerm), State2),
    send_rpc(Self, ?VOTE(true), State2),

    {keep_state, State2, ?ELECTION_TIMEOUT(State2)};

%% [AdvanceTerm] Advance to newer term when requested
candidate(internal, ?ADVANCE_TERM(NewerTerm), #raft_state{name = Name, current_term = CurrentTerm} = State) when NewerTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.candidate.advance_term'),
    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] advancing to new term ~p.",
        [Name, CurrentTerm, NewerTerm], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, advance_term(?FUNCTION_NAME, NewerTerm, undefined, State)};

%% [AdvanceTerm] Ignore attempts to advance to an older or current term
candidate(internal, ?ADVANCE_TERM(Term), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, candidate] ignoring attempt to advance to older or current term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [ForceElection] Resend vote requests with the 'force' type to force an election even if an active leader is available.
candidate(internal, ?FORCE_ELECTION(Term), #raft_state{name = Name, log_view = View, current_term = CurrentTerm} = State) when Term + 1 =:= CurrentTerm ->
    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] accepts request to force election issued by the immediately prior term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    % No changes to the log are expected during an election so we can just reload these values from the log view.
    LastLogIndex = wa_raft_log:last_index(View),
    {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),
    broadcast_rpc(?REQUEST_VOTE(force, LastLogIndex, LastLogTerm), State),
    keep_state_and_data;

%% [Protocol] Handle any RPCs
candidate(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    % eqwalizer:fixme T169659719
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Switch to follower because current term now has a leader (5.2, 5.3)
candidate(Type, ?REMOTE(Sender, ?APPEND_ENTRIES(_PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit, _TrimIndex)) = Event,
          #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] gets first heartbeat of the term from ~p. Switch to follower.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, State, {next_event, Type, Event}};

%% [RequestVote RPC] Candidate has always voted for itself, so vote false on anyone else (5.2)
candidate(_Type, ?REMOTE(Sender, ?REQUEST_VOTE(_ElectionType, _LastLogIndex, _LastLogTerm)), State) ->
    send_rpc(Sender, ?VOTE(false), State),
    keep_state_and_data;

%% [Vote RPC] Candidate receives an affirmative vote (5.2)
candidate(cast, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, NodeId), ?VOTE(true)),
          #raft_state{name = Name, log_view = View, current_term = CurrentTerm, state_start_ts = StateStartTs,
                      heartbeat_response_ts = HeartbeatResponse0, votes = Votes0} = State0) ->
    HeartbeatResponse1 = HeartbeatResponse0#{NodeId => erlang:monotonic_time(millisecond)},
    Votes1 = Votes0#{NodeId => true},
    State1 = State0#raft_state{heartbeat_response_ts = HeartbeatResponse1, votes = Votes1},
    case compute_quorum(Votes1, false, config(State1)) of
        true ->
            Duration = erlang:monotonic_time(millisecond) - StateStartTs,
            LastIndex = wa_raft_log:last_index(View),
            {ok, LastTerm} = wa_raft_log:term(View, LastIndex),
            EstablishedQuorum = [Peer || {Peer, true} <- maps:to_list(Votes1)],
            ?LOG_NOTICE("Server[~0p, term ~0p, candidate] is becoming leader after ~0p ms with log at ~0p:~0p and votes from ~0p.",
                [Name, CurrentTerm, Duration, LastIndex, LastTerm, EstablishedQuorum], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_GATHER('raft.candidate.election.duration', Duration),
            {next_state, leader, State1};
        false ->
            {keep_state, State1}
    end;

%% [Vote RPC] Candidate receives a negative vote (Candidate cannot become leader here. Losing
%%            an election does not need to convert candidate to follower.) (5.2)
candidate(cast, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, NodeId), ?VOTE(false)), #raft_state{votes = Votes} = State) ->
    {keep_state, State#raft_state{votes = maps:put(NodeId, false, Votes)}};

%% [Handover RPC] Switch to follower because current term now has a leader (5.2, 5.3)
candidate(Type, ?REMOTE(_Sender, ?HANDOVER(_Ref, _PrevLogIndex, _PrevLogTerm, _LogEntries)) = Event,
          #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] switching to follower to handle handover.",
        [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    {next_state, follower, State, {next_event, Type, Event}};

%% [HandoverFailed RPC] Candidates cannot initiate handovers
candidate(_Type, ?REMOTE(Sender, ?HANDOVER_FAILED(_Ref)),
          #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, candidate] got conflicting handover failed from ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Candidate] Handle Election Timeout (5.2)
%% Candidate doesn't get enough votes after a period of time, restart election.
candidate(state_timeout, _, #raft_state{name = Name, current_term = CurrentTerm, votes = Votes} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, candidate] election timed out with votes ~0p. Starting new election.",
        [Name, CurrentTerm, Votes], #{domain => [whatsapp, wa_raft]}),
    {repeat_state, State};

candidate(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

candidate(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, candidate] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

-spec disabled(Type :: enter, PreviousStateName :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
              (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
disabled(enter, PreviousStateName, #raft_state{name = Name, current_term = CurrentTerm, disable_reason = DisableReason} = State0) ->
    ?RAFT_COUNT('raft.disabled.enter'),
    ?LOG_NOTICE("Server[~0p, term ~0p, disabled] becomes disabled from state ~0p with reason ~0p.",
        [Name, CurrentTerm, PreviousStateName, DisableReason], #{domain => [whatsapp, wa_raft]}),
    State1 = case DisableReason of
        undefined -> State0#raft_state{disable_reason = "No reason specified."};
        _         -> State0
    end,
    {keep_state, enter_state(?FUNCTION_NAME, State1)};

%% [AdvanceTerm] Advance to newer term when requested
disabled(internal, ?ADVANCE_TERM(NewerTerm), #raft_state{name = Name, current_term = CurrentTerm} = State) when NewerTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.disabled.advance_term'),
    ?LOG_NOTICE("Server[~0p, term ~0p, disabled] advancing to new term ~0p.",
        [Name, CurrentTerm, NewerTerm], #{domain => [whatsapp, wa_raft]}),
    {keep_state, advance_term(?FUNCTION_NAME, NewerTerm, undefined, State)};

%% [AdvanceTerm] Ignore attempts to advance to an older or current term
disabled(internal, ?ADVANCE_TERM(Term), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, disabled] ignoring attempt to advance to older or current term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Protocol] Handle any RPCs
disabled(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    % eqwalizer:fixme T169659719
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

disabled(_Type, ?REMOTE(_Sender, ?APPEND_ENTRIES(_PrevLogIndex, _PrevLogTerm, _Entries, _CommitIndex, _TrimIndex)), #raft_state{}) ->
    %% Ignore any other AppendEntries RPC calls because a disabled node should be invisible to the cluster.
    keep_state_and_data;

disabled(_Type, ?REMOTE(_Sender, ?REQUEST_VOTE(_ElectionType, _LastLogIndex, _LastLogTerm)), #raft_state{}) ->
    %% Ignore any RequestVote RPC calls because a disabled node should be invisible to the cluster.
    keep_state_and_data;

disabled({call, From}, ?PROMOTE_COMMAND(_Term, _Force, _Config), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, disabled] cannot be promoted.", [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, disabled}}};

disabled({call, From}, ?ENABLE_COMMAND, #raft_state{name = Name, current_term = CurrentTerm} = State0) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, disabled] re-enabling by request from ~p by moving to stalled state.",
        [Name, CurrentTerm, From], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{disable_reason = undefined},
    wa_raft_durable_state:store(State1),
    {next_state, stalled, State1, {reply, From, ok}};

disabled(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

disabled(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, disabled] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

%% [Witness] State functions
-spec witness(Type :: enter, PreviousStateName :: state(), Data :: #raft_state{}) -> gen_statem:state_enter_result(state(), #raft_state{});
             (Type :: gen_statem:event_type(), Event :: event(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
witness(enter, PreviousStateName, #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?RAFT_COUNT('raft.witness.enter'),
    ?LOG_NOTICE("Server[~0p, term ~0p, witness] becomes witness from state ~0p.",
        [Name, CurrentTerm, PreviousStateName], #{domain => [whatsapp, wa_raft]}),
    State1 = enter_state(?FUNCTION_NAME, State#raft_state{witness = true}),
    {keep_state, State1, ?ELECTION_TIMEOUT(State1)};

%% [AdvanceTerm] Advance to newer term when requested
witness(internal, ?ADVANCE_TERM(NewerTerm), #raft_state{name = Name, current_term = CurrentTerm} = State) when NewerTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.witness.advance_term'),
    ?LOG_NOTICE("Server[~0p, term ~0p, witness] advancing to new term ~0p.",
        [Name, CurrentTerm, NewerTerm], #{domain => [whatsapp, wa_raft]}),
    {keep_state, advance_term(?FUNCTION_NAME, NewerTerm, undefined, State)};

%% [AdvanceTerm] Ignore attempts to advance to an older or current term
witness(internal, ?ADVANCE_TERM(Term), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, witness] ignoring attempt to advance to older or current term ~0p.",
        [Name, CurrentTerm, Term], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

witness(state_timeout, _, State) ->
    {repeat_state, State};

%% [Protocol] Handle any RPCs
witness(Type, Event, State) when is_tuple(Event), element(1, Event) =:= rpc ->
    % eqwalizer:fixme T169659719
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [Witness] Handle AppendEntries RPC (5.2, 5.3)
witness(Type, ?REMOTE(Leader, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex)), State) ->
    handle_heartbeat(?FUNCTION_NAME, Type, Leader, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex, State);

%% [AppendEntriesResponse] Witnesses do not send AppendEntries and so should not get responses
witness(_Type, ?REMOTE(Sender, ?APPEND_ENTRIES_RESPONSE(_PrevLogIndex, _Success, _LastLogIndex)),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, follower] got conflicting response from ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

% [Witness] Witnesses ignore any handover requests.
witness(_Type, ?REMOTE(Sender, ?HANDOVER(Ref, _PrevLogIndex, _PrevLogTerm, _LogEntries)),
       #raft_state{name = Name, current_term = CurrentTerm} = State) ->
    ?LOG_WARNING("Server[~0p, term ~0p, witness] got invalid handover request from ~p while witness.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Sender, ?HANDOVER_FAILED(Ref), State),
    keep_state_and_data;

%% [HandoverFailed RPC] Witnesses cannot initiate handovers
witness(_Type, ?REMOTE(Sender, ?HANDOVER_FAILED(_Ref)),
         #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, witness] got conflicting handover failed from ~p.",
        [Name, CurrentTerm, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

witness({call, From}, ?SNAPSHOT_AVAILABLE_COMMAND(_, #raft_log_pos{index = SnapshotIndex, term = SnapshotTerm} = SnapshotPos),
        #raft_state{log_view = View0, name = Name, current_term = CurrentTerm, last_applied = LastApplied} = State0) ->
    case SnapshotIndex > LastApplied orelse LastApplied =:= 0 of
        true ->
            ?LOG_NOTICE("Server[~0p, term ~0p, witness] accepting snapshot ~p:~p but not loading",
                [Name, CurrentTerm, SnapshotIndex, SnapshotTerm], #{domain => [whatsapp, wa_raft]}),
            {ok, View1} = wa_raft_log:reset(View0, SnapshotPos),
            State1 = State0#raft_state{log_view = View1, last_applied = SnapshotIndex, commit_index = SnapshotIndex},
            State2 = load_config(State1),
            {next_state, witness, State2, {reply, From, ok}};
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, witness] ignoring available snapshot ~p:~p with index not past ours (~p)",
                [Name, CurrentTerm, SnapshotIndex, SnapshotTerm, LastApplied], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;

%% [RequestVote] A witness with an unallocated vote should decide if the requesting candidate is eligible to receive
%%               its vote for the current term and affirm or reject accordingly.
witness(_Type, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, CandidateId) = Candidate, ?REQUEST_VOTE(_ElectionType, CandidateIndex, CandidateTerm)),
         #raft_state{name = Name, log_view = View, current_term = CurrentTerm, voted_for = undefined} = State) ->
    Index = wa_raft_log:last_index(View),
    {ok, Term} = wa_raft_log:term(View, Index),
    % Witnesses should only vote for candidates whose logs are at least as up-to-date as the local log.
    % Logs are ordered in up-to-dateness by the lexicographic order of the {Term, Index} pair of their latest entry. (5.4.1)
    case {CandidateTerm, CandidateIndex} >= {Term, Index} of
        true ->
            ?LOG_NOTICE("Server[~0p, term ~0p, witness] decides to vote for candidate ~0p with up-to-date log at ~0p:~0p versus local log at ~0p:~0p.",
                [Name, CurrentTerm, Candidate, CandidateIndex, CandidateTerm, Index, Term], #{domain => [whatsapp, wa_raft]}),
            NewState = State#raft_state{voted_for = CandidateId},
            % Persist the vote to stable storage before responding to the vote request. (Fig. 2)
            wa_raft_durable_state:store(NewState),
            send_rpc(Candidate, ?VOTE(true), State),
            {keep_state, NewState};
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, witness] refuses to vote for candidate ~0p with outdated log at ~0p:~0p versus local log at ~0p:~0p.",
                [Name, CurrentTerm, Candidate, CandidateIndex, CandidateTerm, Index, Term], #{domain => [whatsapp, wa_raft]}),
            send_rpc(Candidate, ?VOTE(false), State),
            keep_state_and_data
    end;
%% [RequestVote] A witness should affirm any vote requests for the candidate it already voted for in the current term.
witness(_Type, ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, CandidateId) = Candidate, ?REQUEST_VOTE(_ElectionType, _CandidateIndex, _CandidateTerm)),
         #raft_state{name = Name, current_term = CurrentTerm, voted_for = CandidateId} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, witness] repeating prior vote for candidate ~0p.",
        [Name, CurrentTerm, Candidate], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Candidate, ?VOTE(true), State),
    keep_state_and_data;
%% [RequestVote] A witness should reject any vote requests for the candidate it did not vote for in the current term.
witness(_Type, ?REMOTE(Candidate, ?REQUEST_VOTE(_ElectionType, _CandidateIndex, _CandidateTerm)),
         #raft_state{name = Name, current_term = CurrentTerm, voted_for = VotedFor} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, witness] refusing to vote for candidate ~0p after previously voting for candidate ~0p in the current term.",
        [Name, CurrentTerm, Candidate, VotedFor], #{domain => [whatsapp, wa_raft]}),
    send_rpc(Candidate, ?VOTE(false), State),
    keep_state_and_data;

%% [Vote] A witness should ignore any votes because its not eligible for leadership
witness(_Type, ?REMOTE(Sender, ?VOTE(Voted)), #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, witness] got unexecpted vote ~p from ~p.",
        [Name, CurrentTerm, Voted, Sender], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data;

%% [Witness] Witness receives RAFT command
witness(Type, ?RAFT_COMMAND(_COMMAND, _Payload) = Event, State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Witness] Witness receives unknown event
witness(Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_WARNING("Server[~0p, term ~0p, witness] receives unknown ~p event ~p",
        [Name, CurrentTerm, Type, Event], #{domain => [whatsapp, wa_raft]}),
    keep_state_and_data.

-spec terminate(Reason :: term(), State :: state(), Data :: #raft_state{}) -> ok.
terminate(Reason, State, #raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm} = Data) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] terminating due to ~p.",
        [Name, CurrentTerm, State, Reason], #{domain => [whatsapp, wa_raft]}),
    wa_raft_durable_state:sync(Data),
    wa_raft_info:delete_state(Table, Partition),
    wa_raft_info:set_stale(Table, Partition, true),
    ok.

%% Fallbacks for command calls to the RAFT server for when there is no state-specific
%% handler for a particular command defined for a particular RAFT server FSM state.
-spec command(state(), gen_statem:event_type(), command(), #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% [Commit] Non-leader nodes should fail commits with {error, not_leader}.
command(StateName, cast, ?COMMIT_COMMAND({Key, _}),
        #raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm, leader_id = LeaderId}) when StateName =/= leader ->
    ?LOG_WARNING("Server[~0p, term ~0p, ~0p] commit with key ~p fails. Leader is ~p.",
        [Name, CurrentTerm, StateName, Key, LeaderId], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Key, {error, not_leader}),
    keep_state_and_data;
%% [Strong Read] Non-leader nodes are not eligible for strong reads.
command(StateName, cast, ?READ_COMMAND({From, _}),
        #raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm, leader_id = LeaderId}) when StateName =/= leader ->
    ?LOG_WARNING("Server[~0p, term ~0p, ~0p] strong read fails. Leader is ~p.",
        [Name, CurrentTerm, StateName, LeaderId], #{domain => [whatsapp, wa_raft]}),
    wa_raft_queue:fulfill_incomplete_read(Table, Partition, From, {error, not_leader}),
    keep_state_and_data;
%% [Status] Get status of node.
command(StateName, {call, From}, ?STATUS_COMMAND, State) ->
    Status = [
        {state, StateName},
        {id, State#raft_state.self#raft_identity.node},
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
        #raft_state{application = App, name = Name, log_view = View0, current_term = CurrentTerm, leader_heartbeat_ts = HeartbeatTs} = State0) when StateName =/= disabled ->
    ElectionWeight = ?RAFT_ELECTION_WEIGHT(App),
    SavedConfig = config(State0),
    Allowed = if
        Term =< CurrentTerm ->
            ?LOG_ERROR("Server[~0p, term ~0p, ~0p] cannot attempt promotion to invalid term ~p.",
                [Name, CurrentTerm, StateName, Term], #{domain => [whatsapp, wa_raft]}),
            false;
        ElectionWeight =:= 0 ->
            ?LOG_ERROR("Server[~0p, term ~0p, ~0p] node election weight is zero and cannot be promoted as leader.",
                [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
            false;
        % Prevent promotions to any operational state when there is no cluster membership configuration.
        (not is_map_key(membership, SavedConfig)) andalso (Config =:= undefined orelse not is_map_key(membership, Config)) ->
            ?LOG_ERROR("Server[~0p, term ~0p, ~0p] cannot promote with neither existing nor forced config having configured membership.",
                [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
            false;
        StateName =:= witness ->
            ?LOG_ERROR("Server[~0p, term ~0p, ~0p] cannot promote a witness node.",
                [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
            false;
        Force ->
            true;
        Config =/= undefined ->
            ?LOG_ERROR("Server[~0p, term ~0p, ~0p] cannot forcibly apply a configuration when doing a non-forced promotion.",
                [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
            false;
        StateName =:= stalled ->
            ?LOG_ERROR("Server[~0p, term ~0p, ~0p] cannot promote a stalled node.",
                [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
            false;
        HeartbeatTs =:= undefined ->
            true;
        true ->
            erlang:monotonic_time(millisecond) - HeartbeatTs >= ?RAFT_PROMOTION_GRACE_PERIOD(App) * 1000
    end,
    case Allowed of
        true ->
            NewStateName = case Force of
                true  -> leader;
                false -> candidate
            end,
            ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] promoted to ~p of term ~p.",
                [Name, CurrentTerm, StateName, NewStateName, Term], #{domain => [whatsapp, wa_raft]}),

            % Advance to the term requested for promotion to
            State1 = advance_term(StateName, Term, undefined, State0),
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
            ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] rejected leader promotion of term ~p.",
                [Name, CurrentTerm, StateName, Term], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;
%% [Resign] Non-leader nodes cannot resign.
command(StateName, {call, From}, ?RESIGN_COMMAND, #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= leader ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] not resigning because we are not leader.",
        [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, not_leader}}};
%% [AdjustMembership] Non-leader nodes cannot adjust their config.
command(StateName, Type, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer, ConfigIndex), #raft_state{name = Name, current_term = CurrentTerm} = State) when StateName =/= leader ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] cannot ~p peer ~p config index ~p because we are not leader.",
        [Name, CurrentTerm, StateName, Action, Peer, ConfigIndex], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, not_leader}),
    {keep_state, State};
%% [Snapshot Available] Follower and candidate nodes might switch to stalled to install snapshot.
command(StateName, {call, From} = Type, ?SNAPSHOT_AVAILABLE_COMMAND(_Root, #raft_log_pos{index = SnapshotIndex}) = Event,
        #raft_state{name = Name, current_term = CurrentTerm, last_applied = LastAppliedIndex} = State)
            when StateName =:= follower orelse StateName =:= candidate ->
    case SnapshotIndex > LastAppliedIndex of
        true ->
            ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] got snapshot with newer index ~p compared to currently applied index ~p",
                [Name, CurrentTerm, StateName, SnapshotIndex, LastAppliedIndex], #{domain => [whatsapp, wa_raft]}),
            {next_state, stalled, State, {next_event, Type, Event}};
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] ignoring snapshot with index ~p compared to currently applied index ~p",
                [Name, CurrentTerm, StateName, SnapshotIndex, LastAppliedIndex], #{domain => [whatsapp, wa_raft]}),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;
%% [Handover Candidates] Non-leader nodes cannot serve handovers.
command(StateName, {call, From}, ?HANDOVER_CANDIDATES_COMMAND,
        #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= leader ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] dropping handover candidates request due to not being leader.",
        [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, not_leader}}};
%% [Handover] Non-leader nodes cannot serve handovers.
command(StateName, Type, ?HANDOVER_COMMAND(_Peer),
        #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= leader ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] rejecting handover request because we are not leader.",
        [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
    reply(Type, {error, not_leader}),
    keep_state_and_data;
%% [Enable] Non-disabled nodes are already enabled.
command(StateName, {call, From}, ?ENABLE_COMMAND, #raft_state{name = Name, current_term = CurrentTerm}) when StateName =/= disabled ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] got enable request from ~p while enabled.",
        [Name, CurrentTerm, StateName, From], #{domain => [whatsapp, wa_raft]}),
    {keep_state_and_data, {reply, From, {error, already_enabled}}};
%% [Disable] All nodes should disable by setting RAFT state disable_reason.
command(StateName, cast, ?DISABLE_COMMAND(Reason), #raft_state{name = Name, self = ?IDENTITY_REQUIRES_MIGRATION(_, NodeId), current_term = CurrentTerm, leader_id = LeaderId} = State0) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] disabling due to reason ~p.",
        [Name, CurrentTerm, StateName, Reason], #{domain => [whatsapp, wa_raft]}),
    State1 = State0#raft_state{disable_reason = Reason},
    State2 = case NodeId =:= LeaderId of
        true  -> clear_leader(StateName, State1);
        false -> State1
    end,
    wa_raft_durable_state:store(State2),
    {next_state, disabled, State2};
%% [Fallback] Drop unknown command calls.
command(StateName, Type, Event, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] dropping unhandled command ~p event ~p",
        [Name, CurrentTerm, StateName, Type, Event], #{domain => [whatsapp, wa_raft]}),
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

-spec config_identities(Config :: config()) -> Peers :: [#raft_identity{}].
config_identities(#{membership := Membership}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Membership];
config_identities(_Config) ->
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
load_config(#raft_state{storage = Storage, table = Table, partition = Partition} = State) ->
    case wa_raft_storage:read_metadata(Storage, config) of
        {ok, #raft_log_pos{index = ConfigIndex}, Config} ->
            wa_raft_info:set_membership(Table, Partition, maps:get(membership, Config, [])),
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
maybe_update_config(Index, _Term, {_Ref, {config, Config}}, #raft_state{table = Table, partition = Partition} = State) ->
    wa_raft_info:set_membership(Table, Partition, maps:get(membership, Config, [])),
    State#raft_state{cached_config = {Index, Config}};
maybe_update_config(_Index, _Term, _Op, State) ->
    State.

%%
%% Private functions
%%
-spec random_election_timeout(#raft_state{}) -> non_neg_integer().
random_election_timeout(#raft_state{application = App}) ->
    Max = ?RAFT_ELECTION_TIMEOUT_MAX(App),
    Min = ?RAFT_ELECTION_TIMEOUT_MIN(App),
    Timeout =
        case Max > Min of
            true -> Min + rand:uniform(Max - Min);
            false -> Min
        end,
    case ?RAFT_ELECTION_WEIGHT(App) of
        Weight when Weight > 0 andalso Weight =< ?RAFT_ELECTION_MAX_WEIGHT ->
            % higher weight, shorter timeout so it has more chances to initiate an leader election
            round(Timeout * ?RAFT_ELECTION_MAX_WEIGHT div Weight);
        _ ->
            Timeout * ?RAFT_ELECTION_DEFAULT_WEIGHT
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
                    apply_log(State0, CommitIndex, TrimIndex, CurrentTerm);
                {ok, Term} when Term < CurrentTerm ->
                    % Raft paper section 5.4.3 - as a leader, don't commit entries from previous term if no log entry of current term has applied yet
                    ?RAFT_COUNT('raft.apply.delay.old'),
                    ?LOG_WARNING("Server[~0p, term ~0p, leader] delays commit of log entry ~0p with old term ~0p.",
                        [Name, CurrentTerm, EndIndex, Term], #{domain => [whatsapp, wa_raft]}),
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

-spec apply_log(Data :: #raft_state{}, CommitIndex :: wa_raft_log:log_index(), TrimIndex :: wa_raft_log:log_index() | infinity, EffectiveTerm :: wa_raft_log:log_term() | undefined) -> NewData :: #raft_state{}.
apply_log(#raft_state{witness = true} = State0, CommitIndex, _TrimIndex, _EffectiveTerm) ->
    % Update CommitIndex and LastAppliedIndex, but don't apply new log entries
    State0#raft_state{last_applied = CommitIndex, commit_index = CommitIndex};
apply_log(#raft_state{application = App, name = Name, table = Table, partition = Partition, log_view = View,
                      last_applied = LastApplied, current_term = CurrentTerm} = State0, CommitIndex, TrimIndex, EffectiveTerm) when CommitIndex > LastApplied ->
    StartT = os:timestamp(),
    case wa_raft_queue:apply_queue_full(Table, Partition) of
        false ->
            % Apply a limited number of log entries (both count and total byte size limited)
            LimitedIndex = erlang:min(CommitIndex, LastApplied + ?RAFT_MAX_CONSECUTIVE_APPLY_ENTRIES(App)),
            LimitBytes = ?RAFT_MAX_CONSECUTIVE_APPLY_BYTES(App),
            {ok, {_, #raft_state{log_view = View1} = State1}} = wa_raft_log:fold(View, LastApplied + 1, LimitedIndex, LimitBytes,
                fun (Index, Entry, {Index, State}) ->
                    wa_raft_queue:reserve_apply(Table, Partition),
                    {Index + 1, apply_op(Index, Entry, EffectiveTerm, State)}
                end, {LastApplied + 1, State0}),

            % Perform log trimming since we've now applied some log entries, only keeping
            % at maximum MaxRotateDelay log entries.
            MaxRotateDelay = ?RAFT_MAX_RETAINED_ENTRIES(App),
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
                ?LOG_WARNING("Server[~0p, term ~0p] delays applying for long queue ~0p with last applied ~0p.",
                    [Name, CurrentTerm, ApplyQueueSize, LastApplied], #{domain => [whatsapp, wa_raft]}),
            ?RAFT_GATHER('raft.apply_log.latency_us', timer:now_diff(os:timestamp(), StartT)),
            State0
    end;
apply_log(State, _CommitIndex, _TrimIndex, _EffectiveTerm) ->
    State.

-spec apply_op(wa_raft_log:log_index(), wa_raft_log:log_entry(), wa_raft_log:log_term() | undefined, #raft_state{}) -> #raft_state{}.
apply_op(LogIndex, _Entry, _EffectiveTerm, #raft_state{name = Name, last_applied = LastAppliedIndex, current_term = CurrentTerm} = State) when LogIndex =< LastAppliedIndex ->
    ?LOG_WARNING("Server[~0p, term ~0p] is skipping applying log entry ~0p because log entries up to ~0p are already applied.",
        [Name, CurrentTerm, LogIndex, LastAppliedIndex], #{domain => [whatsapp, wa_raft]}),
    State;
apply_op(LogIndex, {Term, Op}, EffectiveTerm, #raft_state{storage = Storage} = State0) ->
    wa_raft_storage:apply_op(Storage, {LogIndex, {Term, Op}}, EffectiveTerm),
    maybe_update_config(LogIndex, Term, Op, State0#raft_state{last_applied = LogIndex, commit_index = LogIndex});
apply_op(LogIndex, undefined, _EffectiveTerm, #raft_state{name = Name, log_view = View, current_term = CurrentTerm}) ->
    ?RAFT_COUNT('raft.server.missing.log.entry'),
    ?LOG_ERROR("Server[~0p, term ~0p] failed to apply ~0p because log entry is missing from log covering ~0p to ~0p.",
        [Name, CurrentTerm, LogIndex, wa_raft_log:first_index(View), wa_raft_log:last_index(View)], #{domain => [whatsapp, wa_raft]}),
    exit({invalid_op, LogIndex});
apply_op(LogIndex, Entry, _EffectiveTerm, #raft_state{name = Name, current_term = CurrentTerm}) ->
    ?RAFT_COUNT('raft.server.corrupted.log.entry'),
    ?LOG_ERROR("Server[~0p, term ~0p] failed to apply unrecognized entry ~p at ~p",
        [Name, CurrentTerm, Entry, LogIndex], #{domain => [whatsapp, wa_raft]}),
    exit({invalid_op, LogIndex, Entry}).

-spec append_entries_to_followers(State0 :: #raft_state{}) -> State1 :: #raft_state{}.
append_entries_to_followers(#raft_state{name = Name, table = Table, partition = Partition, log_view = View0, current_term = CurrentTerm} = State0) ->
    State1 = case wa_raft_log:sync(View0) of
        {ok, View1} ->
            State0#raft_state{log_view = View1};
        skipped ->
            {ok, Pending, View1} = wa_raft_log:cancel(View0),
            ?RAFT_COUNT('raft.server.sync.skipped'),
            ?LOG_WARNING("Server[~0p, term ~0p, leader] skipped pre-heartbeat sync for ~p log entr(ies).",
                [Name, CurrentTerm, length(Pending)], #{domain => [whatsapp, wa_raft]}),
            [wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Reference, {error, commit_stalled}) || {_Term, {Reference, _Command}} <- Pending],
            State0#raft_state{log_view = View1};
        {error, Error} ->
            ?RAFT_COUNT({'raft.server.sync', Error}),
            ?LOG_ERROR("Server[~0p, ter ~0p, leader] sync failed due to ~p.",
                [Name, CurrentTerm, Error], #{domain => [whatsapp, wa_raft]}),
            error(Error)
    end,
    lists:foldl(
        fun (Self, #raft_state{self = Self} = StateN) -> StateN;
            (Peer, StateN)                            -> heartbeat(Peer, StateN)
        end, State1, config_identities(config(State1))).

%%-------------------------------------------------------------------
%% RAFT Server State Management
%%-------------------------------------------------------------------

-spec set_leader(StateName :: state(), Leader :: #raft_identity{}, State :: #raft_state{}) -> #raft_state{}.
set_leader(_StateName, ?IDENTITY_REQUIRES_MIGRATION(_, Node), #raft_state{leader_id = Node} = State) ->
    State;
set_leader(StateName, ?IDENTITY_REQUIRES_MIGRATION(_, Node), #raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] changes leader to ~0p.",
        [Name, CurrentTerm, StateName, Node], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_current_term_and_leader(Table, Partition, CurrentTerm, Node),
    State#raft_state{leader_id = Node}.

-spec clear_leader(state(), #raft_state{}) -> #raft_state{}.
clear_leader(_StateName, #raft_state{leader_id = undefined} = State) ->
    State;
clear_leader(StateName, #raft_state{name = Name, table = Table, partition = Partition, current_term = CurrentTerm} = State) ->
    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] clears leader record.",
        [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
    wa_raft_info:set_current_term_and_leader(Table, Partition, CurrentTerm, undefined),
    State#raft_state{leader_id = undefined}.

%% Setup the RAFT state upon entry into a new RAFT server state.
-spec enter_state(StateName :: state(), State :: #raft_state{}) -> #raft_state{}.
enter_state(StateName, #raft_state{table = Table, partition = Partition, storage = Storage} = State0) ->
    Now = erlang:monotonic_time(millisecond),
    State1 = State0#raft_state{state_start_ts = Now},
    true = wa_raft_info:set_state(Table, Partition, StateName),
    ok = check_stale_upon_entry(StateName, Now, State1),
    ok = wa_raft_storage:cancel(Storage),
    State1.

-spec check_stale_upon_entry(StateName :: state(), Now :: integer(), State :: #raft_state{}) -> ok.
%% Followers and candidates may be stale upon entry due to not receiving a timely heartbeat from an active leader.
check_stale_upon_entry(StateName, Now, #raft_state{application = Application, name = Name, table = Table, partition = Partition,
                                                   leader_heartbeat_ts = LeaderHeartbeatTs, current_term = CurrentTerm}) when StateName =:= follower; StateName =:= candidate ->
    Stale = case LeaderHeartbeatTs of
        undefined ->
            ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] is stale upon entry due to having no prior leader heartbeat.",
                [Name, CurrentTerm, StateName], #{domain => [whatsapp, wa_raft]}),
            true;
        _ ->
            Delay = Now - LeaderHeartbeatTs,
            case Delay > ?RAFT_FOLLOWER_STALE_INTERVAL(Application) of
                true ->
                    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] is stale upon entry because the last leader heartbeat was received ~0p ms ago.",
                        [Name, CurrentTerm, StateName, Delay], #{domain => [whatsapp, wa_raft]}),
                    true;
                false ->
                    false
            end
    end,
    true = wa_raft_info:set_stale(Table, Partition, Stale),
    ok;
%% Leaders are never stale upon entry.
check_stale_upon_entry(leader, _Now, #raft_state{table = Table, partition = Partition}) ->
    true = wa_raft_info:set_stale(Table, Partition, false),
    ok;
%% Witness, stalled and disabled servers are always stale.
check_stale_upon_entry(_StateName, _Now, #raft_state{table = Table, partition = Partition}) ->
    true = wa_raft_info:set_stale(Table, Partition, true),
    ok.

%% Set a new current term and voted-for peer and clear any state that is associated with the previous term.
-spec advance_term(StateName :: state(), NewerTerm :: wa_raft_log:log_term(), VotedFor :: undefined | node(), State :: #raft_state{}) -> #raft_state{}.
advance_term(StateName, NewerTerm, VotedFor, #raft_state{current_term = CurrentTerm} = State0) when NewerTerm > CurrentTerm ->
    State1 = State0#raft_state{
        current_term = NewerTerm,
        voted_for = VotedFor,
        votes = #{},
        next_index = #{},
        match_index = #{},
        last_heartbeat_ts = #{},
        heartbeat_response_ts = #{},
        handover = undefined
    },
    State2 = clear_leader(StateName, State1),
    ok = wa_raft_durable_state:store(State2),
    State2.

%%-------------------------------------------------------------------
%% RAFT Leader Functionality
%%-------------------------------------------------------------------

-spec heartbeat(#raft_identity{}, #raft_state{}) -> #raft_state{}.
heartbeat(?IDENTITY_REQUIRES_MIGRATION(_, FollowerId) = Sender,
          #raft_state{application = App, name = Name, log_view = View, catchup = Catchup, current_term = CurrentTerm,
                      commit_index = CommitIndex, next_index = NextIndex0, match_index = MatchIndex,
                      last_heartbeat_ts = LastHeartbeatTs, first_current_term_log_index = TermStartIndex} = State0) ->
    FollowerNextIndex = maps:get(FollowerId, NextIndex0, TermStartIndex),
    PrevLogIndex = FollowerNextIndex - 1,
    PrevLogTermRes = wa_raft_log:term(View, PrevLogIndex),
    FollowerMatchIndex = maps:get(FollowerId, MatchIndex, 0),
    FollowerMatchIndex =/= 0 andalso
        ?RAFT_GATHER('raft.leader.follower.lag', CommitIndex - FollowerMatchIndex),
    IsCatchingUp = wa_raft_log_catchup:is_catching_up(Catchup, Sender),
    NowTs = erlang:monotonic_time(millisecond),
    LastFollowerHeartbeatTs = maps:get(FollowerId, LastHeartbeatTs, undefined),
    State1 = State0#raft_state{last_heartbeat_ts = LastHeartbeatTs#{FollowerId => NowTs}, leader_heartbeat_ts = NowTs},
    LastIndex = wa_raft_log:last_index(View),
    Witnesses = config_witnesses(config(State0)),
    case PrevLogTermRes =:= not_found orelse IsCatchingUp of %% catching up, or prep
        true ->
            {ok, LastTerm} = wa_raft_log:term(View, LastIndex),
            ?LOG_DEBUG("Server[~0p, term ~0p, leader] sends empty heartbeat to follower ~p (local ~p, prev ~p, catching-up ~p)",
                [Name, CurrentTerm, FollowerId, LastIndex, PrevLogIndex, IsCatchingUp], #{domain => [whatsapp, wa_raft]}),
            % Send append entries request.
            send_rpc(Sender, ?APPEND_ENTRIES(LastIndex, LastTerm, [], CommitIndex, 0), State1),
            LastFollowerHeartbeatTs =/= undefined andalso ?RAFT_GATHER('raft.leader.heartbeat.interval_ms', erlang:monotonic_time(millisecond) - LastFollowerHeartbeatTs),
            State1;
        false ->
            Entries =
                case lists:member({Name, FollowerId}, Witnesses) of
                    true ->
                        MaxWitnessLogEntries = ?RAFT_HEARTBEAT_MAX_ENTRIES_TO_WITNESS(App),
                        {ok, Terms} = wa_raft_log:get_terms(View, FollowerNextIndex, MaxWitnessLogEntries),
                        [{Term, []} || Term <- Terms];
                    _ ->
                        MaxLogEntries = ?RAFT_HEARTBEAT_MAX_ENTRIES(App),
                        MaxHeartbeatSize = ?RAFT_HEARTBEAT_MAX_BYTES(App),
                        {ok, Ret} = wa_raft_log:get(View, FollowerNextIndex, MaxLogEntries, MaxHeartbeatSize),
                        Ret
                    end,
            {ok, PrevLogTerm} = PrevLogTermRes,
            ?RAFT_GATHER('raft.leader.heartbeat.size', length(Entries)),
            ?LOG_DEBUG("Server[~0p, term ~0p, leader] heartbeat to follower ~p from ~p(~p entries). Commit index ~p",
                [Name, CurrentTerm, FollowerId, FollowerNextIndex, length(Entries), CommitIndex], #{domain => [whatsapp, wa_raft]}),
            % Compute trim index.
            TrimIndex = lists:min(to_member_list(MatchIndex#{node() => LastIndex}, 0, config(State1))),
            % Send append entries request.
            CastResult = send_rpc(Sender, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex), State1),
            NextIndex1 =
                case CastResult of
                    ok ->
                        % pipelining - move NextIndex after sending out logs. If a packet is lost, follower's AppendEntriesResponse
                        % will return send back its correct index
                        maps:put(FollowerId, PrevLogIndex + length(Entries) + 1, NextIndex0);
                    _ ->
                        NextIndex0
                end,
            LastFollowerHeartbeatTs =/= undefined andalso ?RAFT_GATHER('raft.leader.heartbeat.interval_ms', erlang:monotonic_time(millisecond) - LastFollowerHeartbeatTs),
            State1#raft_state{next_index = NextIndex1}
    end.

-spec compute_handover_candidates(State :: #raft_state{}) -> [node()].
compute_handover_candidates(#raft_state{application = App, log_view = View, match_index = MatchIndex} = State) ->
    Membership = config_membership(config(State)),
    LastLogIndex = wa_raft_log:last_index(View),
    MaxHandoverLogEntries = ?RAFT_HANDOVER_MAX_ENTRIES(App),
    [Peer || {_Name, Peer} <- Membership, Peer =/= node(), LastLogIndex - maps:get(Peer, MatchIndex, 0) =< MaxHandoverLogEntries].

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

%%-----------------------------------------------------------------------------
%% [AppendEntries] Logic for handling heartbeats from leaders (5.3)
%%-----------------------------------------------------------------------------
%% Attempt to append the log entries declared by a leader in a heartbeat,
%% apply committed but not yet applied log entries, trim the log, and reset
%% the election timeout timer as necessary.
%%-----------------------------------------------------------------------------

-spec handle_heartbeat(State :: state(), Event :: gen_statem:event_type(), Leader :: #raft_identity{}, PrevLogIndex :: wa_raft_log:log_index(), PrevLogTerm :: wa_raft_log:log_term(),
                       Entries :: [wa_raft_log:log_entry()], CommitIndex :: wa_raft_log:log_index(), TrimIndex :: wa_raft_log:log_index(), Data :: #raft_state{}) -> gen_statem:event_handler_result(state(), #raft_state{}).
handle_heartbeat(State, Event, Leader, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex, #raft_state{application = App, name = Name, current_term = CurrentTerm, log_view = View} = Data0) ->
    EntryCount = length(Entries),

    ?RAFT_GATHER({raft, State, 'heartbeat.size'}, EntryCount),
    ?LOG_DEBUG("Server[~0p, term ~0p, ~0p] considering appending ~0p log entries in range ~0p to ~0p to log ending at ~0p.",
        [Name, CurrentTerm, State, EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, wa_raft_log:last_index(View)], #{domain => [whatsapp, wa_raft]}),

    case append_entries(State, PrevLogIndex, PrevLogTerm, Entries, EntryCount, Data0) of
        {ok, Accepted, NewLastIndex, Data1} ->
            send_rpc(Leader, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, Accepted, NewLastIndex), Data1),
            reply(Event, ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, Accepted, NewLastIndex)),

            LocalTrimIndex = case ?RAFT_LOG_ROTATION_BY_TRIM_INDEX(App) of
                true  -> TrimIndex;
                false -> infinity
            end,
            Data2 = Data1#raft_state{leader_heartbeat_ts = erlang:monotonic_time(millisecond)},
            Data3 = case Accepted of
                true -> apply_log(Data2, min(CommitIndex, NewLastIndex), LocalTrimIndex, undefined);
                _    -> Data2
            end,
            check_follower_lagging(CommitIndex, Data3),
            {keep_state, Data3, ?ELECTION_TIMEOUT(Data3)};
        {fatal, Reason} ->
            {next_state, disabled, Data0#raft_state{disable_reason = Reason}}
    end.

%%-----------------------------------------------------------------------------
%% [AppendEntries] Logic for log append during heartbeat and handover (5.3)
%%-----------------------------------------------------------------------------
%% Append the provided range of the log entries to the local log only if the
%% term of the previous log matches the term stored by the local log,
%% otherwise, truncate the log if the term does not match or do nothing if
%% the previous log entry is not available locally. If an unrecoverable error
%% is encountered, returns a diagnostic that can be used as a reason to
%% disable the current replica.
%%-------------------------------------------------------------------

-spec append_entries(State :: state(), PrevLogIndex :: wa_raft_log:log_index(), PrevLogTerm :: wa_raft_log:log_term(), Entries :: [wa_raft_log:log_entry()], EntryCount :: non_neg_integer(), Data :: #raft_state{}) ->
    {ok, Accepted :: boolean(), NewLastIndex :: wa_raft_log:log_index(), NewData :: #raft_state{}} | {fatal, Reason :: term()}.
append_entries(State, PrevLogIndex, PrevLogTerm, Entries, EntryCount, #raft_state{name = Name, log_view = View, last_applied = LastApplied, current_term = CurrentTerm, leader_id = LeaderId} = Data) ->
    % Inspect the locally stored term associated with the previous log entry to discern if
    % appending the provided range of log entries is allowed.
    case wa_raft_log:term(View, PrevLogIndex) of
        {ok, PrevLogTerm} ->
            % If the term of the log entry previous the entries to be applied matches the term stored
            % with the previous log entry in the local RAFT log, then this follower can proceed with
            % appending to the log.
            {ok, NewLastIndex, NewView} = wa_raft_log:append(View, PrevLogIndex + 1, Entries),
            {ok, true, NewLastIndex, Data#raft_state{log_view = NewView}};
        {ok, LocalPrevLogTerm} ->
            % If the term of the log entry proceeding the entries to be applied does not match the log
            % entry stored with the previous log entry in the local RAFT log, then we need to truncate
            % the log because there is a mismatch between this follower and the leader of the cluster.
            ?RAFT_COUNT({raft, State, 'heartbeat.skip.log_term_mismatch'}),
            ?LOG_WARNING("Server[~0p, term ~0p, ~0p] rejects appending ~0p log entries in range ~0p to ~0p as previous log entry ~0p has term ~0p locally when leader ~0p expects it to have term ~0p.",
                [Name, CurrentTerm, State, EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, PrevLogIndex, LocalPrevLogTerm, LeaderId, PrevLogTerm], #{domain => [whatsapp, wa_raft]}),
            case PrevLogIndex < LastApplied of
                true ->
                    % We cannot validly delete log entries that have already been applied because doing
                    % so means that we are erasing log entries that have already been committed. If we try
                    % to do so, then disable this partition as we've violated a critical invariant.
                    ?RAFT_COUNT({raft, State, 'heartbeat.error.corruption.excessive_truncation'}),
                    ?LOG_WARNING("Server[~0p, term ~0p, ~0p] fails as progress requires truncation of log entry at ~0p due to log mismatch when log entries up to ~0p were already applied.",
                        [Name, CurrentTerm, State, PrevLogIndex, LastApplied], #{domain => [whatsapp, wa_raft]}),
                    {fatal,
                        io_lib:format("Leader ~0p of term ~0p requested truncation of log entry at ~0p due to log term mismatch (local ~0p, leader ~0p) when log entries up to ~0p were already applied.",
                            [LeaderId, CurrentTerm, PrevLogIndex, LocalPrevLogTerm, PrevLogTerm, LastApplied])};
                false ->
                    % We are not deleting already applied log entries, so proceed with truncation.
                    ?LOG_NOTICE("Server[~0p, term ~0p, ~0p] truncating local log ending at ~0p to past ~0p due to log mismatch.",
                        [Name, CurrentTerm, State, wa_raft_log:last_index(View), PrevLogIndex], #{domain => [whatsapp, wa_raft]}),
                    {ok, NewView} = wa_raft_log:truncate(View, PrevLogIndex),
                    {ok, false, wa_raft_log:last_index(NewView), Data#raft_state{log_view = NewView}}
            end;
        not_found ->
            % If the log entry is not found, then ignore and notify the leader of what log entry
            % is required by this follower in the reply.
            ?RAFT_COUNT({raft, State, 'heartbeat.skip.missing_previous_log_entry'}),
            ?LOG_WARNING("Server[~0p, term ~0p, ~0p] skips appending ~0p log entries in range ~0p to ~0p because previous log entry at ~0p is not available in local log covering ~0p to ~0p.",
                [Name, CurrentTerm, State, EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, PrevLogIndex, wa_raft_log:first_index(View), wa_raft_log:last_index(View)], #{domain => [whatsapp, wa_raft]}),
            {ok, false, wa_raft_log:last_index(View), Data};
        {error, Reason} ->
            ?RAFT_COUNT({raft, State, 'heartbeat.skip.failed_to_read_previous_log_entry'}),
            ?LOG_WARNING("Server[~0p, term ~0p, ~0p] skips appending ~0p log entries in range ~0p to ~0p because reading previous log entry at ~0p failed with error ~0P.",
                [Name, CurrentTerm, State, EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, PrevLogIndex, Reason, 30], #{domain => [whatsapp, wa_raft]}),
            {ok, false, wa_raft_log:last_index(View), Data}
    end.

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

-spec send_rpc(Destination :: #raft_identity{}, ProcedureCall :: normalized_procedure(), State :: #raft_state{}) -> term().
send_rpc(Destination, Procedure, #raft_state{self = Self, current_term = Term} = State) ->
    ?MODULE:cast(Destination, make_rpc(Self, Term, Procedure), State).

-spec broadcast_rpc(ProcedureCall :: normalized_procedure(), State :: #raft_state{}) -> term().
broadcast_rpc(ProcedureCall, #raft_state{self = Self} = State) ->
    [send_rpc(Peer, ProcedureCall, State) || Peer <- config_identities(config(State)), Peer =/= Self].

-spec cast(#raft_identity{}, rpc(), #raft_state{}) -> ok | {error, term()}.
cast(#raft_identity{name = Name, node = Node} = Destination, Message, #raft_state{identifier = Identifier, distribution_module = DistributionModule}) ->
    try
        ok = DistributionModule:cast({Name, Node}, Identifier, Message)
    catch
        _:E ->
            ?RAFT_COUNT({'raft.server.cast.error', E}),
            ?LOG_DEBUG("Cast to ~p error ~100p", [Destination, E], #{domain => [whatsapp, wa_raft]}),
            {error, E}
    end.

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
should_heartbeat(#raft_state{application = App, last_heartbeat_ts = LastHeartbeatTs}) ->
    Latest = lists:max(maps:values(LastHeartbeatTs)),
    Current = erlang:monotonic_time(millisecond),
    Current - Latest > ?RAFT_HEARTBEAT_INTERVAL(App).

%% Check follower/witness state due to log entry lag and change stale flag if needed
-spec check_follower_lagging(pos_integer(), #raft_state{}) -> ok.
check_follower_lagging(LeaderCommit, #raft_state{application = App, name = Name, table = Table, partition = Partition, last_applied = LastApplied, current_term = CurrentTerm}) ->
    Lagging = LeaderCommit - LastApplied,
    ?RAFT_GATHER('raft.follower.lagging', Lagging),
    case Lagging < ?RAFT_FOLLOWER_STALE_ENTRIES(App) of
        true ->
            wa_raft_info:get_stale(Table, Partition) =/= false andalso begin
                ?LOG_NOTICE("Server[~0p, term ~0p, follower] catches up.",
                    [Name, CurrentTerm], #{domain => [whatsapp, wa_raft]}),
                wa_raft_info:set_stale(Table, Partition, false)
            end;
        false ->
            wa_raft_info:get_stale(Table, Partition) =/= true andalso begin
                ?LOG_NOTICE("Server[~0p, term ~0p, follower] is far behind ~p (leader ~p, follower ~p)",
                    [Name, CurrentTerm, Lagging, LeaderCommit, LastApplied], #{domain => [whatsapp, wa_raft]}),
                wa_raft_info:set_stale(Table, Partition, true)
            end
    end,
    ok.

%% Check leader state and set stale if needed
-spec check_leader_lagging(#raft_state{}) -> term().
check_leader_lagging(#raft_state{application = App, name = Name, table = Table, partition = Partition,
                                 current_term = CurrentTerm, heartbeat_response_ts = HeartbeatResponse} = State) ->
    NowTs = erlang:monotonic_time(millisecond),
    QuorumTs = compute_quorum(HeartbeatResponse#{node() => NowTs}, 0, config(State)),

    Stale = wa_raft_info:get_stale(Table, Partition),
    QuorumAge = NowTs - QuorumTs,
    MaxAge = ?RAFT_LEADER_STALE_INTERVAL(App),

    case QuorumAge >= MaxAge of
        Stale ->
            ok;
        true ->
            ?LOG_NOTICE("Server[~0p, term ~0p, leader] is now stale due to last heartbeat quorum age being ~p ms >= ~p ms max",
                [Name, CurrentTerm, QuorumAge, MaxAge], #{domain => [whatsapp, wa_raft]}),
            wa_raft_info:set_stale(Table, Partition, true);
        false ->
            ?LOG_NOTICE("Server[~0p, term ~0p, leader] is no longer stale after heartbeat quorum age drops to ~p ms < ~p ms max",
                [Name, CurrentTerm, QuorumAge, MaxAge], #{domain => [whatsapp, wa_raft]}),
            wa_raft_info:set_stale(Table, Partition, false)
    end.

%% Based on information that the leader has available as a result of heartbeat replies, attempt
%% to discern what the best subsequent replication mode would be for this follower.
-spec select_follower_replication_mode(wa_raft_log:log_index(), #raft_state{}) -> snapshot | bulk_logs | logs.
select_follower_replication_mode(FollowerLastIndex, #raft_state{application = App, log_view = View, last_applied = LastAppliedIndex}) ->
    CatchupEnabled = ?RAFT_CATCHUP_ENABLED(App),
    BulkLogThreshold = ?RAFT_CATCHUP_THRESHOLD(App),
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
request_snapshot_for_follower(FollowerId, #raft_state{application = App, name = Name, table = Table, partition = Partition, data_dir = DataDir, log_view = View} = State) ->
    case lists:member({Name, FollowerId}, config_witnesses(config(State))) of
        true  ->
            % If node is a witness, we can bypass the transport process since we don't have to
            % send the full log.  Thus, we can run snapshot_available() here directly
            LastLogIndex = wa_raft_log:last_index(View),
            {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),
            LastLogPos = #raft_log_pos{index = LastLogIndex, term = LastLogTerm},
            wa_raft_server:snapshot_available({Name, FollowerId}, DataDir, LastLogPos);
        false ->
            wa_raft_snapshot_catchup:request_snapshot_transport(App, FollowerId, Table, Partition)
    end.

-spec request_bulk_logs_for_follower(#raft_identity{}, wa_raft_log:log_index(), #raft_state{}) -> ok.
request_bulk_logs_for_follower(#raft_identity{node = FollowerId} = Peer, FollowerEndIndex, #raft_state{name = Name, catchup = Catchup, current_term = CurrentTerm, commit_index = CommitIndex} = State) ->
    ?LOG_DEBUG("Server[~0p, term ~0p, leader] requesting bulk logs catchup for follower ~0p.",
        [Name, CurrentTerm, Peer], #{domain => [whatsapp, wa_raft]}),
    Witness = lists:member({Name, FollowerId}, config_witnesses(config(State))),
    wa_raft_log_catchup:start_catchup_request(Catchup, Peer, FollowerEndIndex, CurrentTerm, CommitIndex, Witness).

-spec cancel_bulk_logs_for_follower(#raft_identity{}, #raft_state{}) -> ok.
cancel_bulk_logs_for_follower(Peer, #raft_state{catchup = Catchup}) ->
    wa_raft_log_catchup:cancel_catchup_request(Catchup, Peer).
