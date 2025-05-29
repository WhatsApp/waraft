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

%%------------------------------------------------------------------------------
%% RAFT Server - OTP Supervision
%%------------------------------------------------------------------------------

-export([
    child_spec/1,
    start_link/1
]).

%%------------------------------------------------------------------------------
%% RAFT Server - Public APIs - RAFT Cluster Configuration
%%------------------------------------------------------------------------------

-export([
    latest_config_version/0
]).

%% Inspection of cluster configuration
-export([
    get_config_version/1,
    get_config_members/1,
    get_config_full_members/1,
    get_config_witness_members/1,
    get_config_witnesses/1,
    is_data_replica/2,
    is_witness/2
]).

%% Creation and modification of cluster configuration
-export([
    make_config/0,
    make_config/1,
    make_config/2,
    normalize_config/1
]).

%% Modification of cluster configuration
-export([
    set_config_members/2,
    set_config_members/3
]).

%%------------------------------------------------------------------------------
%% RAFT Server - Public APIs
%%------------------------------------------------------------------------------

-export([
    status/1,
    status/2,
    membership/1
]).

%%------------------------------------------------------------------------------
%% RAFT Server - Internal APIs - Local Options
%%------------------------------------------------------------------------------

-export([
    default_name/2,
    registered_name/2
]).

%%------------------------------------------------------------------------------
%% RAFT Server - Internal APIs - RPC Handling
%%------------------------------------------------------------------------------

-export([
    make_rpc/3,
    parse_rpc/2
]).

%%------------------------------------------------------------------------------
%% RAFT Server - Internal APIs - Commands
%%------------------------------------------------------------------------------

-export([
    commit/2,
    read/2,
    snapshot_available/3,
    adjust_membership/3,
    adjust_membership/4,
    trigger_election/1,
    trigger_election/2,
    promote/2,
    promote/3,
    resign/1,
    handover/1,
    handover/2,
    handover_candidates/1,
    disable/2,
    enable/1,
    bootstrap/4,
    notify_complete/1
]).

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation
%%------------------------------------------------------------------------------

%% General callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3
]).

%% State-specific callbacks
-export([
    stalled/3,
    leader/3,
    follower/3,
    candidate/3,
    disabled/3,
    witness/3
]).

%%------------------------------------------------------------------------------
%% RAFT Server - Test Exports
%%------------------------------------------------------------------------------

-ifdef(TEST).
-export([
    compute_quorum/3,
    config/1,
    max_index_to_apply/3,
    adjust_config/3
]).
-endif.

%%------------------------------------------------------------------------------
%% RAFT Server - Public Types
%%------------------------------------------------------------------------------

-export_type([
    state/0,
    config/0,
    config_all/0,
    membership/0,
    status/0
]).

%%------------------------------------------------------------------------------

-include_lib("kernel/include/logger.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_rpc.hrl").

%%------------------------------------------------------------------------------

%% Section 5.2. Randomized election timeout for fast election and to avoid split votes
-define(ELECTION_TIMEOUT(State), {state_timeout, random_election_timeout(State), election}).

%% Timeout in milliseconds before the next heartbeat is to be sent by a RAFT leader with no pending log entries
-define(HEARTBEAT_TIMEOUT(State),    {state_timeout, ?RAFT_HEARTBEAT_INTERVAL(State#raft_state.application), heartbeat}).
%% Timeout in milliseconds before the next heartbeat is to be sent by a RAFT leader with pending log entries
-define(COMMIT_BATCH_TIMEOUT(State), {state_timeout, ?RAFT_COMMIT_BATCH_INTERVAL(State#raft_state.application), batch_commit}).

%%------------------------------------------------------------------------------

-define(RAFT_LOG_PREFIX, "Server[~0p, term ~0p, ~0p] ").
-define(RAFT_LOG_ARGS(State, Data, Args), [(Data)#raft_state.name, (Data)#raft_state.current_term, State | Args]).
-define(RAFT_LOG_OPTS, #{domain => [whatsapp, wa_raft]}).

-define(RAFT_LOG(Level, Data, Format, Args), ?RAFT_LOG(Level, ?FUNCTION_NAME, Data, Format, Args)).
-define(RAFT_LOG(Level, State, Data, Format, Args), ?LOG(Level, ?RAFT_LOG_PREFIX Format, ?RAFT_LOG_ARGS(State, Data, Args), ?RAFT_LOG_OPTS)).

-define(RAFT_LOG_DEBUG(Data, Format, Args), ?RAFT_LOG(debug, Data, Format, Args)).
-define(RAFT_LOG_DEBUG(State, Data, Format, Args), ?RAFT_LOG(debug, State, Data, Format, Args)).

-define(RAFT_LOG_NOTICE(Data, Format, Args), ?RAFT_LOG(notice, Data, Format, Args)).
-define(RAFT_LOG_NOTICE(State, Data, Format, Args), ?RAFT_LOG(notice, State, Data, Format, Args)).

-define(RAFT_LOG_WARNING(Data, Format, Args), ?RAFT_LOG(warning, Data, Format, Args)).
-define(RAFT_LOG_WARNING(State, Data, Format, Args), ?RAFT_LOG(warning, State, Data, Format, Args)).

-define(RAFT_LOG_ERROR(Data, Format, Args), ?RAFT_LOG(error, Data, Format, Args)).
-define(RAFT_LOG_ERROR(State, Data, Format, Args), ?RAFT_LOG(error, State, Data, Format, Args)).

%%------------------------------------------------------------------------------
%% RAFT Server - Public Types
%%------------------------------------------------------------------------------

-type state() ::
    stalled |
    leader |
    follower |
    candidate |
    disabled |
    witness.

-type term_or_offset() :: wa_raft_log:log_term() | current | next | {next, Offset :: pos_integer()}.

-type peer() :: {Name :: atom(), Node :: node()}.
-type membership() :: [peer()].

-opaque config() :: config_v1().
-opaque config_all() :: config_v1().

-type config_v1() ::
    #{
        version := 1,
        membership => membership(),
        witness => membership()
    }.

-type status() :: [status_element()].
-type status_element() ::
      {state, state()}
    | {id, atom()}
    | {peers, [{atom(), {node(), atom()}}]}
    | {partition, wa_raft:partition()}
    | {partition_path, file:filename_all()}
    | {current_term, wa_raft_log:log_term()}
    | {voted_for, node()}
    | {commit_index, wa_raft_log:log_index()}
    | {last_applied, wa_raft_log:log_index()}
    | {leader_id, node()}
    | {next_indices, #{node() => wa_raft_log:log_index()}}
    | {match_indices, #{node() => wa_raft_log:log_index()}}
    | {log_module, module()}
    | {log_first, wa_raft_log:log_index()}
    | {log_last, wa_raft_log:log_index()}
    | {votes, #{node() => boolean()}}
    | {inflight_applies, non_neg_integer()}
    | {disable_reason, string()}
    | {witness, boolean()}
    | {config, config()}
    | {config_index, wa_raft_log:log_index()}.

%%------------------------------------------------------------------------------
%% RAFT Server - Private Types
%%------------------------------------------------------------------------------

-type event() :: rpc() | remote(normalized_procedure()) | command() | internal_event() | timeout_type().

-type rpc() :: rpc_named() | legacy_rpc().
-type legacy_rpc() :: ?LEGACY_RAFT_RPC(atom(), wa_raft_log:log_term(), node(), undefined | tuple()).
-type rpc_named() :: ?RAFT_NAMED_RPC(atom(), wa_raft_log:log_term(), atom(), node(), undefined | tuple()).

-type command() :: commit_command() | read_command() | status_command() | trigger_election_command() |
                   promote_command() | resign_command() | adjust_membership_command() |
                   snapshot_available_command() | handover_candidates_command() | handover_command() |
                   enable_command() | disable_command() | bootstrap_command() | notify_complete_command().

-type commit_command()              :: ?COMMIT_COMMAND(wa_raft_acceptor:op()).
-type read_command()                :: ?READ_COMMAND(wa_raft_acceptor:read_op()).
-type status_command()              :: ?STATUS_COMMAND.
-type trigger_election_command()    :: ?TRIGGER_ELECTION_COMMAND(term_or_offset()).
-type promote_command()             :: ?PROMOTE_COMMAND(term_or_offset(), boolean()).
-type resign_command()              :: ?RESIGN_COMMAND.
-type adjust_membership_command()   :: ?ADJUST_MEMBERSHIP_COMMAND(membership_action(), peer() | undefined, wa_raft_log:log_index() | undefined).
-type snapshot_available_command()  :: ?SNAPSHOT_AVAILABLE_COMMAND(string(), wa_raft_log:log_pos()).
-type handover_candidates_command() :: ?HANDOVER_CANDIDATES_COMMAND.
-type handover_command()            :: ?HANDOVER_COMMAND(node()).
-type enable_command()              :: ?ENABLE_COMMAND.
-type disable_command()             :: ?DISABLE_COMMAND(term()).
-type bootstrap_command()           :: ?BOOTSTRAP_COMMAND(wa_raft_log:log_pos(), config(), dynamic()).
-type notify_complete_command()     :: ?NOTIFY_COMPLETE_COMMAND().

-type internal_event() :: advance_term_event() | force_election_event().
-type advance_term_event() :: ?ADVANCE_TERM(wa_raft_log:log_term()).
-type force_election_event() :: ?FORCE_ELECTION(wa_raft_log:log_term()).

-type timeout_type() :: election | heartbeat.

-type membership_action() :: add | add_witness | remove | remove_witness | refresh.

%%------------------------------------------------------------------------------
%% RAFT Server - OTP Supervision
%%------------------------------------------------------------------------------

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

%%------------------------------------------------------------------------------
%% RAFT Server - Public APIs - RAFT Cluster Configuration
%%------------------------------------------------------------------------------

%% Returns the version number for the latest cluster configuration format that
%% is supported by the current RAFT implementation. All cluster configurations
%% returned by methods used to create or modify cluster configurations in this
%% module will return cluster configurations of this version.
-spec latest_config_version() -> pos_integer().
latest_config_version() ->
    1.

-spec get_config_version(Config :: config() | config_all()) -> pos_integer().
get_config_version(#{version := Version}) ->
    Version.

-spec get_config_members(Config :: config() | config_all()) -> [#raft_identity{}].
get_config_members(#{version := 1, membership := Members}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Members];
get_config_members(_) ->
    [].

-spec get_config_full_members(Config :: config() | config_all()) -> [#raft_identity{}].
get_config_full_members(#{version := 1, membership := Members, witness := Witnesses}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Members -- Witnesses];
get_config_full_members(Config) ->
    get_config_members(Config).

-spec get_config_witness_members(Config :: config() | config_all()) -> [#raft_identity{}].
get_config_witness_members(#{version := 1, membership := Members, witness := Witnesses}) ->
    MembersMap = maps:from_keys(Members, []),
    [#raft_identity{name = Name, node = Node} || {Name, Node} = Witness <- Witnesses, maps:is_key(Witness, MembersMap)];
get_config_witness_members(_) ->
    [].

-spec get_config_witnesses(Config :: config() | config_all()) -> [#raft_identity{}].
get_config_witnesses(#{version := 1, witness := Witnesses}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Witnesses];
get_config_witnesses(_) ->
    [].

-spec is_data_replica(Identity :: #raft_identity{}, Config :: config() | config_all()) -> boolean().
is_data_replica(#raft_identity{name = Name, node = Node}, #{version := 1, membership := Membership, witness := Witnesses}) ->
    lists:member({Name, Node}, Membership) and not lists:member({Name, Node}, Witnesses);
is_data_replica(#raft_identity{name = Name, node = Node}, #{version := 1, membership := Membership}) ->
    lists:member({Name, Node}, Membership);
is_data_replica(_, _) ->
    false.

-spec is_witness(Identity :: #raft_identity{}, Config :: config() | config_all()) -> boolean().
is_witness(#raft_identity{name = Name, node = Node}, #{version := 1, witness := Witnesses}) ->
    lists:member({Name, Node}, Witnesses);
is_witness(_, _) ->
    false.

%% Create a new cluster configuration with no members.
%% Without any members, this cluster configuration should not be used as
%% the active configuration for a RAFT cluster.
-spec make_config() -> config().
make_config() ->
    #{
        version => 1,
        membership => [],
        witness => []
    }.

%% Create a new cluster configuration with the provided members.
-spec make_config(Members :: [#raft_identity{}]) -> config().
make_config(Members) ->
    set_config_members(Members, make_config()).

%% Create a new cluster configuration with the provided members and witnesses.
-spec make_config(Members :: [#raft_identity{}], Witnesses :: [#raft_identity{}]) -> config().
make_config(Members, Witnesses) ->
    set_config_members(Members, Witnesses, make_config()).

%% Replace the set of members in the provided cluster configuration.
%% Will upgrade the cluster configuration to the latest version.
-spec set_config_members(Members :: [#raft_identity{}], ConfigAll :: config() | config_all()) -> config().
set_config_members(Members, ConfigAll) ->
    Config = normalize_config(ConfigAll),
    Config#{membership => lists:sort([{Name, Node} || #raft_identity{name = Name, node = Node} <- Members])}.

%% Replace the set of members and witnesses in the provided cluster configuration.
%% Will upgrade the cluster configuration to the latest version.
-spec set_config_members(Members :: [#raft_identity{}], Witnesses :: [#raft_identity{}], ConfigAll :: config() | config_all()) -> config().
set_config_members(Members, Witnesses, ConfigAll) ->
    Config = set_config_members(Members, ConfigAll),
    Config#{witness => lists:sort([{Name, Node} || #raft_identity{name = Name, node = Node} <- Witnesses])}.

%% Attempt to upgrade any configuration from an older configuration version to the
%% latest configuration version if possible.
-spec normalize_config(ConfigAll :: config() | config_all()) -> Config :: config().
normalize_config(#{version := 1} = Config) ->
    % Fill in any missing fields.
    Config#{
        membership => maps:get(membership, Config, []),
        witness => maps:get(witness, Config, [])
    };
normalize_config(#{version := Version}) ->
    % All valid configurations will contain at least their own version; however,
    % we do not know how to handle configurations with newer versions.
    error({unsupported_version, Version});
normalize_config(#{}) ->
    error(no_version).

%%------------------------------------------------------------------------------
%% RAFT Server - Public APIs
%%------------------------------------------------------------------------------

-spec status(Server :: gen_statem:server_ref()) -> status().
status(Server) ->
    gen_statem:call(Server, ?STATUS_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

-spec status
    (Server :: gen_statem:server_ref(), Key :: atom()) -> Value :: dynamic();
    (Server :: gen_statem:server_ref(), Keys :: [atom()]) -> Value :: [dynamic()].
status(Server, Key) when is_atom(Key) ->
    hd(status(Server, [Key]));
status(Server, Keys) when is_list(Keys) ->
    case status(Server) of
        [_|_] = Status ->
            [proplists:get_value(Key, Status, undefined) || Key <- Keys];
        _ ->
            lists:duplicate(length(Keys), undefined)
    end.

-spec membership(Service :: gen_statem:server_ref()) -> undefined | [#raft_identity{}].
membership(Service) ->
    case proplists:get_value(config, status(Service), undefined) of
        undefined -> undefined;
        Config    -> get_config_members(Config)
    end.

%%------------------------------------------------------------------------------
%% RAFT Server - Internal APIs - Local Options
%%------------------------------------------------------------------------------

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

%%------------------------------------------------------------------------------
%% RAFT Server - Internal APIs - RPC Handling
%%------------------------------------------------------------------------------

-spec make_rpc(Self :: #raft_identity{}, Term :: wa_raft_log:log_term(), Procedure :: normalized_procedure()) -> rpc().
make_rpc(#raft_identity{name = Name, node = Node}, Term, ?PROCEDURE(Procedure, Payload)) ->
    % For compatibility with legacy versions that expect RPCs sent with no arguments to have payload 'undefined' instead of {}.
    PayloadOrUndefined = case Payload of
        {} -> undefined;
        _  -> Payload
    end,
    ?RAFT_NAMED_RPC(Procedure, Term, Name, Node, PayloadOrUndefined).

-spec parse_rpc(Self :: #raft_identity{}, RPC :: rpc()) -> {Term :: wa_raft_log:log_term(), Sender :: #raft_identity{}, Procedure :: procedure()}.
parse_rpc(_, ?RAFT_NAMED_RPC(Key, Term, SenderName, SenderNode, PayloadOrUndefined)) ->
    Payload = case PayloadOrUndefined of
        undefined -> {};
        _         -> PayloadOrUndefined
    end,
    #{Key := ?PROCEDURE(Procedure, Defaults)} = protocol(),
    {Term, #raft_identity{name = SenderName, node = SenderNode}, ?PROCEDURE(Procedure, defaultize_payload(Defaults, Payload))};
parse_rpc(#raft_identity{name = Name} = Self, ?LEGACY_RAFT_RPC(Procedure, Term, SenderId, Payload)) ->
    parse_rpc(Self, ?RAFT_NAMED_RPC(Procedure, Term, Name, SenderId, Payload)).

%%------------------------------------------------------------------------------
%% RAFT Server - Internal APIs - Commands
%%------------------------------------------------------------------------------

-spec commit(
    Server :: gen_statem:server_ref(),
    Op :: wa_raft_acceptor:op()
) -> ok | wa_raft:error().
commit(Server, Op) ->
    gen_statem:cast(Server, ?COMMIT_COMMAND(Op)).

-spec read(
    Server :: gen_statem:server_ref(),
    Op :: wa_raft_acceptor:read_op()
) -> ok | wa_raft:error().
read(Server, Op) ->
    gen_statem:cast(Server, ?READ_COMMAND(Op)).

-spec snapshot_available(
    Server :: gen_statem:server_ref(),
    Root :: file:filename(),
    Position :: wa_raft_log:log_pos()
) -> ok | wa_raft:error().
snapshot_available(Server, Root, Position) ->
    % Use the storage call timeout because this command requires the RAFT
    % server to make a potentially expensive call against the RAFT storage
    % server to complete.
    gen_statem:call(Server, ?SNAPSHOT_AVAILABLE_COMMAND(Root, Position), ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec adjust_membership(
    Server :: gen_statem:server_ref(),
    Action :: add | remove | add_witness | remove_witness,
    Peer :: peer()
) -> {ok, Position :: wa_raft_log:log_pos()} | wa_raft:error().
adjust_membership(Server, Action, Peer) ->
    adjust_membership(Server, Action, Peer, undefined).

-spec adjust_membership(
    Server :: gen_statem:server_ref(),
    Action :: add | remove | add_witness | remove_witness,
    Peer :: peer(),
    ConfigIndex :: wa_raft_log:log_index() | undefined
) -> {ok, Position :: wa_raft_log:log_pos()} | wa_raft:error().
adjust_membership(Server, Action, Peer, ConfigIndex) ->
    gen_statem:call(Server, ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer, ConfigIndex), ?RAFT_RPC_CALL_TIMEOUT()).

%% Request the specified RAFT server to start an election in the next term.
-spec trigger_election(Server :: gen_statem:server_ref()) -> ok | wa_raft:error().
trigger_election(Server) ->
    trigger_election(Server, current).

%% Request the specified RAFT server to trigger a new election in the term *after* the specified term.
-spec trigger_election(Server :: gen_statem:server_ref(), Term :: term_or_offset()) -> ok | wa_raft:error().
trigger_election(Server, Term) ->
    gen_statem:call(Server, ?TRIGGER_ELECTION_COMMAND(Term), ?RAFT_RPC_CALL_TIMEOUT()).

%% Request the specified RAFT server to promote itself to leader of the specified term.
-spec promote(Server :: gen_statem:server_ref(), Term :: term_or_offset()) -> ok | wa_raft:error().
promote(Server, Term) ->
    promote(Server, Term, false).

-spec promote(Server :: gen_statem:server_ref(), Term :: term_or_offset(), Force :: boolean()) -> ok | wa_raft:error().
promote(Server, Term, Force) ->
    gen_statem:call(Server, ?PROMOTE_COMMAND(Term, Force), ?RAFT_RPC_CALL_TIMEOUT()).

-spec resign(Server :: gen_statem:server_ref()) -> ok | wa_raft:error().
resign(Server) ->
    gen_statem:call(Server, ?RESIGN_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

%% Instruct a RAFT leader to attempt a handover to a random handover candidate.
-spec handover(Server :: gen_statem:server_ref()) -> ok.
handover(Server) ->
    gen_statem:cast(Server, ?HANDOVER_COMMAND(undefined)).

%% Instruct a RAFT leader to attempt a handover to the specified peer node.
%% If an `undefined` peer node is specified, then handover to a random handover candidate.
%% Returns which peer node the handover was sent to or otherwise an error.
-spec handover(Server :: gen_statem:server_ref(), Peer :: node() | undefined) -> {ok, Peer :: node()} | wa_raft:error().
handover(Server, Peer) ->
    gen_statem:call(Server, ?HANDOVER_COMMAND(Peer), ?RAFT_RPC_CALL_TIMEOUT()).

-spec handover_candidates(Server :: gen_statem:server_ref()) -> {ok, Candidates :: [node()]} | wa_raft:error().
handover_candidates(Server) ->
    gen_statem:call(Server, ?HANDOVER_CANDIDATES_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

-spec disable(Server :: gen_statem:server_ref(), Reason :: term()) -> ok | {error, ErrorReason :: atom()}.
disable(Server, Reason) ->
    gen_statem:call(Server, ?DISABLE_COMMAND(Reason), ?RAFT_RPC_CALL_TIMEOUT()).

-spec enable(Server :: gen_statem:server_ref()) -> ok | {error, ErrorReason :: atom()}.
enable(Server) ->
    gen_statem:call(Server, ?ENABLE_COMMAND, ?RAFT_RPC_CALL_TIMEOUT()).

-spec bootstrap(
    Server :: gen_statem:server_ref(),
    Position :: wa_raft_log:log_pos(),
    Config :: config(),
    Data :: dynamic()
) -> ok | wa_raft:error().
bootstrap(Server, Position, Config, Data) ->
    gen_statem:call(Server, ?BOOTSTRAP_COMMAND(Position, Config, Data), ?RAFT_STORAGE_CALL_TIMEOUT()).

-spec notify_complete(Server :: gen_statem:server_ref()) -> ok.
notify_complete(Server) ->
    gen_statem:cast(Server, ?NOTIFY_COMPLETE_COMMAND()).

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - General Callbacks
%%------------------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> gen_statem:init_result(state()).
init(
    #raft_options{
        application = Application,
        table = Table,
        partition = Partition,
        self = Self,
        identifier = Identifier,
        database = PartitionPath,
        label_module = LabelModule,
        distribution_module = DistributionModule,
        log_name = Log,
        log_catchup_name = Catchup,
        server_name = Name,
        storage_name = Storage
    } = Options
) ->
    process_flag(trap_exit, true),

    ?LOG_NOTICE("Server[~0p] starting with options ~0p", [Name, Options], ?RAFT_LOG_OPTS),

    % This increases the potential overhead of sending messages to server;
    % however, can protect the server from GC overhead
    % and other memory-related issues (most notably when receiving log entries
    % when undergoing a fast log catchup).
    ?RAFT_CONFIG(raft_server_message_queue_off_heap, true) andalso
        process_flag(message_queue_data, off_heap),

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
        partition_path = PartitionPath,
        log_view = View,
        label_module = LabelModule,
        last_label = undefined,
        distribution_module = DistributionModule,
        storage = Storage,
        catchup = Catchup,
        commit_index = Last#raft_log_pos.index,
        last_applied = Last#raft_log_pos.index,
        current_term = Last#raft_log_pos.term,
        state_start_ts = Now
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
    % 2. Begin as stalled if there is no data
    % 3. Begin as witness if configured
    % 4. Begin as follower otherwise
    case {State2#raft_state.last_applied, State2#raft_state.disable_reason} of
        {0, undefined}    -> {ok, stalled, State2};
        {_, undefined}    -> {ok, follower_or_witness_state(State2), State2};
        {_, _}            -> {ok, disabled, State2}
    end.

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() ->
    [state_functions, state_enter].

-spec terminate(Reason :: term(), State :: state(), Data :: #raft_state{}) -> ok.
terminate(Reason, State, #raft_state{table = Table, partition = Partition} = Data) ->
    ?RAFT_LOG_NOTICE(State, Data, "terminating due to ~0P", [Reason, 20]),
    wa_raft_durable_state:sync(Data),
    wa_raft_info:delete_state(Table, Partition),
    wa_raft_info:set_stale(Table, Partition, true),
    ok.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Procedure Call Marshalling
%%------------------------------------------------------------------------------

%% A macro that destructures the identity record indicating that the
%% relevant procedure should be refactored to treat identities
%% opaquely.
-define(IDENTITY_REQUIRES_MIGRATION(Name, Node), #raft_identity{name = Name, node = Node}).

-type remote(Call) :: ?REMOTE(#raft_identity{}, Call).
-type procedure()  :: ?PROCEDURE(atom(), tuple()).

-type normalized_procedure() :: append_entries() | append_entries_response() | request_vote() | vote() | handover() | handover_failed() | notify_term().
-type append_entries()          :: ?APPEND_ENTRIES         (wa_raft_log:log_index(), wa_raft_log:log_term(), [wa_raft_log:log_entry()], wa_raft_log:log_index(), wa_raft_log:log_index()).
-type append_entries_response() :: ?APPEND_ENTRIES_RESPONSE(wa_raft_log:log_index(), boolean(), wa_raft_log:log_index(), wa_raft_log:log_index() | undefined).
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
        ?APPEND_ENTRIES_RESPONSE => ?APPEND_ENTRIES_RESPONSE(0, false, 0, undefined),
        ?REQUEST_VOTE            => ?REQUEST_VOTE(normal, 0, 0),
        ?VOTE                    => ?VOTE(false),
        ?HANDOVER                => ?HANDOVER(undefined, 0, 0, []),
        ?HANDOVER_FAILED         => ?HANDOVER_FAILED(undefined)
    }.

-spec handle_rpc(
    Type :: gen_statem:event_type(),
    RPC :: rpc(),
    State :: state(),
    Data :: #raft_state{}
) -> gen_statem:event_handler_result(state(), #raft_state{}).

handle_rpc(Type, ?RAFT_NAMED_RPC(Procedure, Term, SenderName, SenderNode, Payload) = Event, State, #raft_state{} = Data) ->
    handle_rpc_impl(Type, Event, Procedure, Term, #raft_identity{name = SenderName, node = SenderNode}, Payload, State, Data);
handle_rpc(Type, ?LEGACY_RAFT_RPC(Procedure, Term, SenderId, Payload) = Event, State, #raft_state{name = Name} = Data) ->
    handle_rpc_impl(Type, Event, Procedure, Term, #raft_identity{name = Name, node = SenderId}, Payload, State, Data);
handle_rpc(_, RPC, State, #raft_state{} = Data) ->
    ?RAFT_COUNT({'raft', State, 'rpc.unrecognized'}),
    ?RAFT_LOG_NOTICE(State, Data, "receives unknown RPC format ~0P", [RPC, 20]),
    keep_state_and_data.

-spec handle_rpc_impl(
    Type :: gen_statem:event_type(),
    Event :: rpc(),
    Key :: atom(),
    Term :: wa_raft_log:log_term(),
    Sender :: #raft_identity{},
    Payload :: undefined | tuple(),
    State :: state(),
    Data :: #raft_state{}
) -> gen_statem:event_handler_result(state(), #raft_state{}).

%% [Protocol] Undefined payload should be treated as an empty tuple
handle_rpc_impl(Type, Event, Key, Term, Sender, undefined, State, Data) ->
    handle_rpc_impl(Type, Event, Key, Term, Sender, {}, State, Data);
%% [General Rules] Discard any incoming RPCs with a term older than the current term
handle_rpc_impl(_, _, Key, Term, Sender, _, State, #raft_state{current_term = CurrentTerm} = Data) when Term < CurrentTerm ->
    ?RAFT_LOG_NOTICE(State, Data, "dropping stale ~0p from ~0p with old term ~0p.", [Key, Sender, Term]),
    State =/= disabled andalso send_rpc(Sender, ?NOTIFY_TERM(), Data),
    keep_state_and_data;
%% [RequestVote RPC] RAFT servers should ignore vote requests with reason `normal`
%%                   if it knows about a currently active leader even if the vote
%%                   request has a newer term. A leader is only active if it is
%%                   replicating to peers so we check if we have recently received
%%                   a heartbeat. (4.2.3)
handle_rpc_impl(Type, Event, ?REQUEST_VOTE, Term, Sender, Payload, State,
                #raft_state{application = App, leader_heartbeat_ts = LeaderHeartbeatTs} = Data) when is_tuple(Payload), element(1, Payload) =:= normal ->
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
            ?RAFT_LOG_DEBUG(State, Data, "dropping normal vote request from ~p because leader was still active ~p ms ago (allowed ~p ms).",
                [Sender, Delay, AllowedDelay]),
            keep_state_and_data
    end;
%% [General Rules] Advance to the newer term and reset state when seeing a newer term in an incoming RPC
handle_rpc_impl(Type, Event, _, Term, _, _, _, #raft_state{current_term = CurrentTerm}) when Term > CurrentTerm ->
    {keep_state_and_data, [{next_event, internal, ?ADVANCE_TERM(Term)}, {next_event, Type, Event}]};
%% [NotifyTerm RPC] Drop NotifyTerm RPCs with matching term
handle_rpc_impl(_, _, ?NOTIFY_TERM, _, _, _, _, #raft_state{}) ->
    keep_state_and_data;
%% [Protocol] Convert any valid remote procedure call to the appropriate local procedure call.
handle_rpc_impl(Type, _, Key, _, Sender, Payload, State, #raft_state{} = Data) when is_tuple(Payload) ->
    case protocol() of
        #{Key := ?PROCEDURE(Procedure, Defaults)} ->
            handle_procedure(Type, ?REMOTE(Sender, ?PROCEDURE(Procedure, defaultize_payload(Defaults, Payload))), State, Data);
        #{} ->
            ?RAFT_COUNT({'raft', State, 'rpc.unknown'}),
            ?RAFT_LOG_DEBUG(State, Data, "receives unknown RPC type ~0p with payload ~0P", [Key, Payload, 25]),
            keep_state_and_data
    end.

-spec handle_procedure(
    Type :: gen_statem:event_type(),
    ProcedureCall :: remote(procedure()),
    State :: state(),
    Data :: #raft_state{}
) -> gen_statem:event_handler_result(state(), #raft_state{}).

%% [AppendEntries RPC] If we haven't discovered leader for this term, record it
handle_procedure(Type, ?REMOTE(Sender, ?APPEND_ENTRIES(_, _, _, _, _)) = Procedure, State, #raft_state{leader_id = undefined} = Data) ->
    {keep_state, set_leader(State, Sender, Data), {next_event, Type, Procedure}};
%% [Handover][Handover RPC] If we haven't discovered leader for this term, record it
handle_procedure(Type, ?REMOTE(Sender, ?HANDOVER(_, _, _, _)) = Procedure, State, #raft_state{leader_id = undefined} = Data) ->
    {keep_state, set_leader(State, Sender, Data), {next_event, Type, Procedure}};
handle_procedure(Type, Procedure, _, #raft_state{}) ->
    {keep_state_and_data, {next_event, Type, Procedure}}.

-spec defaultize_payload(Defaults :: tuple(), Payload :: tuple()) -> tuple().
defaultize_payload(Defaults, Payload) ->
    defaultize_payload(Defaults, Payload, tuple_size(Defaults), tuple_size(Payload)).

-spec defaultize_payload(tuple(), tuple(), non_neg_integer(), non_neg_integer()) -> tuple().
defaultize_payload(_, Payload, N, N) ->
    Payload;
defaultize_payload(Defaults, Payload, N, M) when N > M ->
    defaultize_payload(Defaults, erlang:insert_element(M + 1, Payload, element(M + 1, Defaults)), N, M + 1);
defaultize_payload(Defaults, Payload, N, M) when N < M ->
    defaultize_payload(Defaults, erlang:delete_element(M, Payload), N, M - 1).

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Stalled State
%%------------------------------------------------------------------------------
%% The stalled state is an extension to the RAFT protocol designed to handle
%% situations in which a replica of the FSM is lost or replaced within a RAFT
%% cluster without being removed from the cluster membership. As the replica of
%% the FSM stored before the machine was lost or replaced could have been a
%% critical member of a quorum, it is important to ensure that the replacement
%% does not support a different result for any quorums before it receives a
%% fresh copy of the FSM state and log that is guaranteed to reflect any
%% quorums that the machine supported before it was lost or replaced.
%%
%% This is acheived by preventing a stalled node from participating in quorum
%% for both log entries and election. A leader of the cluster must provide a
%% fresh copy of its FSM state before the stalled node can return to normal
%% operation.
%%------------------------------------------------------------------------------

-spec stalled
    (
        Type :: enter,
        PreviousStateName :: state(),
        Data :: #raft_state{}
    ) -> gen_statem:state_enter_result(state(), #raft_state{});
    (
        Type :: gen_statem:event_type(),
        Event :: event(),
        Data :: #raft_state{}
    ) -> gen_statem:event_handler_result(state(), #raft_state{}).

stalled(enter, PreviousStateName, #raft_state{} = State) ->
    ?RAFT_COUNT('raft.stalled.enter'),
    ?RAFT_LOG_NOTICE(State, "becomes stalled from state ~0p.", [PreviousStateName]),
    {keep_state, enter_state(?FUNCTION_NAME, State)};

%% [Internal] Advance to newer term when requested
stalled(internal, ?ADVANCE_TERM(NewTerm), #raft_state{current_term = CurrentTerm} = State) when NewTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.stalled.advance_term'),
    ?RAFT_LOG_NOTICE(State, "advancing to new term ~0p.", [NewTerm]),
    {repeat_state, advance_term(?FUNCTION_NAME, NewTerm, undefined, State)};

%% [Protocol] Parse any RPCs in network formats
stalled(Type, Event, #raft_state{} = State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Stalled nodes always discard heartbeats
stalled(_, ?REMOTE(Sender, ?APPEND_ENTRIES(PrevLogIndex, _, _, _, _)), #raft_state{} = State) ->
    NewState = State#raft_state{leader_heartbeat_ts = erlang:monotonic_time(millisecond)},
    send_rpc(Sender, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, false, 0, 0), NewState),
    {keep_state, NewState};

stalled({call, From}, ?TRIGGER_ELECTION_COMMAND(_), #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "cannot start an election.", []),
    {keep_state_and_data, {reply, From, {error, invalid_state}}};

stalled({call, From}, ?PROMOTE_COMMAND(_, _), #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "cannot be promoted to leader.", []),
    {keep_state_and_data, {reply, From, {error, invalid_state}}};

stalled(
    {call, From},
    ?BOOTSTRAP_COMMAND(#raft_log_pos{index = Index, term = Term} = Position, Config, Data),
    #raft_state{
        self = Self,
        partition_path = PartitionPath,
        storage = Storage,
        current_term = CurrentTerm,
        last_applied = LastApplied
    } = State0
) ->
    case LastApplied =:= 0 of
        true ->
            ?RAFT_LOG_NOTICE(State0, "attempting bootstrap at ~0p:~0p with config ~0p and data ~0P.", [Index, Term, Config, Data, 30]),
            Path = filename:join(PartitionPath, io_lib:format("snapshot.~0p.~0p.bootstrap.tmp", [Index, Term])),
            try
                ok = wa_raft_storage:make_empty_snapshot(Storage, Path, Position, Config, Data),
                State1 = open_snapshot(Path, Position, State0),
                AdjustedTerm = max(1, Term),
                case AdjustedTerm > CurrentTerm of
                    true ->
                        case is_single_member(Self, config(State1)) of
                            true ->
                                State2 = advance_term(?FUNCTION_NAME, AdjustedTerm, node(), State1),
                                ?RAFT_LOG_NOTICE(State2, "switching to leader as sole member after successful bootstrap.", []),
                                {next_state, leader, State2, {reply, From, ok}};
                            false ->
                                State2 = advance_term(?FUNCTION_NAME, AdjustedTerm, undefined, State1),
                                ?RAFT_LOG_NOTICE(State2, "switching to follower after successful bootstrap.", []),
                                {next_state, follower_or_witness_state(State2), State2, {reply, From, ok}}
                        end;
                    false ->
                        ?RAFT_LOG_NOTICE(State1, "switching to follower after successful bootstrap.", []),
                        {next_state, follower_or_witness_state(State1), State1, {reply, From, ok}}
                end
            catch
                _:Reason ->
                    ?RAFT_LOG_WARNING(State0, "failed to bootstrap due to ~0P.", [Reason, 20]),
                    {keep_state_and_data, {reply, From, {error, Reason}}}
            after
                catch file:del_dir_r(Path)
            end;
        false ->
            ?RAFT_LOG_NOTICE(State0, "at ~0p rejecting request to bootstrap with data.", [LastApplied, Index, Term]),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;

stalled(
    Type,
    ?SNAPSHOT_AVAILABLE_COMMAND(Root, #raft_log_pos{index = SnapshotIndex, term = SnapshotTerm} = SnapshotPos),
    #raft_state{
        current_term = CurrentTerm,
        last_applied = LastApplied
    } = State0
) ->
    case SnapshotIndex > LastApplied orelse LastApplied =:= 0 of
        true ->
            try
                ?RAFT_LOG_NOTICE(State0, "applying snapshot at ~0p:~0p.", [SnapshotIndex, SnapshotTerm]),
                State1 = open_snapshot(Root, SnapshotPos, State0),
                State2 = case SnapshotTerm > CurrentTerm of
                    true -> advance_term(?FUNCTION_NAME, SnapshotTerm, undefined, State1);
                    false -> State1
                end,
                % At this point, we assume that we received some cluster membership configuration from
                % our peer so it is safe to transition to an operational state.
                reply(Type, ok),
                {next_state, follower_or_witness_state(State2), State2}
            catch
                _:Reason ->
                    ?RAFT_LOG_WARNING(State0, "failed to load available snapshot ~0p due to ~0P", [Root, Reason, 20]),
                    reply(Type, {error, Reason}),
                    keep_state_and_data
            end;
        false ->
            ?RAFT_LOG_NOTICE(State0, "at ~0p ignoring old snapshot at ~0p:~0p", [LastApplied, SnapshotIndex, SnapshotTerm]),
            reply(Type, {error, rejected}),
            keep_state_and_data
    end;

%% [Command] Defer to common handling for generic RAFT server commands
stalled(Type, ?RAFT_COMMAND(_, _) = Event, #raft_state{} = State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Fallback] Report unhandled events
stalled(Type, Event, #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "did not know how to handle ~0p event ~0P", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Leader State
%%------------------------------------------------------------------------------
%% In a RAFT cluster, the leader of a RAFT term is a replica that has received
%% a quorum of votes from the cluster in that RAFT term, establishing it as the
%% unique coordinator for that RAFT term. The leader is responsible for
%% accepting and replicating new log entries to progress the state of the FSM.
%%------------------------------------------------------------------------------

-spec leader
    (
        Type :: enter,
        PreviousStateName :: state(),
        Data :: #raft_state{}
    ) -> gen_statem:state_enter_result(state(), #raft_state{});
    (
        Type :: gen_statem:event_type(),
        Event :: event(),
        Data :: #raft_state{}
    ) -> gen_statem:event_handler_result(state(), #raft_state{}).

leader(enter, PreviousStateName, #raft_state{self = Self, log_view = View0} = State0) ->
    ?RAFT_COUNT('raft.leader.enter'),
    ?RAFT_COUNT('raft.leader.elected'),
    ?RAFT_LOG_NOTICE(State0, "becomes leader from state ~0p.", [PreviousStateName]),

    % Setup leader state and announce leadership
    State1 = enter_state(?FUNCTION_NAME, State0),
    State2 = set_leader(?FUNCTION_NAME, Self, State1),

    % Attempt to refresh the label state as necessary for new log entries
    LastLogIndex = wa_raft_log:last_index(View0),
    State3 = case wa_raft_log:get(View0, LastLogIndex) of
        {ok, {_, {_, LastLabel, _}}} ->
            State2#raft_state{last_label = LastLabel};
        {ok, {_, undefined}} ->
            % The RAFT log could have been reset (i.e. after snapshot installation).
            % In such case load the log label state from storage.
            LastLabel = load_label_state(State2),
            State2#raft_state{last_label = LastLabel};
        {ok, _} ->
            State2#raft_state{last_label = undefined}
    end,

    % At the start of a new term, the leader should append a new log
    % entry that will start the process of establishing the first
    % quorum in the new term by starting replication and clearing out
    % any log mismatches on follower replicas.
    {State4, LogEntry} = get_log_entry(State3, {make_ref(), noop}),
    {ok, NewLastLogIndex, View1} = wa_raft_log:append(View0, [LogEntry]),
    State5 = State4#raft_state{log_view = View1, first_current_term_log_index = NewLastLogIndex},

    % Perform initial heartbeat and log entry resolution
    State6 = append_entries_to_followers(State5),
    State7 = apply_single_node_cluster(State6), % apply immediately for single node cluster
    {keep_state, State7, ?HEARTBEAT_TIMEOUT(State7)};

%% [Internal] Advance to newer term when requested
leader(
    internal,
    ?ADVANCE_TERM(NewTerm),
    #raft_state{log_view = View, current_term = CurrentTerm} = State
) when NewTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.leader.advance_term'),
    ?RAFT_LOG_NOTICE(State, "advancing to new term ~0p.", [NewTerm]),
    %% Drop any pending log entries queued in the log view before advancing
    {ok, _, NewView} = wa_raft_log:cancel(View),
    State1 = advance_term(?FUNCTION_NAME, NewTerm, undefined, State#raft_state{log_view = NewView}),
    {next_state, follower_or_witness_state(State1), State1};

%% [Protocol] Parse any RPCs in network formats
leader(Type, Event, #raft_state{} = State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Leaders should not act upon any incoming heartbeats (5.1, 5.2)
leader(_, ?REMOTE(_, ?APPEND_ENTRIES(_, _, _, _, _)), #raft_state{}) ->
    keep_state_and_data;

%% [Leader] Handle AppendEntries RPC responses (5.2, 5.3, 7).
%% Handle normal-case successes
leader(
    cast,
    ?REMOTE(
        ?IDENTITY_REQUIRES_MIGRATION(_, FollowerId) = Sender,
        ?APPEND_ENTRIES_RESPONSE(_, true, FollowerMatchIndex, FollowerLastAppliedIndex)
    ),
    #raft_state{
        commit_index = CommitIndex,
        next_indices = NextIndices,
        match_indices = MatchIndices,
        last_applied_indices = LastAppliedIndices,
        heartbeat_response_ts = HeartbeatResponse0,
        first_current_term_log_index = TermStartIndex
    } = State0
) ->
    StartT = os:timestamp(),
    ?RAFT_LOG_DEBUG(State0, "at commit index ~0p completed append to ~0p whose log now matches up to ~0p.",
        [CommitIndex, Sender, FollowerMatchIndex]),
    HeartbeatResponse1 = HeartbeatResponse0#{FollowerId => erlang:monotonic_time(millisecond)},
    State1 = State0#raft_state{heartbeat_response_ts = HeartbeatResponse1},

    case select_follower_replication_mode(FollowerMatchIndex, State1) of
        bulk_logs -> request_bulk_logs_for_follower(Sender, FollowerMatchIndex, State1);
        _         -> cancel_bulk_logs_for_follower(Sender, State1)
    end,

    NextIndex = maps:get(FollowerId, NextIndices, TermStartIndex),
    NewMatchIndices = MatchIndices#{FollowerId => FollowerMatchIndex},
    NewNextIndices = NextIndices#{FollowerId => max(NextIndex, FollowerMatchIndex + 1)},
    NewLastAppliedIndices = case FollowerLastAppliedIndex of
        undefined -> LastAppliedIndices;
        _ -> LastAppliedIndices#{FollowerId => FollowerLastAppliedIndex}
    end,

    State2 = State1#raft_state{
        next_indices = NewNextIndices,
        match_indices = NewMatchIndices,
        last_applied_indices = NewLastAppliedIndices
    },
    State3 = maybe_advance(State2),
    State4 = apply_log_leader(State3),
    ?RAFT_GATHER('raft.leader.apply.func', timer:now_diff(os:timestamp(), StartT)),
    {keep_state, maybe_heartbeat(State4), ?HEARTBEAT_TIMEOUT(State4)};

%% and failures.
leader(
    cast,
    ?REMOTE(
        ?IDENTITY_REQUIRES_MIGRATION(_, FollowerId) = Sender,
        ?APPEND_ENTRIES_RESPONSE(_, false, FollowerEndIndex, FollowerLastAppliedIndex)
    ),
    #raft_state{
        commit_index = CommitIndex,
        next_indices = NextIndices,
        last_applied_indices = LastAppliedIndices
    } = State0
) ->
    ?RAFT_COUNT('raft.leader.append.failure'),
    ?RAFT_LOG_DEBUG(State0, "at commit index ~0p failed append to ~0p whose log now ends at ~0p.",
        [CommitIndex, Sender, FollowerEndIndex]),

    select_follower_replication_mode(FollowerEndIndex, State0) =:= snapshot andalso
        request_snapshot_for_follower(FollowerId, State0),
    cancel_bulk_logs_for_follower(Sender, State0),

    % We must trust the follower's last log index here because the follower may have
    % applied a snapshot since the last successful heartbeat. In such case, we need
    % to fast-forward the follower's next index so that we resume replication at the
    % point after the snapshot.
    NewNextIndices = NextIndices#{FollowerId => FollowerEndIndex + 1},
    NewLastAppliedIndices = case FollowerLastAppliedIndex of
        undefined -> LastAppliedIndices;
        _ -> LastAppliedIndices#{FollowerId => FollowerLastAppliedIndex}
    end,

    State1 = State0#raft_state{
        next_indices = NewNextIndices,
        last_applied_indices = NewLastAppliedIndices
    },
    State2 = apply_log_leader(State1),
    {keep_state, maybe_heartbeat(State2), ?HEARTBEAT_TIMEOUT(State2)};

%% [RequestVote RPC] Ignore any vote requests as leadership is aleady established (5.1, 5.2)
leader(_, ?REMOTE(_, ?REQUEST_VOTE(_, _, _)), #raft_state{}) ->
    keep_state_and_data;

%% [Vote RPC] We are already leader, so we don't need to consider any more votes (5.1)
leader(_, ?REMOTE(_, ?VOTE(_)), #raft_state{}) ->
    keep_state_and_data;

%% [Handover][Handover RPC] We are already leader so ignore any handover requests.
leader(_, ?REMOTE(Sender, ?HANDOVER(Reference, _, _, _)), #raft_state{} = State) ->
    send_rpc(Sender, ?HANDOVER_FAILED(Reference), State),
    keep_state_and_data;

%% [Handover][HandoverFailed RPC] Our handover failed, so clear the handover status.
leader(
    _,
    ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, NodeId) = Sender, ?HANDOVER_FAILED(Reference)),
    #raft_state{
        handover = {NodeId, Reference, _}
    } = State
) ->
    ?RAFT_LOG_NOTICE(State, "resuming normal operations after failed handover to ~0p.", [Sender]),
    {keep_state, State#raft_state{handover = undefined}};

%% [Handover][HandoverFailed RPC] Got a handover failed with an unknown ID. Ignore.
leader(_, ?REMOTE(_, ?HANDOVER_FAILED(_)), #raft_state{}) ->
    keep_state_and_data;

%% [Timeout] Suspend periodic heartbeat to followers while handover is active
leader(state_timeout = Type, Event, #raft_state{handover = {Peer, _, Timeout}} = State) ->
    NowMillis = erlang:monotonic_time(millisecond),
    case NowMillis > Timeout of
        true ->
            ?RAFT_LOG_NOTICE(State, "handover to ~0p times out.", [Peer]),
            {keep_state, State#raft_state{handover = undefined}, {next_event, Type, Event}};
        false ->
            {keep_state_and_data, ?HEARTBEAT_TIMEOUT(State)}
    end;

%% [Timeout] Periodic heartbeat to followers
leader(state_timeout, _, #raft_state{application = App, log_view = View} = State0) ->
    case ?RAFT_LEADER_ELIGIBLE(App) of
        true ->
            State1 = append_entries_to_followers(State0),
            State2 = apply_single_node_cluster(State1),
            check_leader_lagging(State2),
            {keep_state, State1, ?HEARTBEAT_TIMEOUT(State2)};
        false ->
            ?RAFT_LOG_NOTICE(State0, "resigns from leadership because this node is ineligible.", []),
            %% Drop any pending log entries queued in the log view before resigning
            {ok, _, NewView} = wa_raft_log:cancel(View),
            State1 = clear_leader(?FUNCTION_NAME, State0#raft_state{log_view = NewView}),
            {next_state, follower_or_witness_state(State1), State1}
    end;

%% [Commit] If a handover is in progress, then try to redirect to handover target
leader(
    cast,
    ?COMMIT_COMMAND({Reference, _}),
    #raft_state{
        table = Table,
        partition = Partition,
        handover = {Peer, _, _}
    } = State
) ->
    ?RAFT_COUNT('raft.commit.handover'),
    wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Reference, {error, {notify_redirect, Peer}}), % Optimistically redirect to handover peer
    {keep_state, State};

%% [Commit] Otherwise, add a new commit to the RAFT log
leader(
    cast,
    ?COMMIT_COMMAND(Op),
    #raft_state{
        application = App,
        log_view = View0
    } = State0
) ->
    ?RAFT_COUNT('raft.commit'),
    {State1, LogEntry} = get_log_entry(State0, Op),
    {ok, View1} = wa_raft_log:submit(View0, LogEntry),
    Pending = wa_raft_log:pending(View1),
    State2 = apply_single_node_cluster(State1#raft_state{log_view = View1}), % apply immediately for single node cluster

    case ?RAFT_COMMIT_BATCH_INTERVAL(App) > 0 andalso Pending =< ?RAFT_COMMIT_BATCH_MAX_ENTRIES(App) of
        true ->
            ?RAFT_COUNT('raft.commit.batch.delay'),
            {keep_state, State2, ?COMMIT_BATCH_TIMEOUT(State2)};
        false ->
            State3 = append_entries_to_followers(State2),
            {keep_state, State3, ?HEARTBEAT_TIMEOUT(State3)}
    end;

%% [Strong Read] If a handover is in progress, then try to redirect to handover target
leader(
    cast,
    ?READ_COMMAND({From, _}),
    #raft_state{
        table = Table,
        partition = Partition,
        handover = {Peer, _, _}
    } = State
) ->
    ?RAFT_COUNT('raft.read.handover'),
    wa_raft_queue:fulfill_incomplete_read(Table, Partition, From, {error, {notify_redirect, Peer}}), % Optimistically redirect to handover peer
    {keep_state, State};

%% [Strong Read] Leader is eligible to serve strong reads.
leader(
    cast,
    ?READ_COMMAND({From, Command}),
    #raft_state{
        self = Self,
        table = Table,
        partition = Partition,
        log_view = View0,
        storage = Storage,
        commit_index = CommitIndex,
        last_applied = LastApplied,
        first_current_term_log_index = FirstLogIndex
    } = State0
) ->
    LastLogIndex = wa_raft_log:last_index(View0),
    Pending = wa_raft_log:pending(View0),
    ReadIndex = max(CommitIndex, FirstLogIndex),
    case is_single_member(Self, config(State0)) of
        % If we are a single node cluster and we are fully-applied, then immediately dispatch.
        true when Pending =:= 0, ReadIndex =:= LastApplied ->
            wa_raft_storage:apply_read(Storage, From, Command),
            {keep_state, State0};
        _ ->
            ok = wa_raft_queue:submit_read(Table, Partition, ReadIndex + 1, From, Command),
            State2 = case ReadIndex < LastLogIndex orelse Pending > 0 of
                true  ->
                    State0;
                % TODO(hsun324): Try to reuse the commit code to deal with placeholder ops so we
                % handle batching and timeout properly instead of relying on a previously set timeout.
                false ->
                    {State1, LogEntry} = get_log_entry(State0, {?READ_OP, noop}),
                    {ok, View1} = wa_raft_log:submit(View0, LogEntry),
                    State1#raft_state{log_view = View1}
            end,
            {keep_state, State2}
    end;

%% [Resign] Leader resigns by switching to follower state.
leader({call, From}, ?RESIGN_COMMAND, #raft_state{log_view = View} = State) ->
    ?RAFT_LOG_NOTICE(State, "resigns.", []),

    %% Drop any pending log entries queued in the log view before resigning
    {ok, _, NewView} = wa_raft_log:cancel(View),
    State1 = clear_leader(?FUNCTION_NAME, State#raft_state{log_view = NewView}),
    {next_state, follower_or_witness_state(State1), State1, {reply, From, ok}};

%% [Adjust Membership] Leader attempts to commit a single-node membership change.
leader(
    Type,
    ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer, ExpectedConfigIndex),
    #raft_state{
        log_view = View0,
        storage = Storage,
        current_term = CurrentTerm,
        commit_index = CommitIndex,
        first_current_term_log_index = TermStartIndex
    } = State0
) ->
    % Try to adjust the configuration according to the current request.
    Config = config(State0),
    % eqwalizer:ignore Peer can be undefined
    case adjust_config({Action, Peer}, Config, State0) of
        {ok, NewConfig} ->
            % Ensure that we have committed at least one log entry in the current
            % term before admitting any membership change operations.
            case CommitIndex >= TermStartIndex of
                true ->
                    % Ensure that there is no as-of-yet uncomitted config.
                    StorageConfigIndex = case wa_raft_storage:config(Storage) of
                        {ok, #raft_log_pos{index = SI}, _} -> SI;
                        undefined                          -> 0
                    end,
                    LogConfigIndex = case wa_raft_log:config(View0) of
                        {ok, LI, _} -> LI;
                        not_found   -> 0
                    end,
                    ConfigIndex = max(StorageConfigIndex, LogConfigIndex),
                    case ConfigIndex > CommitIndex of
                        true ->
                            ?RAFT_LOG_NOTICE(State0, "at ~0p refusing to ~0p peer ~0p because it has a pending reconfiguration (storage: ~0p, log: ~0p).",
                                [CommitIndex, Action, Peer, StorageConfigIndex, LogConfigIndex]),
                            reply(Type, {error, rejected}),
                            {keep_state, State0};
                        false ->
                            case ExpectedConfigIndex =/= undefined andalso ExpectedConfigIndex =/= ConfigIndex of
                                true ->
                                    ?RAFT_LOG_NOTICE(State0, "refusing to ~0p peer ~0p because it has a different config index than expected (storage: ~0p, log: ~0p expected: ~0p).",
                                        [Action, Peer, StorageConfigIndex, LogConfigIndex, ExpectedConfigIndex]),
                                    reply(Type, {error, rejected}),
                                    {keep_state, State0};
                                false ->
                                    % Now that we have completed all the required checks, the leader is free to
                                    % attempt to change the config. We heartbeat immediately to make the change as
                                    % soon as possible.
                                    Op = {make_ref(), {config, NewConfig}},
                                    {State1, LogEntry} = get_log_entry(State0, Op),
                                    {ok, LogIndex, View1} = wa_raft_log:append(View0, [LogEntry]),
                                    ?RAFT_LOG_NOTICE(State1, "appended configuration change from ~0p to ~0p at log index ~0p.", [Config, NewConfig, LogIndex]),
                                    State2 = State1#raft_state{log_view = View1},
                                    State3 = apply_single_node_cluster(State2),
                                    State4 = append_entries_to_followers(State3),
                                    reply(Type, {ok, #raft_log_pos{index = LogIndex, term = CurrentTerm}}),
                                    {keep_state, State4, ?HEARTBEAT_TIMEOUT(State4)}
                            end
                    end;
                false ->
                    ?RAFT_LOG_NOTICE(State0, "refusing to ~0p peer ~0p because it has not established current term commit quorum.", [Action, Peer]),
                    reply(Type, {error, rejected}),
                    {keep_state, State0}
            end;
        {error, Reason} ->
            ?RAFT_LOG_NOTICE(State0, "refusing to ~0p peer ~0p on configuration ~0p due to ~0p.", [Action, Peer, Config, Reason]),
            reply(Type, {error, Reason}),
            {keep_state, State0}
    end;

%% [Handover Candidates] Return list of handover candidates (peers that are not lagging too much)
leader({call, From}, ?HANDOVER_CANDIDATES_COMMAND, #raft_state{} = State) ->
    {keep_state_and_data, {reply, From, {ok, compute_handover_candidates(State)}}};

%% [Handover] With peer 'undefined' randomly select a valid candidate to handover to
leader(Type, ?HANDOVER_COMMAND(undefined), #raft_state{} = State) ->
    case compute_handover_candidates(State) of
        [] ->
            ?RAFT_LOG_NOTICE(State, "has no valid peer to handover to.", []),
            reply(Type, {error, no_valid_peer}),
            keep_state_and_data;
        Candidates ->
            Peer = lists:nth(rand:uniform(length(Candidates)), Candidates),
            leader(Type, ?HANDOVER_COMMAND(Peer), State)
    end;

%% [Handover] Handover to self results in no-op
leader(Type, ?HANDOVER_COMMAND(Peer), #raft_state{} = State) when Peer =:= node() ->
    ?RAFT_LOG_WARNING(State, "dropping handover to self.", []),
    reply(Type, {ok, Peer}),
    {keep_state, State};

%% [Handover] Attempt to start a handover to the specified peer
leader(
    Type,
    ?HANDOVER_COMMAND(Peer),
    #raft_state{
        application = App,
        name = Name,
        log_view = View,
        match_indices = MatchIndices,
        handover = undefined
    } = State0
) ->
    % TODO(hsun324): For the time being, assume that all members of the cluster use the same server name.
    case member({Name, Peer}, config(State0)) of
        false ->
            ?RAFT_LOG_WARNING(State0, "dropping handover to unknown peer ~0p.", [Peer]),
            reply(Type, {error, invalid_peer}),
            keep_state_and_data;
        true ->
            PeerMatchIndex = maps:get(Peer, MatchIndices, 0),
            FirstIndex = wa_raft_log:first_index(View),
            PeerSendIndex = max(PeerMatchIndex + 1, FirstIndex + 1),
            LastIndex = wa_raft_log:last_index(View),
            MaxHandoverBatchSize = ?RAFT_HANDOVER_MAX_ENTRIES(App),
            MaxHandoverBytes = ?RAFT_HANDOVER_MAX_BYTES(App),

            case LastIndex - PeerSendIndex =< MaxHandoverBatchSize of
                true ->
                    ?RAFT_COUNT('raft.leader.handover'),
                    ?RAFT_LOG_NOTICE(State0, "starting handover to ~p.", [Peer]),

                    PrevLogIndex = PeerSendIndex - 1,
                    {ok, PrevLogTerm} = wa_raft_log:term(View, PrevLogIndex),
                    {ok, LogEntries} = wa_raft_log:get(View, PeerSendIndex, MaxHandoverBatchSize, MaxHandoverBytes),

                    % The request to load the log may result in not all required log entries being loaded
                    % if we hit the byte size limit. Ensure that we have loaded all required log entries
                    % before initiating a handover.
                    case PrevLogIndex + length(LogEntries) of
                        LastIndex ->
                            Ref = make_ref(),
                            Timeout = erlang:monotonic_time(millisecond) + ?RAFT_HANDOVER_TIMEOUT(App),
                            State1 = State0#raft_state{handover = {Peer, Ref, Timeout}},
                            send_rpc(?IDENTITY_REQUIRES_MIGRATION(Name, Peer), ?HANDOVER(Ref, PrevLogIndex, PrevLogTerm, LogEntries), State1),
                            reply(Type, {ok, Peer}),
                            {keep_state, State1};
                        _ ->
                            ?RAFT_COUNT('raft.leader.handover.oversize'),
                            ?RAFT_LOG_WARNING(State0, "handover to peer ~0p would require an oversized RPC.", [Peer]),
                            reply(Type, {error, oversize}),
                            keep_state_and_data
                    end;
                false ->
                    ?RAFT_COUNT('raft.leader.handover.peer_lagging'),
                    ?RAFT_LOG_WARNING(State0, "determines that peer ~0p is not eligible for handover because it is ~0p entries behind.",
                        [Peer, LastIndex - PeerSendIndex]),
                    reply(Type, {error, peer_lagging}),
                    keep_state_and_data
            end
    end;

%% [Handover] Reject starting a handover when a handover is still in progress
leader({call, From}, ?HANDOVER_COMMAND(Peer), #raft_state{handover = {Node, _, _}} = State) ->
    ?RAFT_LOG_WARNING(State, "rejecting duplicate handover to ~0p with running handover to ~0p.", [Peer, Node]),
    {keep_state_and_data, {reply, From, {error, duplicate}}};

%% [Command] Defer to common handling for generic RAFT server commands
leader(Type, ?RAFT_COMMAND(_, _) = Event, #raft_state{} = State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Fallback] Report unhandled events
leader(Type, Event, #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "did not know how to handle ~0p event ~0P", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Follower State
%%------------------------------------------------------------------------------
%% In a RAFT cluster, a follower is a replica that is receiving replicated log
%% entries from the leader of a RAFT term. The follower participates in quorum
%% decisions about log entries received from the leader by appending those log
%% entries to its own local copy of the RAFT log.
%%------------------------------------------------------------------------------

-spec follower
    (
        Type :: enter,
        PreviousStateName :: state(),
        Data :: #raft_state{}
    ) -> gen_statem:state_enter_result(state(), #raft_state{});
    (
        Type :: gen_statem:event_type(),
        Event :: event(),
        Data :: #raft_state{}
    ) -> gen_statem:event_handler_result(state(), #raft_state{}).

follower(enter, PreviousStateName, #raft_state{} = State) ->
    ?RAFT_COUNT('raft.follower.enter'),
    ?RAFT_LOG_NOTICE(State, "becomes follower from state ~0p.", [PreviousStateName]),
    {keep_state, enter_state(?FUNCTION_NAME, State), ?ELECTION_TIMEOUT(State)};

%% [Internal] Advance to newer term when requested
follower(internal, ?ADVANCE_TERM(NewTerm), #raft_state{current_term = CurrentTerm} = State) when NewTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.follower.advance_term'),
    ?RAFT_LOG_NOTICE(State, "advancing to new term ~0p.", [NewTerm]),
    {repeat_state, advance_term(?FUNCTION_NAME, NewTerm, undefined, State)};

%% [Protocol] Parse any RPCs in network formats
follower(Type, Event, #raft_state{} = State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Handle incoming heartbeats (5.2, 5.3)
follower(
    Type,
    ?REMOTE(Leader, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex)),
    #raft_state{} = State
) ->
    handle_heartbeat(?FUNCTION_NAME, Type, Leader, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex, State);

%% [AppendEntriesResponse RPC] Followers should not act upon any incoming heartbeat responses (5.2)
follower(_, ?REMOTE(_, ?APPEND_ENTRIES_RESPONSE(_, _, _, _)), #raft_state{}) ->
    keep_state_and_data;

%% [RequestVote RPC] Handle incoming vote requests (5.2)
follower(_, ?REMOTE(Candidate, ?REQUEST_VOTE(_, CandidateIndex, CandidateTerm)), #raft_state{} = State) ->
    request_vote_impl(?FUNCTION_NAME, Candidate, CandidateIndex, CandidateTerm, State);

%% [Handover][Handover RPC] The leader is requesting this follower to take over leadership in a new term
follower(
    _,
    ?REMOTE(
        ?IDENTITY_REQUIRES_MIGRATION(_, LeaderId) = Sender,
        ?HANDOVER(Ref, PrevLogIndex, PrevLogTerm, LogEntries)
    ),
    #raft_state{
        application = App,
        leader_id = LeaderId
    } = State0
) ->
    ?RAFT_COUNT('wa_raft.follower.handover'),
    ?RAFT_LOG_NOTICE(State0, "evaluating handover RPC from ~0p.", [Sender]),
    case ?RAFT_LEADER_ELIGIBLE(App) of
        true ->
            case append_entries(?FUNCTION_NAME, PrevLogIndex, PrevLogTerm, LogEntries, length(LogEntries), State0) of
                {ok, true, _, State1} ->
                    ?RAFT_LOG_NOTICE(State1, "immediately starting new election due to append success during handover RPC.", []),
                    candidate_or_witness_state_transition(State1);
                {ok, false, _, State1} ->
                    ?RAFT_COUNT('raft.follower.handover.rejected'),
                    ?RAFT_LOG_WARNING(State1, "failing handover request because append was rejected.", []),
                    send_rpc(Sender, ?HANDOVER_FAILED(Ref), State1),
                    {keep_state, State1};
                {fatal, Reason} ->
                    ?RAFT_COUNT('raft.follower.handover.fatal'),
                    ?RAFT_LOG_WARNING(State0, "failing handover request because append was fatal due to ~0P.", [Reason, 30]),
                    send_rpc(Sender, ?HANDOVER_FAILED(Ref), State0),
                    {next_state, disabled, State0#raft_state{disable_reason = Reason}}
            end;
        false ->
            ?RAFT_LOG_NOTICE(State0, "not considering handover RPC due to being inelgibile for leadership.", []),
            send_rpc(Sender, ?HANDOVER_FAILED(Ref), State0),
            {keep_state, State0}
    end;

%% [Handover][HandoverFailed RPC] Followers should not act upon any incoming failed handover
follower(_, ?REMOTE(_, ?HANDOVER_FAILED(_)), #raft_state{}) ->
    keep_state_and_data;

%% [Follower] handle timeout
%% follower doesn't receive any heartbeat. starting a new election
follower(
    state_timeout,
    _,
    #raft_state{
        application = App,
        leader_id = LeaderId,
        log_view = View,
        leader_heartbeat_ts = HeartbeatTs
    } = State
) ->
    WaitingMs = case HeartbeatTs of
        undefined -> undefined;
        _         -> erlang:monotonic_time(millisecond) - HeartbeatTs
    end,
    ?RAFT_COUNT('raft.follower.timeout'),
    case ?RAFT_LEADER_ELIGIBLE(App) andalso ?RAFT_ELECTION_WEIGHT(App) =/= 0 of
        true ->
            ?RAFT_LOG_NOTICE(State, "times out and starts election at ~0p after waiting for leader ~0p for ~0p ms.",
                [wa_raft_log:last_index(View), WaitingMs, LeaderId]),
            {next_state, candidate, State};
        false ->
            ?RAFT_LOG_NOTICE(State, "is not timing out due to being ineligible or having zero election weight.", []),
            {repeat_state, State}
    end;

%% [Command] Defer to common handling for generic RAFT server commands
follower(Type, ?RAFT_COMMAND(_, _) = Event, #raft_state{} = State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Fallback] Report unhandled events
follower(Type, Event, #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "did not know how to handle ~0p event ~0P", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Candidate State
%%------------------------------------------------------------------------------
%% In a RAFT cluster, a candidate is a replica that is attempting to become the
%% leader of a RAFT term. It is waiting for responses from the other members of
%% the RAFT cluster to determine if it has received enough votes to assume the
%% leadership of the RAFT term.
%%------------------------------------------------------------------------------

-spec candidate
    (
        Type :: enter,
        PreviousStateName :: state(),
        Data :: #raft_state{}
    ) -> gen_statem:state_enter_result(state(), #raft_state{});
    (
        Type :: gen_statem:event_type(),
        Event :: event(),
        Data :: #raft_state{}
    ) -> gen_statem:event_handler_result(state(), #raft_state{}).

%% [Enter] Node starts a new election upon entering the candidate state.
candidate(
    enter,
    PreviousStateName,
    #raft_state{
        self = Self,
        current_term = CurrentTerm,
        log_view = View
    } = State0
) ->
    ?RAFT_COUNT('raft.leader.election_started'),
    ?RAFT_COUNT('raft.candidate.enter'),
    ?RAFT_LOG_NOTICE(State0, "becomes candidate from state ~0p.", [PreviousStateName]),

    % Entering the candidate state means that we are starting a new election, thus
    % advance the term and set VotedFor to the current node. (Candidates always
    % implicitly vote for themselves.)
    State1 = enter_state(?FUNCTION_NAME, State0),
    State2 = advance_term(?FUNCTION_NAME, CurrentTerm + 1, node(), State1),

    % Determine the log index and term at which the election will occur.
    LastLogIndex = wa_raft_log:last_index(View),
    {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),

    ?RAFT_LOG_NOTICE(State2, "advances to new term and starts election at ~0p:~0p.",
        [LastLogIndex, LastLogTerm]),

    % Broadcast vote requests and also send a vote-for-self.
    % (Candidates always implicitly vote for themselves.)
    broadcast_rpc(?REQUEST_VOTE(normal, LastLogIndex, LastLogTerm), State2),
    send_rpc(Self, ?VOTE(true), State2),

    {keep_state, State2, ?ELECTION_TIMEOUT(State2)};

%% [Internal] Advance to newer term when requested
candidate(
    internal,
    ?ADVANCE_TERM(NewTerm),
    #raft_state{
        current_term = CurrentTerm
    } = State
) when NewTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.candidate.advance_term'),
    ?RAFT_LOG_NOTICE(State, "advancing to new term ~0p.", [NewTerm]),
    State1 = advance_term(?FUNCTION_NAME, NewTerm, undefined, State),
    {next_state, follower_or_witness_state(State1), State1};

%% [ForceElection] Resend vote requests with the 'force' type to force an election even if an active leader is available.
candidate(
    internal,
    ?FORCE_ELECTION(Term),
    #raft_state{
        log_view = View,
        current_term = CurrentTerm
    } = State
) when Term + 1 =:= CurrentTerm ->
    ?RAFT_LOG_NOTICE(State, "accepts request to force election issued by the immediately prior term ~0p.", [Term]),
    % No changes to the log are expected during an election so we can just reload these values from the log view.
    LastLogIndex = wa_raft_log:last_index(View),
    {ok, LastLogTerm} = wa_raft_log:term(View, LastLogIndex),
    broadcast_rpc(?REQUEST_VOTE(force, LastLogIndex, LastLogTerm), State),
    keep_state_and_data;

%% [Protocol] Parse any RPCs in network formats
candidate(Type, Event, #raft_state{} = State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Switch to follower because current term now has a leader (5.2, 5.3)
candidate(Type, ?REMOTE(Sender, ?APPEND_ENTRIES(_, _, _, _, _)) = Event, #raft_state{} = State) ->
    ?RAFT_LOG_NOTICE(State, "switching to follower after receiving heartbeat from ~0P.", [Sender]),
    {next_state, follower_or_witness_state(State), State, {next_event, Type, Event}};

%% [RequestVote RPC] Candidates should ignore incoming vote requests as they always vote for themselves (5.2)
candidate(_, ?REMOTE(_, ?REQUEST_VOTE(_, _, _)), #raft_state{}) ->
    keep_state_and_data;

%% [Vote RPC] Candidate receives an affirmative vote (5.2)
candidate(
    cast,
    ?REMOTE(?IDENTITY_REQUIRES_MIGRATION(_, NodeId), ?VOTE(true)),
    #raft_state{
        log_view = View,
        state_start_ts = StateStartTs,
        heartbeat_response_ts = HeartbeatResponse0,
        votes = Votes0
    } = State0
) ->
    HeartbeatResponse1 = HeartbeatResponse0#{NodeId => erlang:monotonic_time(millisecond)},
    Votes1 = Votes0#{NodeId => true},
    State1 = State0#raft_state{heartbeat_response_ts = HeartbeatResponse1, votes = Votes1},
    case compute_quorum(Votes1, false, config(State1)) of
        true ->
            Duration = erlang:monotonic_time(millisecond) - StateStartTs,
            LastIndex = wa_raft_log:last_index(View),
            {ok, LastTerm} = wa_raft_log:term(View, LastIndex),
            EstablishedQuorum = [Peer || Peer := true <- Votes1],
            ?RAFT_LOG_NOTICE(State1, "is becoming leader after ~0p ms with log at ~0p:~0p and votes from ~0p.",
                [Duration, LastIndex, LastTerm, EstablishedQuorum]),
            ?RAFT_GATHER('raft.candidate.election.duration', Duration),
            {next_state, leader, State1};
        false ->
            {keep_state, State1}
    end;

%% [Vote RPC] Candidates should ignore negative votes (5.2)
candidate(cast, ?REMOTE(_, ?VOTE(_)), #raft_state{}) ->
    keep_state_and_data;

%% [Handover][Handover RPC] Switch to follower because current term now has a leader (5.2, 5.3)
candidate(Type, ?REMOTE(_, ?HANDOVER(_, _, _, _)) = Event, #raft_state{} = State) ->
    ?RAFT_LOG_NOTICE(State, "ends election to handle handover.", []),
    {next_state, follower_or_witness_state(State), State, {next_event, Type, Event}};

%% [Handover][HandoverFailed RPC] Candidates should not act upon any incoming failed handover
candidate(_, ?REMOTE(_, ?HANDOVER_FAILED(_)), #raft_state{}) ->
    keep_state_and_data;

%% [Candidate] Handle Election Timeout (5.2)
%% Candidate doesn't get enough votes after a period of time, restart election.
candidate(state_timeout, _, #raft_state{votes = Votes} = State) ->
    ?RAFT_LOG_NOTICE(State, "election timed out with votes ~0p.", [Votes]),
    {repeat_state, State};

%% [Command] Defer to common handling for generic RAFT server commands
candidate(Type, ?RAFT_COMMAND(_, _) = Event, #raft_state{} = State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Fallback] Report unhandled events
candidate(Type, Event, #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "did not know how to handle ~0p event ~0P.", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Disabled State
%%------------------------------------------------------------------------------
%% The disabled state is an extension to the RAFT protocol used to hold any
%% replicas of an FSM that have for some reason or another identified that some
%% deficiency or malfunction that makes them unfit to either enforce any prior
%% quorum decisions or properly participate in future quorum decisions. Common
%% reasons include the detection of corruptions or inconsistencies within the
%% FSM state or RAFT log. The reason for which the replica was disabled is kept
%% in persistent so that the replica will remain disabled even when restarted.
%%------------------------------------------------------------------------------

-spec disabled
    (
        Type :: enter,
        PreviousStateName :: state(),
        Data :: #raft_state{}
    ) -> gen_statem:state_enter_result(state(), #raft_state{});
    (
        Type :: gen_statem:event_type(),
        Event :: event(),
        Data :: #raft_state{}
    ) -> gen_statem:event_handler_result(state(), #raft_state{}).

disabled(enter, PreviousStateName, #raft_state{disable_reason = DisableReason} = State0) ->
    ?RAFT_COUNT('raft.disabled.enter'),
    ?RAFT_LOG_NOTICE(State0, "becomes disabled from state ~0p with reason ~0p.", [PreviousStateName, DisableReason]),
    State1 = case DisableReason of
        undefined -> State0#raft_state{disable_reason = "No reason specified."};
        _         -> State0
    end,
    {keep_state, enter_state(?FUNCTION_NAME, State1)};

%% [Internal] Advance to newer term when requested
disabled(
    internal,
    ?ADVANCE_TERM(NewTerm),
    #raft_state{
        current_term = CurrentTerm
    } = State
) when NewTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.disabled.advance_term'),
    ?RAFT_LOG_NOTICE(State, "advancing to new term ~0p.", [NewTerm]),
    {keep_state, advance_term(?FUNCTION_NAME, NewTerm, undefined, State)};

%% [Protocol] Parse any RPCs in network formats
disabled(Type, Event, #raft_state{} = State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Disabled servers should not act upon any incoming heartbeats as they should
%%                     behave as if dead to the cluster
disabled(_, ?REMOTE(_, ?APPEND_ENTRIES(_, _, _, _, _)), #raft_state{}) ->
    keep_state_and_data;

%% [RequestVote RPC] Disabled servers should not act upon any vote requests as they should behave
%%                   as if dead to the cluster
disabled(_, ?REMOTE(_, ?REQUEST_VOTE(_, _, _)), #raft_state{}) ->
    keep_state_and_data;

disabled({call, From}, ?TRIGGER_ELECTION_COMMAND(_), #raft_state{}) ->
    {keep_state_and_data, {reply, From, {error, invalid_state}}};

disabled({call, From}, ?PROMOTE_COMMAND(_, _), #raft_state{}) ->
    {keep_state_and_data, {reply, From, {error, invalid_state}}};

disabled({call, From}, ?ENABLE_COMMAND, #raft_state{} = State0) ->
    ?RAFT_LOG_NOTICE(State0, "re-enabling by request from ~0p by moving to stalled state.", [From]),
    State1 = State0#raft_state{disable_reason = undefined},
    wa_raft_durable_state:store(State1),
    {next_state, stalled, State1, {reply, From, ok}};

%% [Command] Defer to common handling for generic RAFT server commands
disabled(Type, ?RAFT_COMMAND(_, _) = Event, #raft_state{} = State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Fallback] Report unhandled events
disabled(Type, Event, #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "did not know how to handle ~0p event ~0P", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Witness State
%%------------------------------------------------------------------------------
%% The witness state is an extension to the RAFT protocol that identifies a
%% replica as a special "witness replica" that participates in quorum decisions
%% but does not retain a full copy of the actual underlying FSM. These replicas
%% can use significantly fewer system resources to operate however it is not
%% recommended for more than 25% of the replicas in a RAFT cluster to be
%% witness replicas as having more than such a number of witness replicas can
%% result in significantly reduced chance of data durability in the face of
%% unexpected replica loss.
%%------------------------------------------------------------------------------

-spec witness
    (
        Type :: enter,
        PreviousStateName :: state(),
        Data :: #raft_state{}
    ) -> gen_statem:state_enter_result(state(), #raft_state{});
    (
        Type :: gen_statem:event_type(),
        Event :: event(),
        Data :: #raft_state{}
    ) -> gen_statem:event_handler_result(state(), #raft_state{}).

witness(enter, PreviousStateName, #raft_state{} = State) ->
    ?RAFT_COUNT('raft.witness.enter'),
    ?RAFT_LOG_NOTICE(State, "becomes witness from state ~0p.", [PreviousStateName]),
    {keep_state, enter_state(?FUNCTION_NAME, State)};

%% [Internal] Advance to newer term when requested
witness(
    internal,
    ?ADVANCE_TERM(NewTerm),
    #raft_state{
        current_term = CurrentTerm
    } = State
) when NewTerm > CurrentTerm ->
    ?RAFT_COUNT('raft.witness.advance_term'),
    ?RAFT_LOG_NOTICE(State, "advancing to new term ~0p.", [NewTerm]),
    {keep_state, advance_term(?FUNCTION_NAME, NewTerm, undefined, State)};

%% [Protocol] Parse any RPCs in network formats
witness(Type, Event, #raft_state{} = State) when is_tuple(Event), element(1, Event) =:= rpc ->
    handle_rpc(Type, Event, ?FUNCTION_NAME, State);

%% [AppendEntries RPC] Handle incoming heartbeats (5.2, 5.3)
witness(
    Type,
    ?REMOTE(Leader, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex)),
    #raft_state{} = State
) ->
    handle_heartbeat(?FUNCTION_NAME, Type, Leader, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex, State);

%% [AppendEntriesResponse RPC] Witnesses should not act upon any incoming heartbeat responses (5.2)
witness(_, ?REMOTE(_, ?APPEND_ENTRIES_RESPONSE(_, _, _, _)), #raft_state{}) ->
    keep_state_and_data;

%% [Handover][Handover RPC] Witnesses should not receive handover requests
witness(_, ?REMOTE(Sender, ?HANDOVER(Reference, _, _, _)), #raft_state{} = State) ->
    send_rpc(Sender, ?HANDOVER_FAILED(Reference), State),
    keep_state_and_data;

%% [Handover][HandoverFailed RPC] Witnesses should not act upon any incoming failed handover
witness(_, ?REMOTE(_, ?HANDOVER_FAILED(_)), #raft_state{}) ->
    keep_state_and_data;

witness({call, From}, ?TRIGGER_ELECTION_COMMAND(_), #raft_state{}) ->
    {keep_state_and_data, {reply, From, {error, invalid_state}}};

witness({call, From}, ?PROMOTE_COMMAND(_, _), #raft_state{}) ->
    {keep_state_and_data, {reply, From, {error, invalid_state}}};

%% [RequestVote RPC] Handle incoming vote requests (5.2)
witness(_, ?REMOTE(Candidate, ?REQUEST_VOTE(_, CandidateIndex, CandidateTerm)), #raft_state{} = State) ->
    request_vote_impl(?FUNCTION_NAME, Candidate, CandidateIndex, CandidateTerm, State);

%% [Vote RPC] Witnesses should not act upon any incoming votes as they cannot become leader
witness(_, ?REMOTE(_, ?VOTE(_)), #raft_state{}) ->
    keep_state_and_data;

%% [Command] Defer to common handling for generic RAFT server commands
witness(Type, ?RAFT_COMMAND(_, _) = Event, #raft_state{} = State) ->
    command(?FUNCTION_NAME, Type, Event, State);

%% [Fallback] Report unhandled events
witness(Type, Event, #raft_state{} = State) ->
    ?RAFT_LOG_WARNING(State, "did not know how to handle ~0p event ~0P", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Command Handlers
%%------------------------------------------------------------------------------
%% Fallbacks for command calls to the RAFT server for when there is no special
%% handling for a command defined within the state-specific callback itself.
%%------------------------------------------------------------------------------

-spec command(
    State :: state(),
    Type :: gen_statem:event_type(),
    Command :: command(),
    Data :: #raft_state{}
) -> gen_statem:event_handler_result(state(), #raft_state{}).

%% [Commit] Non-leader nodes should fail commits with {error, not_leader}.
command(
    State,
    cast,
    ?COMMIT_COMMAND({Key, _}),
    #raft_state{
        table = Table,
        partition = Partition,
        leader_id = LeaderId
    } = Data
) when State =/= leader ->
    ?RAFT_LOG_WARNING(State, Data, "commit with key ~0p fails. Leader is ~0p.", [Key, LeaderId]),
    wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Key, {error, not_leader}),
    keep_state_and_data;

%% [Strong Read] Non-leader nodes are not eligible for strong reads.
command(
    State,
    cast,
    ?READ_COMMAND({From, _}),
    #raft_state{
        table = Table,
        partition = Partition,
        leader_id = LeaderId
    } = Data
) when State =/= leader ->
    ?RAFT_LOG_WARNING(State, Data, "strong read fails. Leader is ~p.", [LeaderId]),
    wa_raft_queue:fulfill_incomplete_read(Table, Partition, From, {error, not_leader}),
    keep_state_and_data;

%% [Notify Complete] Attempt to send more log entries to storage if applicable.
command(
    State,
    cast,
    ?NOTIFY_COMPLETE_COMMAND(),
    #raft_state{
        table = Table,
        partition = Partition
    } = Data
) when State =:= leader; State =:= follower; State =:= witness ->
    case wa_raft_queue:apply_queue_size(Table, Partition) of
        0 ->
            NewState = case State of
                leader -> apply_log_leader(Data);
                _ -> apply_log_follower(State, infinity, Data)
            end,
            {keep_state, NewState};
        _ ->
            keep_state_and_data
    end;
command(_, cast, ?NOTIFY_COMPLETE_COMMAND(), #raft_state{}) ->
    keep_state_and_data;

%% [Status] Get status of node.
command(State, {call, From}, ?STATUS_COMMAND, #raft_state{} = Data) ->
    Status = [
        {state, State},
        {id, Data#raft_state.self#raft_identity.node},
        {table, Data#raft_state.table},
        {partition, Data#raft_state.partition},
        {partition_path, Data#raft_state.partition_path},
        {current_term, Data#raft_state.current_term},
        {voted_for, Data#raft_state.voted_for},
        {commit_index, Data#raft_state.commit_index},
        {last_applied, Data#raft_state.last_applied},
        {leader_id, Data#raft_state.leader_id},
        {next_indices, Data#raft_state.next_indices},
        {match_indices, Data#raft_state.match_indices},
        {log_module, wa_raft_log:provider(Data#raft_state.log_view)},
        {log_first, wa_raft_log:first_index(Data#raft_state.log_view)},
        {log_last, wa_raft_log:last_index(Data#raft_state.log_view)},
        {votes, Data#raft_state.votes},
        {inflight_applies, wa_raft_queue:apply_queue_size(Data#raft_state.table, Data#raft_state.partition)},
        {disable_reason, Data#raft_state.disable_reason},
        {config, config(Data)},
        {config_index, config_index(Data)},
        {witness, is_self_witness(Data)}
    ],
    {keep_state_and_data, {reply, From, Status}};

%% [Promote] Request full replica nodes to start a new election.
command(
    State,
    {call, From},
    ?TRIGGER_ELECTION_COMMAND(TermOrOffset),
    #raft_state{
        application = App,
        current_term = CurrentTerm
    } = Data
) when State =/= stalled, State =/= witness, State =/= disabled ->
    Term = case TermOrOffset of
        current -> CurrentTerm;
        next -> CurrentTerm + 1;
        {next, Offset} -> CurrentTerm + Offset;
        _ -> TermOrOffset
    end,
    case is_integer(Term) andalso Term >= CurrentTerm of
        true ->
            case ?RAFT_LEADER_ELIGIBLE(App) of
                true ->
                    ?RAFT_LOG_NOTICE(State, Data, "switching to candidate after promotion request.", []),
                    NewState = case Term > CurrentTerm of
                        true -> advance_term(State, Term, undefined, Data);
                        false -> Data
                    end,
                    case State of
                        candidate -> {repeat_state, NewState, {reply, From, ok}};
                        _         -> {next_state, candidate, NewState, {reply, From, ok}}
                    end;
                false ->
                    ?RAFT_LOG_WARNING(State, Data, "cannot be promoted as candidate while ineligible.", []),
                    {keep_state_and_data, {reply, From, {error, ineligible}}}
            end;
        false ->
            ?RAFT_LOG_WARNING(State, Data, "refusing to promote to current, older, or invalid term ~0p.", [Term]),
            {keep_state_and_data, {reply, From, {error, rejected}}}
    end;

%% [Promote] Non-disabled nodes check if eligible to promote and then promote to leader.
command(
    State,
    {call, From},
    ?PROMOTE_COMMAND(TermOrOffset, Force),
    #raft_state{
        application = App,
        current_term = CurrentTerm,
        leader_heartbeat_ts = HeartbeatTs,
        leader_id = LeaderId
    } = Data
) when State =/= stalled; State =/= witness; State =/= disabled ->
    Now = erlang:monotonic_time(millisecond),
    Eligible = ?RAFT_LEADER_ELIGIBLE(App),
    HeartbeatGracePeriodMs = ?RAFT_PROMOTION_GRACE_PERIOD(App) * 1000,
    Term = case TermOrOffset of
        current -> CurrentTerm;
        next -> CurrentTerm + 1;
        {next, Offset} -> CurrentTerm + Offset;
        _ -> TermOrOffset
    end,
    Membership = get_config_members(config(Data)),
    Allowed = if
        % Prevent promotions to older or invalid terms
        not is_integer(Term) orelse Term < CurrentTerm ->
            ?RAFT_LOG_WARNING(State, Data, "cannot attempt promotion to current, older, or invalid term ~0p.", [Term]),
            invalid_term;
        Term =:= CurrentTerm andalso LeaderId =/= undefined ->
            ?RAFT_LOG_WARNING(State, Data, "refusing to promote to leader of current term already led by ~0p.", [LeaderId]),
            invalid_term;
        % Prevent promotions that will immediately result in a resignation.
        not Eligible ->
            ?RAFT_LOG_WARNING(State, Data, "cannot promote to leader as the node is ineligible.", []),
            ineligible;
        State =:= witness ->
            ?RAFT_LOG_WARNING(State, Data, "cannot promote a witness node.", []),
            invalid_state;
        % Prevent promotions to any operational state when there is no cluster membership configuration.
        Membership =:= [] ->
            ?RAFT_LOG_WARNING(State, Data, "cannot promote to leader with no existing membership.", []),
            invalid_configuration;
        Force ->
            true;
        HeartbeatTs =:= undefined ->
            true;
        Now - HeartbeatTs >= HeartbeatGracePeriodMs ->
            true;
        true ->
            ?RAFT_LOG_WARNING(State, Data, "rejecting request to promote to leader as a valid heartbeat was recently received.", []),
            rejected
    end,
    case Allowed of
        true ->
            ?RAFT_LOG_NOTICE(State, Data, "is promoting to leader of term ~0p.", [Term]),
            NewState = case Term > CurrentTerm of
                true -> advance_term(State, Term, node(), Data);
                false -> Data
            end,
            case State of
                leader -> {repeat_state, NewState, {reply, From, ok}};
                _      -> {next_state, leader, NewState, {reply, From, ok}}
            end;
        Reason ->
            {keep_state_and_data, {reply, From, {error, Reason}}}
    end;

%% [Resign] Non-leader nodes cannot resign.
command(State, {call, From}, ?RESIGN_COMMAND, #raft_state{} = Data) when State =/= leader ->
    ?RAFT_LOG_NOTICE(State, Data, "not resigning because we are not leader.", []),
    {keep_state_and_data, {reply, From, {error, not_leader}}};

%% [AdjustMembership] Non-leader nodes cannot adjust their config.
command(
    State,
    Type,
    ?ADJUST_MEMBERSHIP_COMMAND(Action, Peer, _),
    #raft_state{} = Data
) when State =/= leader ->
    ?RAFT_LOG_NOTICE(State, Data, "cannot ~0p peer ~0p because we are not leader.", [Action, Peer]),
    reply(Type, {error, not_leader}),
    {keep_state, Data};

%% [Snapshot Available] Follower and candidate nodes might switch to stalled to install snapshot.
command(
    State,
    Type,
    ?SNAPSHOT_AVAILABLE_COMMAND(_, #raft_log_pos{index = SnapshotIndex}) = Event,
    #raft_state{
        last_applied = LastAppliedIndex
    } = Data
) when State =:= follower; State =:= candidate; State =:= witness ->
    case SnapshotIndex > LastAppliedIndex of
        true ->
            ?RAFT_LOG_NOTICE(State, Data, "at ~0p is notified of a newer snapshot at ~0p.", [LastAppliedIndex, SnapshotIndex]),
            {next_state, stalled, Data, {next_event, Type, Event}};
        false ->
            ?RAFT_LOG_NOTICE(State, Data, "at ~0p is ignoring an older snapshot at ~0p.", [LastAppliedIndex, SnapshotIndex]),
            reply(Type, {error, rejected}),
            keep_state_and_data
    end;

%% [Snapshot Available] Leader and disabled nodes should not install snapshots.
command(
    State,
    Type,
    ?SNAPSHOT_AVAILABLE_COMMAND(_, _),
    #raft_state{}
) when State =:= leader; State =:= disabled ->
    reply(Type, {error, rejected}),
    keep_state_and_data;

%% [Handover Candidates] Non-leader nodes cannot serve handovers.
command(State, {call, From}, ?HANDOVER_CANDIDATES_COMMAND, #raft_state{}) when State =/= leader ->
    {keep_state_and_data, {reply, From, {error, not_leader}}};

%% [Handover] Non-leader nodes cannot serve handovers.
command(State, Type, ?HANDOVER_COMMAND(_), #raft_state{}) when State =/= leader ->
    reply(Type, {error, not_leader}),
    keep_state_and_data;

%% [Enable] Non-disabled nodes are already enabled.
command(State, {call, From}, ?ENABLE_COMMAND, #raft_state{}) when State =/= disabled ->
    {keep_state_and_data, {reply, From, {error, already_enabled}}};

%% [Disable] All nodes should disable by setting RAFT state disable_reason.
command(
    State,
    {call, From},
    ?DISABLE_COMMAND(Reason),
    #raft_state{
        self = ?IDENTITY_REQUIRES_MIGRATION(_, NodeId),
        leader_id = LeaderId
    } = Data0
) ->
    ?RAFT_LOG_NOTICE(State, Data0, "disabling due to ~0p.", [Reason]),
    Data1 = Data0#raft_state{disable_reason = Reason},
    Data2 = case NodeId =:= LeaderId of
        true  -> clear_leader(State, Data1);
        false -> Data1
    end,
    wa_raft_durable_state:store(Data2),
    {next_state, disabled, Data2, {reply, From, ok}};

%% [Fallback] Drop unknown command calls.
command(State, Type, Event, #raft_state{} = Data) ->
    ?RAFT_LOG_NOTICE(State, Data, "dropping unhandled ~0p command ~0P", [Type, Event, 20]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Cluster Configuration Helpers
%%------------------------------------------------------------------------------

%% Determine if the specified peer is a member of the current cluster configuration,
%% returning false if the specified peer is not a member or there is no current cluster
%% configuration.
-spec member(Peer :: peer(), Config :: config()) -> boolean().
member(Peer, Config) ->
    member(Peer, Config, false).

-spec member(Peer :: peer(), Config :: config(), Default :: boolean()) -> boolean().
member(Peer, #{membership := Membership}, _) ->
    lists:member(Peer, Membership);
member(_, _, Default) ->
    Default.

%% Returns true only if the membership of the current configuration contains exactly
%% the provided peer and that the provided peer is not specified as a witness.
-spec is_single_member(Peer :: #raft_identity{} | peer(), Config :: config()) -> IsSingleMember :: boolean().
is_single_member(#raft_identity{name = Name, node = Node}, Config) ->
    is_single_member({Name, Node}, Config);
is_single_member(Peer, #{membership := Membership, witness := Witnesses}) ->
    Membership =:= [Peer] andalso not lists:member(Peer, Witnesses);
is_single_member(Peer, #{membership := Membership}) ->
    Membership =:= [Peer];
is_single_member(_, #{}) ->
    false.

%% Get the non-empty membership list from the provided config. Raises an error
%% if the membership list is missing or empty.
-spec config_membership(Config :: config()) -> Membership :: membership().
config_membership(#{membership := Membership}) when Membership =/= [] ->
    Membership;
config_membership(_) ->
    error(membership_not_set).

-spec config_witnesses(Config :: config()) -> Witnesses :: [peer()].
config_witnesses(#{witness := Witnesses}) ->
    Witnesses;
config_witnesses(_) ->
    [].

-spec is_self_witness(#raft_state{}) -> boolean().
is_self_witness(#raft_state{self = #raft_identity{name = Name, node = Node}} = RaftState) ->
    lists:member({Name, Node}, config_witnesses(config(RaftState))).

-spec config_identities(Config :: config()) -> Peers :: [#raft_identity{}].
config_identities(#{membership := Membership}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Membership];
config_identities(_) ->
    error(membership_not_set).

-spec config_replica_identities(Config :: config()) -> Replicas :: [#raft_identity{}].
config_replica_identities(#{membership := Membership, witness := Witnesses}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Membership -- Witnesses];
config_replica_identities(#{membership := Membership}) ->
    [#raft_identity{name = Name, node = Node} || {Name, Node} <- Membership];
config_replica_identities(_) ->
    error(membership_not_set).

%% Returns the current effective RAFT configuration. This is the most recent configuration
%% stored in either the RAFT log or the RAFT storage.
-spec config(State :: #raft_state{}) -> Config :: config().
config(#raft_state{log_view = View, cached_config = {ConfigIndex, Config}}) ->
    case wa_raft_log:config(View) of
        {ok, LogConfigIndex, LogConfig} when LogConfigIndex > ConfigIndex ->
            LogConfig;
        {ok, _, _} ->
            % This case will normally only occur when the log has leftover log entries from
            % previous incarnations of the RAFT server that have been applied but not yet
            % trimmed this incarnation.
            Config;
        not_found ->
            Config
    end;
config(#raft_state{log_view = View}) ->
    case wa_raft_log:config(View) of
        {ok, _, LogConfig} -> LogConfig;
        not_found -> make_config()
    end.

-spec config_index(State :: #raft_state{}) -> ConfigIndex :: wa_raft_log:log_index().
config_index(#raft_state{log_view = View, cached_config = {ConfigIndex, _}}) ->
    case wa_raft_log:config(View) of
        {ok, LogConfigIndex, _} ->
            % The case where the log contains a config that is already applied generally
            % only occurs after a restart as any log entries whose trim was deferred
            % will become visible again.
            max(LogConfigIndex, ConfigIndex);
        not_found ->
            ConfigIndex
    end;
config_index(#raft_state{log_view = View}) ->
    case wa_raft_log:config(View) of
        {ok, LogConfigIndex, _} -> LogConfigIndex;
        not_found -> 0
    end.

%% Loads and caches the current configuration stored in the RAFT storage.
%% This configuration is used whenever there is no newer configuration
%% available in the RAFT log and so needs to be kept in sync with what
%% the RAFT server expects is in storage.
-spec load_config(State :: #raft_state{}) -> NewState :: #raft_state{}.
load_config(#raft_state{storage = Storage, table = Table, partition = Partition} = State) ->
    case wa_raft_storage:config(Storage) of
        {ok, #raft_log_pos{index = ConfigIndex}, Config} ->
            wa_raft_info:set_membership(Table, Partition, maps:get(membership, Config, [])),
            State#raft_state{cached_config = {ConfigIndex, normalize_config(Config)}};
        undefined ->
            State#raft_state{cached_config = undefined};
        {error, Reason} ->
            error({could_not_load_config, Reason})
    end.

-spec load_label_state(State :: #raft_state{}) -> LabelState :: wa_raft_label:label().
load_label_state(#raft_state{storage = Storage, label_module = LabelModule}) when LabelModule =/= undefined ->
    case wa_raft_storage:label(Storage) of
        {ok, Label} ->
            Label;
        {error, Reason} ->
            error({failed_to_load_label_state, Reason})
    end;
load_label_state(_) ->
    undefined.

%% After an apply is sent to storage, check to see if it is a new configuration
%% being applied. If it is, then update the cached configuration.
-spec maybe_update_config(
    Index :: wa_raft_log:log_index(),
    Term :: wa_raft_log:log_term(),
    Op :: wa_raft_log:log_op() | undefined,
    State :: #raft_state{}
) -> NewState :: #raft_state{}.
maybe_update_config(Index, _, {_, {config, Config}}, #raft_state{table = Table, partition = Partition} = State) ->
    wa_raft_info:set_membership(Table, Partition, maps:get(membership, Config, [])),
    State#raft_state{cached_config = {Index, Config}};
maybe_update_config(Index, _, {_, _, {config, Config}}, #raft_state{table = Table, partition = Partition} = State) ->
    wa_raft_info:set_membership(Table, Partition, maps:get(membership, Config, [])),
    State#raft_state{cached_config = {Index, Config}};
maybe_update_config(_, _, _, #raft_state{} = State) ->
    State.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Private Functions
%%------------------------------------------------------------------------------

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

-spec get_log_entry(State0 :: #raft_state{}, Op :: wa_raft_acceptor:op()) -> {State1 :: #raft_state{}, LogEntry :: wa_raft_log:log_entry()}.
get_log_entry(#raft_state{current_term = CurrentTerm, label_module = undefined} = State0, Op) ->
    {State0, {CurrentTerm, Op}};
get_log_entry(#raft_state{current_term = CurrentTerm, last_label = undefined} = State0, {_, noop} = Op) ->
    {State0, {CurrentTerm, Op}};
get_log_entry(#raft_state{current_term = CurrentTerm, last_label = LastLabel} = State0, {Ref, noop}) ->
    {State0, {CurrentTerm, {Ref, LastLabel, noop}}};
get_log_entry(#raft_state{current_term = CurrentTerm, last_label = undefined} = State0, {_, {config, _}} = Op) ->
    {State0, {CurrentTerm, Op}};
get_log_entry(#raft_state{current_term = CurrentTerm, last_label = LastLabel} = State0, {Ref, {config, _} = Command}) ->
    {State0, {CurrentTerm, {Ref, LastLabel, Command}}};
get_log_entry(#raft_state{current_term = CurrentTerm, label_module = LabelModule, last_label = LastLabel} = State0, {Ref, Command}) ->
    NewLabel = LabelModule:new_label(LastLabel, Command),
    {State0#raft_state{last_label = NewLabel}, {CurrentTerm, {Ref, NewLabel, Command}}}.

-spec apply_single_node_cluster(Data :: #raft_state{}) -> #raft_state{}.
apply_single_node_cluster(#raft_state{self = Self, log_view = View} = Data) ->
    % TODO(hsun324) T112326686: Review after RAFT RPC id changes.
    case is_single_member(Self, config(Data)) of
        true ->
            NewView = case wa_raft_log:sync(View) of
                {ok, Synced} -> Synced;
                _            -> View
            end,
            apply_log_leader(maybe_advance(Data#raft_state{log_view = NewView}));
        false ->
            Data
    end.

%% Leader - check quorum and advance commit index if possible
-spec maybe_advance(Data :: #raft_state{}) -> #raft_state{}.
maybe_advance(
    #raft_state{
        log_view = View,
        commit_index = CommitIndex,
        match_indices = MatchIndices,
        first_current_term_log_index = TermStartIndex
    } = Data
) ->
    LogLast = wa_raft_log:last_index(View),
    case max_index_to_apply(MatchIndices, LogLast, config(Data)) of
        % Raft paper section 5.4.3 - Only log entries from the leader’s current term are committed
        % by counting replicas; once an entry from the current term has been committed in this way,
        % then all prior entries are committed indirectly because of the View Matching Property
        QuorumIndex when QuorumIndex < TermStartIndex ->
            ?RAFT_COUNT('raft.apply.delay.old'),
            ?RAFT_LOG_WARNING(leader, Data, "cannot establish quorum at ~0p before start of term at ~0p.",
                [QuorumIndex, TermStartIndex]),
            Data;
        QuorumIndex when QuorumIndex > CommitIndex ->
            Data#raft_state{commit_index = QuorumIndex};
        _ ->
            Data
    end.

% Return the max index to potentially apply on the leader. This is the latest log index that
% has achieved replication on at least a quorum of nodes in the current RAFT cluster.
% NOTE: We do not need to enforce that the leader should not commit entries from previous
%       terms here (5.4.2) because we only update the CommitIndex broadcast by the leader
%       when we actually apply the log entry on the leader. (See `apply_log` for information
%       about how this rule is enforced there.)
-spec max_index_to_apply(
    MatchIndices :: #{node() => wa_raft_log:log_index()},
    LastIndex :: wa_raft_log:log_index(),
    Config :: config()
) -> wa_raft_log:log_index().
max_index_to_apply(MatchIndices, LastIndex, Config) ->
    compute_quorum(MatchIndices#{node() => LastIndex}, 0, Config).

%% Create a new list with exactly one element for each member in the membership
%% defined in the provided configuration taking the value mapped to each member in
%% the provided map or a provided default if a pairing is not available.
-spec to_member_list(
    Mapping :: #{node() => Value},
    Default :: Value,
    Config :: config()
) -> Normalized :: [Value].
to_member_list(Mapping, Default, Config) ->
    [maps:get(Node, Mapping, Default) || {_, Node} <- config_membership(Config)].

%% Compute the quorum maximum value for the current membership given a config for
%% the values represented by the given a mapping of peers (see note on config about
%% RAFT RPC ids) to values assuming a default value for peers who are not represented
%% in the mapping.
-spec compute_quorum(
    Mapping :: #{node() => Value},
    Default :: Value,
    Config :: config()
 ) -> Quorum :: Value.
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

-spec apply_log_leader(Data :: #raft_state{}) -> #raft_state{}.
apply_log_leader(
    #raft_state{
        last_applied = LastApplied,
        current_term = CurrentTerm,
        match_indices = MatchIndices
    } = Data
) ->
    TrimIndex = lists:min(to_member_list(MatchIndices#{node() => LastApplied}, 0, config(Data))),
    apply_log(leader, CurrentTerm, TrimIndex, Data).

-spec apply_log_follower(
    State :: state(),
    TrimIndex :: wa_raft_log:log_index() | infinity,
    Data :: #raft_state{}
) -> #raft_state{}.
apply_log_follower(State, TrimIndex, Data) ->
    apply_log(State, undefined, TrimIndex, Data).

-spec apply_log(
    State :: state(),
    EffectiveTerm :: wa_raft_log:log_term() | undefined,
    TrimIndex :: wa_raft_log:log_index() | infinity,
    Data :: #raft_state{}
) -> #raft_state{}.
apply_log(
    State,
    EffectiveTerm,
    TrimIndex,
    #raft_state{
        application = App,
        table = Table,
        partition = Partition,
        log_view = View,
        commit_index = CommitIndex,
        last_applied = LastApplied
    } = Data0
) when CommitIndex > LastApplied ->
    StartT = os:timestamp(),
    case wa_raft_queue:apply_queue_full(Table, Partition) of
        false ->
            % Apply a limited number of log entries (both count and total byte size limited)
            LimitedIndex = min(CommitIndex, LastApplied + ?RAFT_MAX_CONSECUTIVE_APPLY_ENTRIES(App)),
            LimitBytes = ?RAFT_MAX_CONSECUTIVE_APPLY_BYTES(App),
            {ok, {_, #raft_state{log_view = View1, last_applied = NewLastApplied} = Data1}} = wa_raft_log:fold(View, LastApplied + 1, LimitedIndex, LimitBytes,
                fun (Index, Size, Entry, {Index, AccData}) ->
                    wa_raft_queue:reserve_apply(Table, Partition, Size),
                    {Index + 1, apply_op(State, Index, Size, Entry, EffectiveTerm, AccData)}
                end, {LastApplied + 1, Data0}),

            % Perform log trimming since we've now applied some log entries, only keeping
            % at maximum MaxRotateDelay log entries.
            MaxRotateDelay = ?RAFT_MAX_RETAINED_ENTRIES(App),
            RotateIndex = max(LimitedIndex - MaxRotateDelay, min(NewLastApplied, TrimIndex)),
            RotateIndex =/= infinity orelse error(bad_state),
            {ok, View2} = wa_raft_log:rotate(View1, RotateIndex),
            Data2 = Data1#raft_state{log_view = View2},
            ?RAFT_GATHER('raft.apply_log.latency_us', timer:now_diff(os:timestamp(), StartT)),
            Data2;
        true ->
            ApplyQueueSize = wa_raft_queue:apply_queue_size(Table, Partition),
            ?RAFT_COUNT('raft.apply.delay'),
            ?RAFT_GATHER('raft.apply.queue', ApplyQueueSize),
            LastApplied rem 10 =:= 0 andalso
                ?RAFT_LOG_WARNING(State, Data0, "delays applying for long queue ~0p with last applied ~0p.", [ApplyQueueSize, LastApplied]),
            ?RAFT_GATHER('raft.apply_log.latency_us', timer:now_diff(os:timestamp(), StartT)),
            Data0
    end;
apply_log(_, _, _, #raft_state{} = Data) ->
    Data.

-spec apply_op(
    State :: state(),
    Index :: wa_raft_log:log_index(),
    Size :: non_neg_integer(),
    Entry :: wa_raft_log:log_entry() | undefined,
    EffectiveTerm :: wa_raft_log:log_term() | undefined,
    Data :: #raft_state{}
) -> #raft_state{}.
apply_op(State, LogIndex, _, _, _, #raft_state{last_applied = LastApplied} = Data) when LogIndex =< LastApplied ->
    ?RAFT_LOG_WARNING(State, Data, "is skipping applying log entry ~0p because log entries up to ~0p are already applied.", [LogIndex, LastApplied]),
    Data;
apply_op(_, LogIndex, Size, {Term, {Ref, Command} = Op}, EffectiveTerm, #raft_state{storage = Storage} = Data) ->
    wa_raft_storage:apply(Storage, {LogIndex, {Term, {Ref, undefined, Command}}}, Size, EffectiveTerm),
    maybe_update_config(LogIndex, Term, Op, Data#raft_state{last_applied = LogIndex});
apply_op(_, LogIndex, Size, {Term, {_, _, _} = Op}, EffectiveTerm, #raft_state{storage = Storage} = Data) ->
    wa_raft_storage:apply(Storage, {LogIndex, {Term, Op}}, Size, EffectiveTerm),
    maybe_update_config(LogIndex, Term, Op, Data#raft_state{last_applied = LogIndex});
apply_op(State, LogIndex, _, undefined, _, #raft_state{log_view = View} = Data) ->
    ?RAFT_COUNT('raft.server.missing.log.entry'),
    ?RAFT_LOG_ERROR(State, Data, "failed to apply ~0p because log entry is missing from log covering ~0p to ~0p.",
        [LogIndex, wa_raft_log:first_index(View), wa_raft_log:last_index(View)]),
    exit({invalid_op, LogIndex});
apply_op(State, LogIndex, _, Entry, _, #raft_state{} = Data) ->
    ?RAFT_COUNT('raft.server.corrupted.log.entry'),
    ?RAFT_LOG_ERROR(State, Data, "failed to apply unrecognized entry ~0P at ~0p.", [Entry, 20, LogIndex]),
    exit({invalid_op, LogIndex, Entry}).

-spec append_entries_to_followers(State :: #raft_state{}) -> #raft_state{}.
append_entries_to_followers(#raft_state{table = Table, partition = Partition, log_view = View0} = State0) ->
    State1 = case wa_raft_log:sync(View0) of
        {ok, View1} ->
            State0#raft_state{log_view = View1};
        skipped ->
            {ok, Pending, View1} = wa_raft_log:cancel(View0),
            ?RAFT_COUNT('raft.server.sync.skipped'),
            ?RAFT_LOG_WARNING(leader, State0, "skipped pre-heartbeat sync for ~0p log entr(ies).", [length(Pending)]),
            lists:foreach(
                fun({_, {Reference, _}}) ->
                    wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Reference, {error, commit_stalled});
                   ({_, {Reference, _, _}}) ->
                    wa_raft_queue:fulfill_incomplete_commit(Table, Partition, Reference, {error, commit_stalled})
                end,
                Pending),
            State0#raft_state{log_view = View1};
        {error, Error} ->
            ?RAFT_COUNT({'raft.server.sync', Error}),
            ?RAFT_LOG_ERROR(leader, State0, "sync failed due to ~0P.", [Error, 20]),
            error(Error)
    end,
    lists:foldl(
        fun (Self, #raft_state{self = Self} = StateN) -> StateN;
            (Peer, #raft_state{} = StateN)            -> heartbeat(Peer, StateN)
        end, State1, config_identities(config(State1))).

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - State Management
%%------------------------------------------------------------------------------

-spec set_leader(State :: state(), Leader :: #raft_identity{}, Data :: #raft_state{}) -> #raft_state{}.
set_leader(_, ?IDENTITY_REQUIRES_MIGRATION(_, Node), #raft_state{leader_id = Node} = Data) ->
    Data;
set_leader(
    State,
    ?IDENTITY_REQUIRES_MIGRATION(_, Node),
    #raft_state{
        table = Table,
        partition = Partition,
        current_term = CurrentTerm
    } = Data
) ->
    ?RAFT_LOG_NOTICE(State, Data, "changes leader to ~0p.", [Node]),
    wa_raft_info:set_current_term_and_leader(Table, Partition, CurrentTerm, Node),
    Data#raft_state{leader_id = Node}.

-spec clear_leader(state(), #raft_state{}) -> #raft_state{}.
clear_leader(_, #raft_state{leader_id = undefined} = Data) ->
    Data;
clear_leader(State, #raft_state{table = Table, partition = Partition, current_term = CurrentTerm} = Data) ->
    ?RAFT_LOG_NOTICE(State, Data, "clears leader record.", []),
    wa_raft_info:set_current_term_and_leader(Table, Partition, CurrentTerm, undefined),
    Data#raft_state{leader_id = undefined}.

%% Setup the RAFT state upon entry into a new RAFT server state.
-spec enter_state(State :: state(), Data :: #raft_state{}) -> #raft_state{}.
enter_state(State, #raft_state{table = Table, partition = Partition, storage = Storage} = Data0) ->
    Now = erlang:monotonic_time(millisecond),
    Data1 = Data0#raft_state{state_start_ts = Now},
    true = wa_raft_info:set_state(Table, Partition, State),
    ok = check_stale_upon_entry(State, Now, Data1),
    ok = wa_raft_storage:cancel(Storage),
    Data1.

-spec check_stale_upon_entry(State :: state(), Now :: integer(), Data :: #raft_state{}) -> ok.
%% Followers and candidates may be stale upon entry due to not receiving a timely heartbeat from an active leader.
check_stale_upon_entry(
    State,
    Now,
    #raft_state{
        application = Application,
        table = Table,
        partition = Partition,
        leader_heartbeat_ts = LeaderHeartbeatTs
    } = Data
) when State =:= follower; State =:= candidate ->
    Stale = case LeaderHeartbeatTs of
        undefined ->
            ?RAFT_LOG_NOTICE(State, Data, "is stale upon entry due to having no prior leader heartbeat.", []),
            true;
        _ ->
            Delay = Now - LeaderHeartbeatTs,
            case Delay > ?RAFT_FOLLOWER_STALE_INTERVAL(Application) of
                true ->
                    ?RAFT_LOG_NOTICE(State, Data, "is stale upon entry because the last leader heartbeat was received ~0p ms ago.", [Delay]),
                    true;
                false ->
                    false
            end
    end,
    true = wa_raft_info:set_stale(Table, Partition, Stale),
    ok;
%% Leaders are never stale upon entry.
check_stale_upon_entry(leader, _, #raft_state{table = Table, partition = Partition}) ->
    true = wa_raft_info:set_stale(Table, Partition, false),
    ok;
%% Witness, stalled and disabled servers are always stale.
check_stale_upon_entry(_, _, #raft_state{table = Table, partition = Partition}) ->
    true = wa_raft_info:set_stale(Table, Partition, true),
    ok.

%% Set a new current term and voted-for peer and clear any state that is associated with the previous term.
-spec advance_term(
    State :: state(),
    NewTerm :: wa_raft_log:log_term(),
    VotedFor :: undefined | node(),
    Data :: #raft_state{}
) -> #raft_state{}.
advance_term(
    State,
    NewTerm,
    VotedFor,
    #raft_state{
        current_term = CurrentTerm
    } = Data0
) when NewTerm > CurrentTerm ->
    Data1 = Data0#raft_state{
        current_term = NewTerm,
        voted_for = VotedFor,
        votes = #{},
        next_indices = #{},
        match_indices = #{},
        last_applied_indices = #{},
        last_heartbeat_ts = #{},
        heartbeat_response_ts = #{},
        handover = undefined
    },
    Data2 = clear_leader(State, Data1),
    ok = wa_raft_durable_state:store(Data2),
    Data2.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Leader Methods
%%------------------------------------------------------------------------------

-spec heartbeat(Peer :: #raft_identity{}, State :: #raft_state{}) -> #raft_state{}.
heartbeat(
    ?IDENTITY_REQUIRES_MIGRATION(_, FollowerId) = Sender,
    #raft_state{
        application = App,
        name = Name,
        log_view = View,
        catchup = Catchup,
        commit_index = CommitIndex,
        next_indices = NextIndices,
        match_indices = MatchIndices,
        last_heartbeat_ts = LastHeartbeatTs,
        first_current_term_log_index = TermStartIndex
    } = State0
) ->
    FollowerNextIndex = maps:get(FollowerId, NextIndices, TermStartIndex),
    PrevLogIndex = FollowerNextIndex - 1,
    PrevLogTermRes = wa_raft_log:term(View, PrevLogIndex),
    FollowerMatchIndex = maps:get(FollowerId, MatchIndices, 0),
    FollowerMatchIndex =/= 0 andalso
        ?RAFT_GATHER('raft.leader.follower.lag', CommitIndex - FollowerMatchIndex),
    IsCatchingUp = wa_raft_log_catchup:is_catching_up(Catchup, Sender),
    NowTs = erlang:monotonic_time(millisecond),
    LastFollowerHeartbeatTs = maps:get(FollowerId, LastHeartbeatTs, undefined),
    State1 = State0#raft_state{last_heartbeat_ts = LastHeartbeatTs#{FollowerId => NowTs}, leader_heartbeat_ts = NowTs},
    LastIndex = wa_raft_log:last_index(View),
    case PrevLogTermRes =:= not_found orelse IsCatchingUp of %% catching up, or prep
        true ->
            {ok, LastTerm} = wa_raft_log:term(View, LastIndex),
            ?RAFT_LOG_DEBUG(leader, State1, "sends empty heartbeat to follower ~p (local ~p, prev ~p, catching-up ~p)",
                [FollowerId, LastIndex, PrevLogIndex, IsCatchingUp]),
            % Send append entries request.
            send_rpc(Sender, ?APPEND_ENTRIES(LastIndex, LastTerm, [], CommitIndex, 0), State1),
            LastFollowerHeartbeatTs =/= undefined andalso
                ?RAFT_GATHER('raft.leader.heartbeat.interval_ms', erlang:monotonic_time(millisecond) - LastFollowerHeartbeatTs),
            State1;
        false ->
            MaxLogEntries = ?RAFT_HEARTBEAT_MAX_ENTRIES(App),
            MaxHeartbeatSize = ?RAFT_HEARTBEAT_MAX_BYTES(App),
            Witnesses = config_witnesses(config(State0)),
            {ok, RawEntries} = wa_raft_log:get(View, FollowerNextIndex, MaxLogEntries, MaxHeartbeatSize),
            Entries = case lists:member({Name, FollowerId}, Witnesses) of
                true ->
                    stub_entries_for_witness(RawEntries);
                false ->
                    RawEntries
            end,
            {ok, PrevLogTerm} = PrevLogTermRes,
            ?RAFT_GATHER('raft.leader.heartbeat.size', length(Entries)),
            ?RAFT_LOG_DEBUG(leader, State1, "heartbeat to follower ~p from ~p(~p entries). Commit index ~p",
                [FollowerId, FollowerNextIndex, length(Entries), CommitIndex]),
            % Compute trim index.
            TrimIndex = lists:min(to_member_list(MatchIndices#{node() => LastIndex}, 0, config(State1))),
            % Send append entries request.
            CastResult = send_rpc(Sender, ?APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex), State1),
            NewNextIndices =
                case CastResult of
                    ok ->
                        % pipelining - move NextIndex after sending out logs. If a packet is lost, follower's AppendEntriesResponse
                        % will return send back its correct index
                        NextIndices#{FollowerId => PrevLogIndex + length(Entries) + 1};
                    _ ->
                        NextIndices
                end,
            LastFollowerHeartbeatTs =/= undefined andalso
                ?RAFT_GATHER('raft.leader.heartbeat.interval_ms', erlang:monotonic_time(millisecond) - LastFollowerHeartbeatTs),
            State1#raft_state{next_indices = NewNextIndices}
    end.

-spec stub_entries_for_witness([wa_raft_log:log_entry()]) -> [wa_raft_log:log_entry()].
stub_entries_for_witness(Entries) ->
    [stub_entry(E) || E <- Entries].

-spec stub_entry(wa_raft_log:log_entry()) -> wa_raft_log:log_entry().
stub_entry({Term, undefined}) ->
    {Term, undefined};
stub_entry({Term, {Key, Cmd}}) ->
    {Term, {Key, stub_command(Cmd)}};
stub_entry({Term, {Key, Label, Cmd}}) ->
    {Term, {Key, Label, stub_command(Cmd)}}.

-spec stub_command(wa_raft_acceptor:command()) -> wa_raft_acceptor:command().
stub_command(noop) -> noop;
stub_command({config, _} = ConfigCmd) -> ConfigCmd;
stub_command(_) -> noop_omitted.

-spec compute_handover_candidates(State :: #raft_state{}) -> [node()].
compute_handover_candidates(#raft_state{application = App, log_view = View} = State) ->
    Replicas = config_replica_identities(config(State)),
    LastIndex = wa_raft_log:last_index(View),
    MatchCutoffIndex = LastIndex - ?RAFT_HANDOVER_MAX_ENTRIES(App),
    ApplyCutoffIndex =
        case ?RAFT_HANDOVER_MAX_UNAPPLIED_ENTRIES(App) of
            undefined -> MatchCutoffIndex;
            Limit -> LastIndex - Limit
        end,
    [Peer || ?IDENTITY_REQUIRES_MIGRATION(_, Peer) = Replica <- Replicas, Peer =/= node(), is_eligible_for_handover(Replica, MatchCutoffIndex, ApplyCutoffIndex, State)].

-spec is_eligible_for_handover(
    Candidate :: #raft_identity{},
    MatchCutoffIndex :: wa_raft_log:log_index(),
    ApplyCutoffIndex :: wa_raft_log:log_index(),
    State :: #raft_state{}
) -> boolean().
is_eligible_for_handover(
    ?IDENTITY_REQUIRES_MIGRATION(_, CandidateId),
    MatchCutoffIndex,
    ApplyCutoffIndex,
    #raft_state{
        match_indices = MatchIndices,
        last_applied_indices = LastAppliedIndices
    }
) ->
    % A peer whose matching index is unknown should not be eligible for handovers.
    MatchIndex = maps:get(CandidateId, MatchIndices, 0),
    % A peer whose last applied index is unknown should not be eligible for handovers.
    LastAppliedIndex = maps:get(CandidateId, LastAppliedIndices, 0),
    MatchIndex >= MatchCutoffIndex andalso LastAppliedIndex >= ApplyCutoffIndex.

-spec adjust_config(
    Action :: {add, peer()} | {remove, peer()} | {add_witness, peer()} | {remove_witness, peer()} | {refresh, undefined},
    Config :: config(),
    State :: #raft_state{}
) -> {ok, NewConfig :: config()} | {error, Reason :: atom()}.
adjust_config(Action, Config, #raft_state{self = Self}) ->
    Membership = get_config_members(Config),
    Witness = get_config_witnesses(Config),
    case Action of
        % The 'refresh' action is used to commit the current effective configuration to storage in the
        % case of upgrading from adhoc to stored configuration or to materialize changes to the format
        % of stored configurations.
        {refresh, undefined} ->
            {ok, Config};
        {add, {Name, Node}} ->
            PeerIdentity = ?IDENTITY_REQUIRES_MIGRATION(Name, Node),
            case lists:member(PeerIdentity, Membership) of
                true  -> {error, already_member};
                false -> {ok, set_config_members([PeerIdentity | Membership], Config)}
            end;
        {add_witness, {Name, Node}} ->
            PeerIdentity = ?IDENTITY_REQUIRES_MIGRATION(Name, Node),
            case {lists:member(PeerIdentity, Witness), lists:member(PeerIdentity, Membership)} of
                {true, true}  -> {error, already_witness};
                {true, _}     -> {error, already_member};
                {false, _}    -> {ok, set_config_members([PeerIdentity | Membership], [PeerIdentity | Witness], Config)}
            end;
        {remove, {Name, Node}} ->
            PeerIdentity = ?IDENTITY_REQUIRES_MIGRATION(Name, Node),
            case {PeerIdentity, lists:member(PeerIdentity, Membership)} of
                {Self, _}  -> {error, cannot_remove_self};
                {_, false} -> {error, not_a_member};
                {_, true}  -> {ok, set_config_members(lists:delete(PeerIdentity, Membership), lists:delete(PeerIdentity, Witness), Config)}
            end;
        {remove_witness, {Name, Node}} ->
            PeerIdentity = ?IDENTITY_REQUIRES_MIGRATION(Name, Node),
            case {PeerIdentity, lists:member(PeerIdentity, Witness)} of
                {Self, _}  -> {error, cannot_remove_self};
                {_, false} -> {error, not_a_witness};
                {_, true}  -> {ok, set_config_members(lists:delete(PeerIdentity, Membership), lists:delete(PeerIdentity, Witness), Config)}
            end
    end.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Log State
%%------------------------------------------------------------------------------

-spec reset_log(Position :: wa_raft_log:log_pos(), Data :: #raft_state{}) -> #raft_state{}.
reset_log(#raft_log_pos{index = Index} = Position, #raft_state{log_view = View} = Data) ->
    {ok, NewView} = wa_raft_log:reset(View, Position),
    NewData = Data#raft_state{
        log_view = NewView,
        last_applied = Index,
        commit_index = Index
    },
    load_config(NewData).

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Snapshots
%%------------------------------------------------------------------------------

-spec open_snapshot(Root :: string(), Position :: wa_raft_log:log_pos(), Data :: #raft_state{}) -> #raft_state{}.
open_snapshot(Root, Position, #raft_state{storage = Storage} = Data) ->
    ok = wa_raft_storage:open_snapshot(Storage, Root, Position),
    reset_log(Position, Data).

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Heartbeat
%%------------------------------------------------------------------------------

%% Attempt to append the log entries declared by a leader in a heartbeat,
%% apply committed but not yet applied log entries, trim the log, and reset
%% the election timeout timer as necessary.
-spec handle_heartbeat(
    State :: state(),
    Event :: gen_statem:event_type(),
    Leader :: #raft_identity{},
    PrevLogIndex :: wa_raft_log:log_index(),
    PrevLogTerm :: wa_raft_log:log_term(),
    Entries :: [wa_raft_log:log_entry()],
    CommitIndex :: wa_raft_log:log_index(),
    TrimIndex :: wa_raft_log:log_index(),
    Data :: #raft_state{}
) -> gen_statem:event_handler_result(state(), #raft_state{}).

handle_heartbeat(
    State,
    Event,
    Leader,
    PrevLogIndex,
    PrevLogTerm,
    Entries,
    LeaderCommitIndex,
    TrimIndex,
    #raft_state{
        application = App,
        table = Table,
        partition = Partition,
        current_term = CurrentTerm,
        log_view = View,
        commit_index = CommitIndex,
        last_applied = LastApplied
    } = Data0
) ->
    EntryCount = length(Entries),

    ?RAFT_GATHER({raft, State, 'heartbeat.size'}, EntryCount),
    ?RAFT_LOG_DEBUG(State, Data0, "considering appending ~0p log entries in range ~0p to ~0p to log ending at ~0p.",
        [EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, wa_raft_log:last_index(View)]),

    case append_entries(State, PrevLogIndex, PrevLogTerm, Entries, EntryCount, Data0) of
        {ok, Accepted, NewMatchIndex, Data1} ->
            AdjustedLastApplied = max(0, LastApplied - wa_raft_queue:apply_queue_size(Table, Partition)),
            send_rpc(Leader, ?APPEND_ENTRIES_RESPONSE(PrevLogIndex, Accepted, NewMatchIndex, AdjustedLastApplied), Data1),
            reply_rpc(Event, ?LEGACY_APPEND_ENTRIES_RESPONSE_RPC(CurrentTerm, node(), PrevLogIndex, Accepted, NewMatchIndex), Data1),

            Data2 = Data1#raft_state{leader_heartbeat_ts = erlang:monotonic_time(millisecond)},
            Data3 = case Accepted of
                true ->
                    LocalTrimIndex = case ?RAFT_LOG_ROTATION_BY_TRIM_INDEX(App) of
                        true  -> TrimIndex;
                        false -> infinity
                    end,
                    NewCommitIndex = max(CommitIndex, min(LeaderCommitIndex, NewMatchIndex)),
                    apply_log_follower(State, LocalTrimIndex, Data2#raft_state{commit_index = NewCommitIndex});
                _ ->
                    Data2
            end,
            check_follower_lagging(CommitIndex, Data3),
            case follower_or_witness_state(Data3) of
                State ->
                    {keep_state, Data3, ?ELECTION_TIMEOUT(Data3)};
                NewState ->
                    {next_state, NewState, Data3, ?ELECTION_TIMEOUT(Data3)}
            end;
        {fatal, Reason} ->
            {next_state, disabled, Data0#raft_state{disable_reason = Reason}}
    end.

%% Append the provided range of the log entries to the local log only if the
%% term of the previous log matches the term stored by the local log,
%% otherwise, truncate the log if the term does not match or do nothing if
%% the previous log entry is not available locally. If an unrecoverable error
%% is encountered, returns a diagnostic that can be used as a reason to
%% disable the current replica.
-spec append_entries(
    State :: state(),
    PrevLogIndex :: wa_raft_log:log_index(),
    PrevLogTerm :: wa_raft_log:log_term(),
    Entries :: [wa_raft_log:log_entry()],
    EntryCount :: non_neg_integer(),
    Data :: #raft_state{}
) -> {ok, Accepted :: boolean(), NewMatchIndex :: wa_raft_log:log_index(), NewData :: #raft_state{}} | {fatal, Reason :: term()}.

append_entries(
    State,
    PrevLogIndex,
    PrevLogTerm,
    Entries,
    EntryCount,
    #raft_state{
        log_view = View,
        commit_index = CommitIndex,
        current_term = CurrentTerm,
        leader_id = LeaderId
    } = Data
) ->
    % Inspect the locally stored term associated with the previous log entry to discern if
    % appending the provided range of log entries is allowed.
    case wa_raft_log:term(View, PrevLogIndex) of
        {ok, PrevLogTerm} ->
            % If the term of the log entry previous the entries to be applied matches the term stored
            % with the previous log entry in the local RAFT log, then this follower can proceed with
            % appending to the log.
            {ok, NewMatchIndex, NewView} = wa_raft_log:append(View, PrevLogIndex + 1, Entries),
            {ok, true, NewMatchIndex, Data#raft_state{log_view = NewView}};
        {ok, LocalPrevLogTerm} ->
            % If the term of the log entry proceeding the entries to be applied does not match the log
            % entry stored with the previous log entry in the local RAFT log, then we need to truncate
            % the log because there is a mismatch between this follower and the leader of the cluster.
            ?RAFT_COUNT({raft, State, 'heartbeat.skip.log_term_mismatch'}),
            ?RAFT_LOG_WARNING(State, Data, "rejects appending ~0p log entries in range ~0p to ~0p as previous log entry ~0p has term ~0p locally when leader ~0p expects it to have term ~0p.",
                [EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, PrevLogIndex, LocalPrevLogTerm, LeaderId, PrevLogTerm]),
            case PrevLogIndex =< CommitIndex of
                true ->
                    % We cannot validly delete log entries that have already been committed because doing
                    % so means that we are erasing log entries that may be part of the minimum quorum. If
                    % we try to do so, then disable this partition as we've violated a critical invariant.
                    ?RAFT_COUNT({raft, State, 'heartbeat.error.corruption.excessive_truncation'}),
                    ?RAFT_LOG_WARNING(State, Data, "fails as progress requires truncation of log entry at ~0p due to log mismatch when log entries up to ~0p were already committed.",
                        [PrevLogIndex, CommitIndex]),
                    {fatal,
                        lists:flatten(
                            io_lib:format("Leader ~0p of term ~0p requested truncation of log entry at ~0p due to log term mismatch (local ~0p, leader ~0p) when log entries up to ~0p were already committed.",
                                [LeaderId, CurrentTerm, PrevLogIndex, LocalPrevLogTerm, PrevLogTerm, CommitIndex]))};
                false ->
                    % We are not deleting already applied log entries, so proceed with truncation.
                    ?RAFT_LOG_NOTICE(State, Data, "Server[~0p, term ~0p, ~0p] truncating local log ending at ~0p to past ~0p due to log mismatch.",
                        [wa_raft_log:last_index(View), PrevLogIndex]),
                    {ok, NewView} = wa_raft_log:truncate(View, PrevLogIndex),
                    {ok, false, wa_raft_log:last_index(NewView), Data#raft_state{log_view = NewView}}
            end;
        not_found ->
            % If the log entry is not found, then ignore and notify the leader of what log entry
            % is required by this follower in the reply.
            ?RAFT_COUNT({raft, State, 'heartbeat.skip.missing_previous_log_entry'}),
            EntryCount =/= 0 andalso
                ?RAFT_LOG_WARNING(State, Data, "skips appending ~0p log entries in range ~0p to ~0p because previous log entry at ~0p is not available in local log covering ~0p to ~0p.",
                    [EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, PrevLogIndex, wa_raft_log:first_index(View), wa_raft_log:last_index(View)]),
            {ok, false, wa_raft_log:last_index(View), Data};
        {error, Reason} ->
            ?RAFT_COUNT({raft, State, 'heartbeat.skip.failed_to_read_previous_log_entry'}),
            ?RAFT_LOG_WARNING(State, Data, "skips appending ~0p log entries in range ~0p to ~0p because reading previous log entry at ~0p failed with error ~0P.",
                [EntryCount, PrevLogIndex + 1, PrevLogIndex + EntryCount, PrevLogIndex, Reason, 30]),
            {ok, false, wa_raft_log:last_index(View), Data}
    end.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Vote Requests
%%------------------------------------------------------------------------------

%% [RequestVote RPC]
-spec request_vote_impl(
    State :: state(),
    Candidate :: #raft_identity{},
    CandidateIndex :: wa_raft_log:log_index(),
    CandidateTerm :: wa_raft_log:log_term(),
    Data :: #raft_state{}
) -> gen_statem:event_handler_result(state(), #raft_state{}).
%% A replica with an available vote in the current term should allocate its vote
%% if the candidate's log is at least as up-to-date as the local log. (5.4.1)
request_vote_impl(
    State,
    ?IDENTITY_REQUIRES_MIGRATION(_, CandidateId) = Candidate,
    CandidateIndex,
    CandidateTerm,
    #raft_state{
        log_view = View,
        voted_for = VotedFor
    } = Data
) when VotedFor =:= undefined; VotedFor =:= CandidateId ->
    Index = wa_raft_log:last_index(View),
    {ok, Term} = wa_raft_log:term(View, Index),
    case {CandidateTerm, CandidateIndex} >= {Term, Index} of
        true ->
            ?RAFT_LOG_NOTICE(State, Data, "decides to vote for candidate ~0p with up-to-date log at ~0p:~0p versus local log at ~0p:~0p.",
                [Candidate, CandidateIndex, CandidateTerm, Index, Term]),
            case VotedFor of
                undefined ->
                    % If this vote request causes the current replica to allocate its vote, then
                    % persist the vote before responding. (Fig. 2)
                    NewData = Data#raft_state{voted_for = CandidateId},
                    wa_raft_durable_state:store(NewData),
                    send_rpc(Candidate, ?VOTE(true), NewData),
                    {keep_state, NewData};
                CandidateId ->
                    % Otherwise, the vote allocation did not change, so just send the response.
                    send_rpc(Candidate, ?VOTE(true), Data),
                    keep_state_and_data
            end;
        false ->
            ?RAFT_LOG_NOTICE(State, Data, "refuses to vote for candidate ~0p with outdated log at ~0p:~0p versus local log at ~0p:~0p.",
                [Candidate, CandidateIndex, CandidateTerm, Index, Term]),
            keep_state_and_data
    end;
%% A replica that was already allocated its vote to a specific candidate in the
%% current term should ignore vote requests from other candidates. (5.4.1)
request_vote_impl(State, Candidate, _, _,  #raft_state{voted_for = VotedFor} = Data) ->
    ?RAFT_LOG_NOTICE(State, Data, "refusing to vote for candidate ~0p after previously voting for candidate ~0p in the current term.",
        [Candidate, VotedFor]),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% RAFT Server - State Machine Implementation - Helpers
%%------------------------------------------------------------------------------

%% Generic reply function for non-RPC requests that operates based on event type.
-spec reply(Type :: enter | gen_statem:event_type(), Message :: term()) -> ok | wa_raft:error().
reply(cast, _) ->
    ok;
reply({call, From}, Message) ->
    gen_statem:reply(From, Message);
reply(Type, Message) ->
    ?LOG_WARNING("Attempted to reply to non-reply event type ~0p with message ~0P.",
        [Type, Message, 20], ?RAFT_LOG_OPTS),
    ok.

%% Generic reply function for RPC requests that operates based on event type.
-spec reply_rpc(Type :: gen_statem:event_type(), Reply :: term(), Data :: #raft_state{}) -> term().
reply_rpc({call, From}, Reply, #raft_state{identifier = Identifier, distribution_module = Distribution}) ->
    Distribution:reply(From, Identifier, Reply);
reply_rpc(_, _, #raft_state{}) ->
    ok.

-spec send_rpc(
    Destination :: #raft_identity{},
    ProcedureCall :: normalized_procedure(),
    State :: #raft_state{}
) -> term().
send_rpc(Destination, Procedure, #raft_state{self = Self, current_term = Term} = State) ->
    cast(Destination, make_rpc(Self, Term, Procedure), State).

-spec broadcast_rpc(ProcedureCall :: normalized_procedure(), State :: #raft_state{}) -> term().
broadcast_rpc(ProcedureCall, #raft_state{self = Self} = State) ->
    [send_rpc(Peer, ProcedureCall, State) || Peer <- config_identities(config(State)), Peer =/= Self].

-spec cast(Destination :: #raft_identity{}, RPC :: rpc(), State :: #raft_state{}) -> ok | {error, term()}.
cast(
    #raft_identity{
        name = Name,
        node = Node
    } = Destination,
    Message,
    #raft_state{
        identifier = Identifier,
        distribution_module = Distribution
    }
) ->
    try
        ok = Distribution:cast({Name, Node}, Identifier, Message)
    catch
        _:E ->
            ?RAFT_COUNT({'raft.server.cast.error', E}),
            ?LOG_DEBUG("Cast to ~p error ~100p", [Destination, E], ?RAFT_LOG_OPTS),
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
-spec check_follower_lagging(LeaderCommit :: pos_integer(), State :: #raft_state{}) -> ok.
check_follower_lagging(
    LeaderCommit,
    #raft_state{
        application = App,
        table = Table,
        partition = Partition,
        last_applied = LastApplied
    } = Data
) ->
    Lagging = LeaderCommit - LastApplied,
    ?RAFT_GATHER('raft.follower.lagging', Lagging),
    case Lagging < ?RAFT_FOLLOWER_STALE_ENTRIES(App) of
        true ->
            wa_raft_info:get_stale(Table, Partition) =/= false andalso begin
                ?RAFT_LOG_NOTICE(follower, Data, "catches up.", []),
                wa_raft_info:set_stale(Table, Partition, false)
            end;
        false ->
            wa_raft_info:get_stale(Table, Partition) =/= true andalso begin
                ?RAFT_LOG_NOTICE(follower, Data, "is far behind ~p (leader ~p, follower ~p)",
                    [Lagging, LeaderCommit, LastApplied]),
                wa_raft_info:set_stale(Table, Partition, true)
            end
    end,
    ok.

%% Check leader state and set stale if needed
-spec check_leader_lagging(#raft_state{}) -> term().
check_leader_lagging(
    #raft_state{
        application = App,
        table = Table,
        partition = Partition,
        heartbeat_response_ts = HeartbeatResponse
    } = State
) ->
    NowTs = erlang:monotonic_time(millisecond),
    QuorumTs = compute_quorum(HeartbeatResponse#{node() => NowTs}, 0, config(State)),

    Stale = wa_raft_info:get_stale(Table, Partition),
    QuorumAge = NowTs - QuorumTs,
    MaxAge = ?RAFT_LEADER_STALE_INTERVAL(App),

    case QuorumAge >= MaxAge of
        Stale ->
            ok;
        true ->
            ?RAFT_LOG_NOTICE(leader, State, "is now stale due to last heartbeat quorum age being ~0p ms >= ~0p ms max", [QuorumAge, MaxAge]),
            wa_raft_info:set_stale(Table, Partition, true);
        false ->
            ?RAFT_LOG_NOTICE(leader, State, "is no longer stale after heartbeat quorum age drops to ~0p ms < ~0p ms max", [QuorumAge, MaxAge]),
            wa_raft_info:set_stale(Table, Partition, false)
    end.

%% Based on information that the leader has available as a result of heartbeat replies, attempt
%% to discern what the best subsequent replication mode would be for this follower.
-spec select_follower_replication_mode(
    FollowerLastIndex :: wa_raft_log:log_index(),
    State :: #raft_state{}
) -> snapshot | bulk_logs | logs.
select_follower_replication_mode(
    FollowerLastIndex,
    #raft_state{
        application = App,
        log_view = View,
        last_applied = LastAppliedIndex
    }
) ->
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
-spec request_snapshot_for_follower(Follower :: node(), State :: #raft_state{}) -> term().
request_snapshot_for_follower(
    FollowerId,
    #raft_state{
        application = App,
        name = Name,
        table = Table,
        partition = Partition
    } = State
) ->
    Witness = lists:member({Name, FollowerId}, config_witnesses(config(State))),
    wa_raft_snapshot_catchup:catchup(App, Name, FollowerId, Table, Partition, Witness).

-spec request_bulk_logs_for_follower(
    Peer :: #raft_identity{},
    FollowerLastIndex :: wa_raft_log:log_index(),
    State :: #raft_state{}
) -> ok.
request_bulk_logs_for_follower(
    Peer,
    FollowerLastIndex,
    #raft_state{
        catchup = Catchup,
        current_term = CurrentTerm,
        commit_index = CommitIndex
    } = State
) ->
    ?RAFT_LOG_DEBUG(leader, State, "requesting bulk logs catchup for follower ~0p.", [Peer]),
    wa_raft_log_catchup:start_catchup_request(Catchup, Peer, FollowerLastIndex, CurrentTerm, CommitIndex).

-spec cancel_bulk_logs_for_follower(Peer :: #raft_identity{}, State :: #raft_state{}) -> ok.
cancel_bulk_logs_for_follower(Peer, #raft_state{catchup = Catchup}) ->
    wa_raft_log_catchup:cancel_catchup_request(Catchup, Peer).

-spec follower_or_witness_state(State :: #raft_state{}) -> state().
follower_or_witness_state(State) ->
    case is_self_witness(State) of
        true -> witness;
        false -> follower
    end.

-spec candidate_or_witness_state_transition(State :: #raft_state{}) ->
    gen_statem:event_handler_result(state(), #raft_state{}).
candidate_or_witness_state_transition(#raft_state{current_term = CurrentTerm} = State) ->
    case is_self_witness(State) of
        true -> {next_state, witness, State};
        false -> {next_state, candidate, State, {next_event, internal, ?FORCE_ELECTION(CurrentTerm)}}
    end.
