%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file defines general macros and data structures shared across modules.

-define(APP, wa_raft).

%% Get global config
-define(RAFT_CONFIG(Name, Default), application:get_env(?APP, Name, Default)).

%% DB
-define(DB, application:get_env(?APP, db, missing)).
%% Persistent root directory
-define(ROOT_DIR(Table, Partition), lists:concat([?DB, "/", Table, ".", Partition, "/"])).

-define(TO_ATOM(Prefix, Table, Partition), list_to_atom(lists:concat([Prefix, Table, "_" , Partition]))).
%% Raft server name
-define(RAFT_SERVER_NAME(Table, Partition), ?TO_ATOM("raft_server_", Table, Partition)).
%% Raft log service
-define(RAFT_LOG_NAME(Table, Partition), ?TO_ATOM("raft_log_", Table, Partition)).
%% Raft storage service
-define(RAFT_STORAGE_NAME(Table, Partition), ?TO_ATOM("raft_storage_", Table, Partition)).
%% Raft acceptor service
-define(RAFT_ACCEPTOR_NAME(Table, Partition), ?TO_ATOM("raft_acceptor_", Table, Partition)).
%% RAFT log catchup process
-define(RAFT_LOG_CATCHUP(Table, Partition), ?TO_ATOM("raft_log_catchup_", Table, Partition)).

%% RAFT election max weight
-define(RAFT_ELECTION_MAX_WEIGHT, 10).
%% Raft election default weight
-define(RAFT_ELECTION_DEFAULT_WEIGHT, ?RAFT_ELECTION_MAX_WEIGHT).

%% Current db directory
-define(DATA, "db").
%% Data directory
-define(DATA_DIR(Table, Partition), ?ROOT_DIR(Table, Partition) ++ ?DATA ++ "/").
%% Name of server state persist file
-define(STATE_FILE_NAME, "state").
%% Name prefix for snapshots
-define(SNAPSHOT_PREFIX, "snapshot").
%% Snapshot name
-define(SNAPSHOT_NAME(Index, Term), lists:concat([?SNAPSHOT_PREFIX, ".", Index,  ".", Term])).
%% Default Call timeout for all cross node gen_server:call
-define(RPC_CALL_TIMEOUT_MS, ?RAFT_CONFIG(raft_rpc_call_timeout, 30000)).
%% Default call timeout for storage related operation (we need bigger default since storage can be slower)
-define(STORAGE_CALL_TIMEOUT_MS, ?RAFT_CONFIG(raft_storage_call_timeout, 60000)).
%% Counters
-define(RAFT_COUNTERS, raft_counters).
%% Number of counters
-define(RAFT_NUMBER_OF_GLOBAL_COUNTERS, 2).
%% Counter - number of snapshot catchup processes
-define(RAFT_GLOBAL_COUNTER_SNAPSHOT_CATCHUP, 1).
%% Counter - number of log catchup processes
-define(RAFT_GLOBAL_COUNTER_LOG_CATCHUP, 2).


%% [Transport] Atomics - field index for update timestamp
-define(RAFT_TRANSPORT_ATOMICS_UPDATED_TS, 1).
%% [Transport] Transport atomics - field count
-define(RAFT_TRANSPORT_TRANSPORT_ATOMICS_COUNT, 1).
%% [Transport] File atomics - field count
-define(RAFT_TRANSPORT_FILE_ATOMICS_COUNT, 1).

-define(READ_OP, '$read').

%% Raft minimum election timeout
-define(RAFT_ELECTION_TIMEOUT_MS(), ?RAFT_CONFIG(raft_election_timeout_ms, 5000)).
%% Raft maximum election timeout
-define(RAFT_ELECTION_TIMEOUT_MS_MAX(), ?RAFT_CONFIG(raft_election_timeout_ms_max, 7500)).

%% Current version of RAFT config
-define(RAFT_CONFIG_CURRENT_VERSION, 1).

%% Metrics
-define(RAFT_METRICS_MODULE_KEY, {?APP, raft_metrics_module}).
-define(RAFT_METRICS_MODULE, (persistent_term:get(?RAFT_METRICS_MODULE_KEY, wa_raft_metrics))).
-define(RAFT_COUNT(Metric), ?RAFT_METRICS_MODULE:count(Metric)).
-define(RAFT_COUNTV(Metric, Value), ?RAFT_METRICS_MODULE:countv(Metric, Value)).
-define(RAFT_GATHER(Metric, Value), ?RAFT_METRICS_MODULE:gather(Metric, Value)).

%% Distribution
-define(RAFT_DISTRIBUTION_MODULE, (persistent_term:get(raft_distribution_module))).

%% Normalized options produced by `wa_raft_part_sup` for passing into RAFT processes.
%% Not to be created externally.
-record(raft_options, {
    application :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    witness :: boolean(),
    log_module :: module(),
    storage_module :: module()
}).

%% Log position
-record(raft_log_pos, {
    %% log sequence number
    index = 0 :: wa_raft_log:log_index(),
    %% leader's term when log entry is created
    term = 0 :: wa_raft_log:log_term()
}).

%% This record contains the identity of a RAFT server. The intent is
%% for the RAFT server implementation to avoid destructuring this
%% record as much as possible to reduce the code change required if
%% the details about the identity of peers changes. This record
%% should not be sent between RAFT servers as it is not guaranteed to
%% be fixed between versions.
-record(raft_identity, {
    % The service name (registered name) of the RAFT server that this
    % identity record refers to.
    name :: atom(),
    % The node that the RAFT server that this identity record refers
    % to is located on.
    node :: node()
}).

%% Raft runtime state
-record(raft_state, {
    % Service name
    name :: atom(),
    % Self identity
    self :: #raft_identity{},
    % Table name
    table :: wa_raft:table(),
    % Partition
    partition :: wa_raft:partition(),
    % Data dir
    data_dir :: string(),
    % Log handle and view
    log_view :: wa_raft_log:view(),
    % Storage service name
    storage :: atom(),
    % Catchup service name
    catchup :: atom(),

    % Current term
    current_term = 0 :: non_neg_integer(),
    % Candidate who got my vote in current term
    voted_for :: undefined | node(),
    % Log index that committed
    commit_index = 0 :: non_neg_integer(),
    % Log index that applied to storage
    last_applied = 0 :: non_neg_integer(),

    % currently cached RAFT configuration and its index
    %  * at least the most recently applied RAFT configuration
    cached_config :: undefined | {wa_raft_log:log_index(), wa_raft_server:config()},

    %% leader
    next_index = maps:new() :: #{node() => non_neg_integer()},
    match_index = maps:new() :: #{node() => non_neg_integer()},
    %% last timestamp in ms when we send heartbeat
    last_heartbeat_ts = maps:new() :: #{node() => non_neg_integer()},
    %% Timestamps in milliseconds of last time each follower responded successfully to a heartbeat
    heartbeat_response_ts = maps:new() :: #{node() => non_neg_integer()},
    first_current_term_log_index = 0 :: wa_raft_log:log_index(),
    handover :: undefined | {node(), reference(), integer()},

    %% follower
    leader_id :: undefined | node(),
    % Timestamp of last heartbeat from leader
    leader_heartbeat_ts :: undefined | pos_integer(),

    %% candidate
    %% Timestamp (ms) of when the election started (server entered candidate state)
    election_start_ts :: undefined | erlang:timestamp(),
    %% The type (normal = heartbeat/election timeout, force = handover) of the next election
    next_election_type = normal :: normal | force,
    %% The set of votes that this candidate has received from the cluster so far.
    votes = maps:new() :: #{node() => boolean()},

    %% disabled
    disable_reason :: term(),

    %% witness
    witness :: boolean()
}).

%% Storage state
-record(raft_storage, {
    % Service name
    name :: atom(),
    % Table name
    table :: wa_raft:table(),
    % Partition
    partition :: wa_raft:partition(),
    % Root dir for
    root_dir :: string(),
    % Callback module
    module :: module(),
    % Storage handle
    handle :: wa_raft_storage:storage_handle(),

    % Last applied position
    last_applied = #raft_log_pos{} :: wa_raft_log:log_pos()
}).

% Snapshot
-record(raft_snapshot, {
    % Snapshot name
    name :: string(),
    % Last applied log pos
    last_applied :: wa_raft_log:log_pos()
 }).
