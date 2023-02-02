%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file defines general macros and data structures shared across modules.

-define(APP, wa_raft).

%% Get global config
-define(RAFT_CONFIG(Name, Default), application:get_env(?APP, Name, Default)).

%% Default location containing databases for RAFT partitions part of a RAFT client application
-define(RAFT_DATABASE_PATH(Application), wa_raft_env:database_path(Application)).
%% Registered database location for the specified RAFT partition
-define(RAFT_PARTITION_PATH(Table, Partition), wa_raft_part_sup:registered_partition_path(Table, Partition)).

%% Registered name of the RAFT partition supervisor for a RAFT partition
-define(RAFT_SUPERVISOR_NAME(Table, Partition), wa_raft_part_sup:registered_name(Table, Partition)).
%% Registered name of the RAFT acceptor server for a RAFT partition
-define(RAFT_ACCEPTOR_NAME(Table, Partition), wa_raft_acceptor:registered_name(Table, Partition)).
%% Registered name of the RAFT log server for a RAFT partition
-define(RAFT_LOG_NAME(Table, Partition), wa_raft_log:registered_name(Table, Partition)).
%% Registered name of the RAFT log catchup server for a RAFT partition
-define(RAFT_LOG_CATCHUP_NAME(Table, Partition), wa_raft_log_catchup:registered_name(Table, Partition)).
%% Registered name of the RAFT server for a RAFT partition
-define(RAFT_SERVER_NAME(Table, Partition), wa_raft_server:registered_name(Table, Partition)).
%% Registered name of the RAFT storage server for a RAFT partition
-define(RAFT_STORAGE_NAME(Table, Partition), wa_raft_storage:registered_name(Table, Partition)).

%% Default distribution provider module
-define(RAFT_DEFAULT_DISTRIBUTION_MODULE, wa_raft_distribution).
%% Default log provider module
-define(RAFT_DEFAULT_LOG_MODULE, wa_raft_log_ets).
%% Default storage provider module
-define(RAFT_DEFAULT_STORAGE_MODULE, wa_raft_storage_ets).
%% Default module for handling outgoing transports
-define(RAFT_DEFAULT_TRANSPORT_MODULE, wa_raft_dist_transport).

%% RAFT election max weight
-define(RAFT_ELECTION_MAX_WEIGHT, 10).
%% Raft election default weight
-define(RAFT_ELECTION_DEFAULT_WEIGHT, ?RAFT_ELECTION_MAX_WEIGHT).

%% Name of server state persist file
-define(STATE_FILE_NAME, "state").
%% Name prefix for snapshots
-define(SNAPSHOT_PREFIX, "snapshot").
%% Snapshot name
-define(SNAPSHOT_NAME(Index, Term), (?SNAPSHOT_PREFIX "." ++ integer_to_list(Index) ++ "." ++ integer_to_list(Term))).
%% Location of a snapshot
-define(RAFT_SNAPSHOT_PATH(Table, Partition, Name), filename:join(?RAFT_PARTITION_PATH(Table, Partition), Name)).
-define(RAFT_SNAPSHOT_PATH(Table, Partition, Index, Term), ?RAFT_SNAPSHOT_PATH(Table, Partition, ?SNAPSHOT_NAME(Index, Term))).

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

%% Information about an application that has started a RAFT supervisor.
-record(raft_application, {
    % Application name
    name :: atom(),
    % Config search path
    config_search_apps :: [atom()]
}).

%% Normalized options produced by `wa_raft_part_sup` for passing into RAFT processes.
%% Not to be created externally.
-record(raft_options, {
    % General options
    application :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    witness :: boolean(),
    database :: file:filename(),

    % Acceptor options
    acceptor_name :: atom(),

    % Distribution options
    distribution_module :: module(),

    % Log options
    log_name :: atom(),
    log_module :: module(),

    % Log catchup options
    log_catchup_name :: atom(),

    % Queue options
    queue_name :: atom(),
    queue_counters :: counters:counters_ref(),
    queue_commits :: atom(),
    queue_reads :: atom(),

    % Server options
    server_name :: atom(),

    % Storage options
    storage_name :: atom(),
    storage_module :: module(),

    % Partition supervisor options
    supervisor_name :: atom(),

    % Transport options
    transport_cleanup_name :: atom(),
    transport_directory :: file:filename(),
    transport_module :: module()
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
    % Module for distribution
    distribution_module :: module(),
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
