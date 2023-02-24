%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file defines general macros and data structures shared across modules.

-define(APP, wa_raft).

%%-------------------------------------------------------------------
%% Registered information about applications and partitions
%%-------------------------------------------------------------------

%% Default location containing databases for RAFT partitions part of a RAFT client application
-define(RAFT_DATABASE_PATH(Application), (wa_raft_env:database_path(Application))).
%% Registered database location for the specified RAFT partition
-define(RAFT_PARTITION_PATH(Table, Partition), (wa_raft_part_sup:registered_partition_path(Table, Partition))).

%% Registered name of the RAFT partition supervisor for a RAFT partition
-define(RAFT_SUPERVISOR_NAME(Table, Partition), (wa_raft_part_sup:registered_name(Table, Partition))).
%% Registered name of the RAFT acceptor server for a RAFT partition
-define(RAFT_ACCEPTOR_NAME(Table, Partition), (wa_raft_acceptor:registered_name(Table, Partition))).
%% Registered name of the RAFT log server for a RAFT partition
-define(RAFT_LOG_NAME(Table, Partition), (wa_raft_log:registered_name(Table, Partition))).
%% Registered name of the RAFT log catchup server for a RAFT partition
-define(RAFT_LOG_CATCHUP_NAME(Table, Partition), (wa_raft_log_catchup:registered_name(Table, Partition))).
%% Registered name of the RAFT server for a RAFT partition
-define(RAFT_SERVER_NAME(Table, Partition), (wa_raft_server:registered_name(Table, Partition))).
%% Registered name of the RAFT storage server for a RAFT partition
-define(RAFT_STORAGE_NAME(Table, Partition), (wa_raft_storage:registered_name(Table, Partition))).

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
-define(RAFT_SNAPSHOT_PATH(Path, Name), (filename:join(Path, Name))).
-define(RAFT_SNAPSHOT_PATH(Table, Partition, Name), ?RAFT_SNAPSHOT_PATH(?RAFT_PARTITION_PATH(Table, Partition), Name)).
-define(RAFT_SNAPSHOT_PATH(Table, Partition, Index, Term), ?RAFT_SNAPSHOT_PATH(Table, Partition, ?SNAPSHOT_NAME(Index, Term))).

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

%% Current version of RAFT config
-define(RAFT_CONFIG_CURRENT_VERSION, 1).

%%-------------------------------------------------------------------
%% Metrics
%%-------------------------------------------------------------------

-define(RAFT_METRICS_MODULE_KEY, {?APP, raft_metrics_module}).
-define(RAFT_METRICS_MODULE, (persistent_term:get(?RAFT_METRICS_MODULE_KEY, wa_raft_metrics))).
-define(RAFT_COUNT(Metric), ?RAFT_METRICS_MODULE:count(Metric)).
-define(RAFT_COUNTV(Metric, Value), ?RAFT_METRICS_MODULE:countv(Metric, Value)).
-define(RAFT_GATHER(Metric, Value), ?RAFT_METRICS_MODULE:gather(Metric, Value)).

%%-------------------------------------------------------------------
%% Global Configuration
%%-------------------------------------------------------------------

%% Get global config
-define(RAFT_CONFIG(Name), (application:get_env(?APP, Name))).
-define(RAFT_CONFIG(Name, Default), (application:get_env(?APP, Name, Default))).

%% Default metrics module
-define(RAFT_METRICS_MODULE(), ?RAFT_CONFIG(raft_metrics_module)).

%% Default Call timeout for all cross node gen_server:call
-define(RAFT_RPC_CALL_TIMEOUT(), ?RAFT_CONFIG(raft_rpc_call_timeout, 30000)).
%% Default call timeout for storage related operation (we need bigger default since storage can be slower)
-define(RAFT_STORAGE_CALL_TIMEOUT(), ?RAFT_CONFIG(raft_storage_call_timeout, 60000)).

%% Maximum number of concurrent catchups by bulk log transfer
-define(RAFT_MAX_CONCURRENT_LOG_CATCHUP(), ?RAFT_CONFIG(raft_max_log_catchup, 5)).
%% Maximum number of concurrent catchups by snapshot transfer
-define(RAFT_MAX_CONCURRENT_SNAPSHOT_CATCHUP(), ?RAFT_CONFIG(raft_max_snapshot_catchup, 5)).

%% Default cross-node call timeout for heartbeats made for bulk logs catchup
-define(RAFT_CATCHUP_HEARTBEAT_TIMEOUT(), ?RAFT_CONFIG(raft_catchup_rpc_timeout_ms, 5000)).

%% Number of workers to use for transports
-define(RAFT_TRANSPORT_THREADS(), ?RAFT_CONFIG(raft_transport_threads, 1)).
%% Time in seconds after which a transport that has not made progress should be considered failed
-define(RAFT_TRANSPORT_IDLE_TIMEOUT(), ?RAFT_CONFIG(transport_idle_timeout_secs, 30)).

%% Size in bytes of individual chunks (messages containing file data) to be sent during transports
%% using the dist transport provider
-define(RAFT_DIST_TRANSPORT_CHUNK_SIZE(), ?RAFT_CONFIG(dist_transport_chunk_size, 1 * 1024 * 1024)).
%% Maximum number of chunks that can be sent by the dist transport provider without being
%% acknowledged by the recipient
-define(RAFT_DIST_TRANSPORT_MAX_INFLIGHT(), ?RAFT_CONFIG(dist_transport_max_inflight, 4)).

%%-------------------------------------------------------------------
%% Application-specific Configuration
%%-------------------------------------------------------------------

%% Get application-scoped config
-define(RAFT_APP_CONFIG(App, Name, Default), (wa_raft_env:get_env(App, Name, Default))).

%% Maximum number of pending applies for any single RAFT partition
-define(RAFT_MAX_PENDING_APPLIES, raft_max_pending_applies).
-define(RAFT_MAX_PENDING_APPLIES(App), ?RAFT_APP_CONFIG(App, {?RAFT_MAX_PENDING_APPLIES, raft_apply_queue_max_size}, 1000)).
%% Maximum number of pending commits for any single RAFT partition
-define(RAFT_MAX_PENDING_COMMITS, raft_max_pending_commits).
-define(RAFT_MAX_PENDING_COMMITS(App), ?RAFT_APP_CONFIG(App, ?RAFT_MAX_PENDING_COMMITS, 1500)).
%% Maximum number of pending reads for any single RAFT partition
-define(RAFT_MAX_PENDING_READS, raft_max_pending_reads).
-define(RAFT_MAX_PENDING_READS(App), ?RAFT_APP_CONFIG(App, ?RAFT_MAX_PENDING_READS, 5000)).

%% Whether or not this node is eligible to be leader.
-define(RAFT_LEADER_ELIGIBLE, raft_leader_eligible).
-define(RAFT_LEADER_ELIGIBLE(App), (?RAFT_APP_CONFIG(App, ?RAFT_LEADER_ELIGIBLE, true) =/= false)).
%% Time in milliseconds during which a leader was unable to replicate heartbeats to a
%% quorum of followers before considering the leader to be stale.
-define(RAFT_LEADER_STALE_INTERVAL, raft_max_heartbeat_age_msecs).
-define(RAFT_LEADER_STALE_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_LEADER_STALE_INTERVAL, 180 * 1000)).
%% Relative "weight" at which this node will trigger elections and thereby be elected.
-define(RAFT_ELECTION_WEIGHT, raft_election_weight).
-define(RAFT_ELECTION_WEIGHT(App), ?RAFT_APP_CONFIG(App, ?RAFT_ELECTION_WEIGHT, ?RAFT_ELECTION_DEFAULT_WEIGHT)).
%% Interval in milliseconds between heartbeats sent by RAFT leaders with no pending log entries
-define(RAFT_HEARTBEAT_INTERVAL, raft_heartbeat_interval_ms).
-define(RAFT_HEARTBEAT_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_HEARTBEAT_INTERVAL, 120)).
%% Maximum number of log entries to include in a single heartbeat
-define(RAFT_HEARTBEAT_MAX_ENTRIES, raft_max_log_entries_per_heartbeat).
-define(RAFT_HEARTBEAT_MAX_ENTRIES(App), ?RAFT_APP_CONFIG(App, ?RAFT_HEARTBEAT_MAX_ENTRIES, 15)).
%% Maximum number of log entries to include in a single heartbeat to a witness
-define(RAFT_HEARTBEAT_MAX_ENTRIES_TO_WITNESS, raft_max_witness_log_entries_per_heartbeat).
-define(RAFT_HEARTBEAT_MAX_ENTRIES_TO_WITNESS(App), ?RAFT_APP_CONFIG(App, ?RAFT_HEARTBEAT_MAX_ENTRIES_TO_WITNESS, 250)).
%% Maximum bytes of log entries to include in a single heartbeat
-define(RAFT_HEARTBEAT_MAX_BYTES, raft_max_heartbeat_size).
-define(RAFT_HEARTBEAT_MAX_BYTES(App), ?RAFT_APP_CONFIG(App, ?RAFT_HEARTBEAT_MAX_BYTES, 1 * 1024 * 1024)).
%% Time in milliseconds to wait to collect pending log entries into a single heartbeat before
%% triggering a heartbeat due to having pending log entries
-define(RAFT_COMMIT_BATCH_INTERVAL, raft_commit_batch_interval_ms).
-define(RAFT_COMMIT_BATCH_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_COMMIT_BATCH_INTERVAL, 2)).
%% Maximum number of pending log entries to collect before a heartbeat is forced. This should
%% be at most equal to the maximum number of log entries permitted per heartbeat.
-define(RAFT_COMMIT_BATCH_MAX_ENTRIES, raft_commit_batch_max).
-define(RAFT_COMMIT_BATCH_MAX_ENTRIES(App), ?RAFT_APP_CONFIG(App, ?RAFT_COMMIT_BATCH_MAX_ENTRIES, 15)).
%% Maximum number of log entries to speculatively retain in the log due to followers
%% not yet reporting having replicated the log entry locally
-define(RAFT_MAX_RETAINED_ENTRIES, raft_max_retained_entries).
-define(RAFT_MAX_RETAINED_ENTRIES(App), ?RAFT_APP_CONFIG(App, {?RAFT_MAX_RETAINED_ENTRIES, max_log_rotate_delay}, 1500000)).

%% Maximum number of log entries to queue for application by storage at once before
%% continuing to process the incoming message queue on the RAFT server.
-define(RAFT_MAX_CONSECUTIVE_APPLY_ENTRIES, raft_apply_log_batch_size).
-define(RAFT_MAX_CONSECUTIVE_APPLY_ENTRIES(App), ?RAFT_APP_CONFIG(App, ?RAFT_MAX_CONSECUTIVE_APPLY_ENTRIES, 200)).
%% Maximum bytes of log entries to queue for application by storage at once before
%% continuing to process the incoming message queue on the RAFT server.
-define(RAFT_MAX_CONSECUTIVE_APPLY_BYTES, raft_apply_batch_max_bytes).
-define(RAFT_MAX_CONSECUTIVE_APPLY_BYTES(App), ?RAFT_APP_CONFIG(App, ?RAFT_MAX_CONSECUTIVE_APPLY_BYTES, 200 * 4 * 1024)).

%% Minimum time in milliseconds since the receiving the last valid leader heartbeat
%% before triggering a new election due to term timeout. This time should be much
%% greater than the maximum expected network delay.
-define(RAFT_ELECTION_TIMEOUT_MIN, raft_election_timeout_ms).
-define(RAFT_ELECTION_TIMEOUT_MIN(App), ?RAFT_APP_CONFIG(App, ?RAFT_ELECTION_TIMEOUT_MIN, 5000)).
%% Maximum time in milliseconds since the receiving the last valid leader heartbeat
%% before triggering a new election due to term timeout. The difference between this
%% time and the minimum election timeout should be much greater than the expected
%% variance in network delay.
-define(RAFT_ELECTION_TIMEOUT_MAX, raft_election_timeout_ms_max).
-define(RAFT_ELECTION_TIMEOUT_MAX(App), ?RAFT_APP_CONFIG(App, ?RAFT_ELECTION_TIMEOUT_MAX, 7500)).

%% Maximum time in milliseconds for which no valid heartbeat was received from a leader
%% before considering a follower stale
-define(RAFT_FOLLOWER_STALE_INTERVAL, raft_follower_heartbeat_stale_ms).
-define(RAFT_FOLLOWER_STALE_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_FOLLOWER_STALE_INTERVAL, 5000)).
%% Maximum number of unapplied log entries compared to the commited log entry before
%% considering a follower stale
-define(RAFT_FOLLOWER_STALE_ENTRIES, raft_follower_max_lagging).
-define(RAFT_FOLLOWER_STALE_ENTRIES(App), ?RAFT_APP_CONFIG(App, ?RAFT_FOLLOWER_STALE_ENTRIES, 5000)).

%% Minium amount of time in seconds since the last successfully received
%% heartbeat from a leader of a term for non-forced promotion to be allowed.
-define(RAFT_PROMOTION_GRACE_PERIOD, raft_promotion_grace_period_secs).
-define(RAFT_PROMOTION_GRACE_PERIOD(App), ?RAFT_APP_CONFIG(App, ?RAFT_PROMOTION_GRACE_PERIOD, 60)).

%% Maximum number of log entries to include in a Handover RPC to pass
%% leadership to another peer. A limit is enforced to prevent a handover
%% trying to send huge numbers of logs to catchup a peer during handover.
-define(RAFT_HANDOVER_MAX_ENTRIES, raft_max_handover_log_entries).
-define(RAFT_HANDOVER_MAX_ENTRIES(App), ?RAFT_APP_CONFIG(App, ?RAFT_HANDOVER_MAX_ENTRIES, 200)).
%% Maximum total byte size of log entries to include in a Handover RPC.
-define(RAFT_HANDOVER_MAX_BYTES, raft_max_handover_log_size).
-define(RAFT_HANDOVER_MAX_BYTES(App), ?RAFT_APP_CONFIG(App, ?RAFT_HANDOVER_MAX_BYTES, 50 * 1024 * 1024)).
%% Time in milliseconds to wait before considering a previously triggered handover failed.
-define(RAFT_HANDOVER_TIMEOUT, raft_handover_timeout_ms).
-define(RAFT_HANDOVER_TIMEOUT(App), ?RAFT_APP_CONFIG(App, ?RAFT_HANDOVER_TIMEOUT, 600)).

%% Minimum nubmer of log entries past the minimum kept by the RAFT server before triggering
%% log rotation
-define(RAFT_LOG_ROTATION_INTERVAL, raft_max_log_records_per_file).
-define(RAFT_LOG_ROTATION_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_LOG_ROTATION_INTERVAL, 200000)).
%% Maximum number of log entries past the minimum kept by the RAFT server to retain in
%% the log after rotation
-define(RAFT_LOG_ROTATION_KEEP, raft_max_log_records).
-define(RAFT_LOG_ROTATION_KEEP(App, Interval), ?RAFT_APP_CONFIG(App, ?RAFT_LOG_ROTATION_KEEP, Interval * 10)).
%% Whether log rotation should be controlled by local log length or by
%% leader-announced cluster trimming index
-define(RAFT_LOG_ROTATION_BY_TRIM_INDEX, raft_rotate_by_trim_index).
-define(RAFT_LOG_ROTATION_BY_TRIM_INDEX(App), (?RAFT_APP_CONFIG(App, {?RAFT_LOG_ROTATION_BY_TRIM_INDEX, use_trim_index}, false) =:= true)).

%% Whether or not the RAFT server should use any special catchup strategy to bring peers back in sync.
-define(RAFT_CATCHUP_ENABLED, raft_catchup_enabled).
-define(RAFT_CATCHUP_ENABLED(App), (?RAFT_APP_CONFIG(App, {?RAFT_CATCHUP_ENABLED, catchup_enabled}, true) =/= false)).
%% Minimum number of log entries after which RAFT servers should use bulk logs catchup to bring peers
%% back into sync if enabled.
-define(RAFT_CATCHUP_THRESHOLD, raft_catchup_threshold).
-define(RAFT_CATCHUP_THRESHOLD(App), ?RAFT_APP_CONFIG(App, {?RAFT_CATCHUP_THRESHOLD, catchup_max_follower_lag}, 50000)).
%% Maximum log entries per heartbeat for catchup by bulk log transfer
-define(RAFT_CATCHUP_MAX_ENTRIES_PER_BATCH, raft_catchup_log_batch_entries).
-define(RAFT_CATCHUP_MAX_ENTRIES_PER_BATCH(App), ?RAFT_APP_CONFIG(App, ?RAFT_CATCHUP_MAX_ENTRIES_PER_BATCH, 800)).
%% Maximum bytes per heartbeat for catchup by bulk log transfer
-define(RAFT_CATCHUP_MAX_BYTES_PER_BATCH, raft_catchup_log_batch_bytes).
-define(RAFT_CATCHUP_MAX_BYTES_PER_BATCH(App), ?RAFT_APP_CONFIG(App, ?RAFT_CATCHUP_MAX_BYTES_PER_BATCH, 4 * 1024 * 1024)).

%% Time in seconds to retain transport destination directories after use
-define(RAFT_TRANSPORT_RETAIN_INTERVAL, transport_retain_min_secs).
-define(RAFT_TRANSPORT_RETAIN_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_TRANSPORT_RETAIN_INTERVAL, 300)).

%%-------------------------------------------------------------------
%% Records
%%-------------------------------------------------------------------

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

%%-------------------------------------------------------------------
%% Records for registered application and partition information
%%-------------------------------------------------------------------

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
    self :: #raft_identity{},
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

%%-------------------------------------------------------------------
%% Internal server states
%%-------------------------------------------------------------------

%% Raft runtime state
-record(raft_state, {
    % Application
    application :: atom(),
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
