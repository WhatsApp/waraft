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
%% Default module for log labeling
-define(RAFT_DEFAULT_LABEL_MODULE, undefined).

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

%% Witness Snapshot name
-define(WITNESS_SNAPSHOT_NAME(Index, Term), (?SNAPSHOT_PREFIX "." ++ integer_to_list(Index) ++ "." ++ integer_to_list(Term) ++ ".witness")).

%% Location of a snapshot
-define(RAFT_SNAPSHOT_PATH(Path, Name), (filename:join(Path, Name))).
-define(RAFT_SNAPSHOT_PATH(Table, Partition, Name), ?RAFT_SNAPSHOT_PATH(?RAFT_PARTITION_PATH(Table, Partition), Name)).
-define(RAFT_SNAPSHOT_PATH(Table, Partition, Index, Term), ?RAFT_SNAPSHOT_PATH(Table, Partition, ?SNAPSHOT_NAME(Index, Term))).

%% [Transport] Atomics - field index for update timestamp
-define(RAFT_TRANSPORT_ATOMICS_UPDATED_TS, 1).
%% [Transport] Transport atomics - field count
-define(RAFT_TRANSPORT_TRANSPORT_ATOMICS_COUNT, 1).
%% [Transport] File atomics - field count
-define(RAFT_TRANSPORT_FILE_ATOMICS_COUNT, 1).

-define(READ_OP, '$read').

%%-------------------------------------------------------------------
%% Metrics
%%-------------------------------------------------------------------

-define(RAFT_METRICS_MODULE_KEY, {?APP, raft_metrics_module}).
-define(RAFT_METRICS_MODULE, (persistent_term:get(?RAFT_METRICS_MODULE_KEY, wa_raft_metrics))).
-define(RAFT_COUNT(Metric), ?RAFT_METRICS_MODULE:count(Metric)).
-define(RAFT_COUNTV(Metric, Value), ?RAFT_METRICS_MODULE:countv(Metric, Value)).
-define(RAFT_GATHER(Metric, Value), ?RAFT_METRICS_MODULE:gather(Metric, Value)).
-define(RAFT_GATHER_LATENCY(Metric, Value), ?RAFT_METRICS_MODULE:gather_latency(Metric, Value)).

%%-------------------------------------------------------------------
%% Global Configuration
%%-------------------------------------------------------------------

%% Get global config
-define(RAFT_CONFIG(Name), (application:get_env(?APP, Name))).
-define(RAFT_CONFIG(Name, Default), (application:get_env(?APP, Name, Default))).

%% Default metrics module
-define(RAFT_METRICS_MODULE(), ?RAFT_CONFIG(raft_metrics_module)).

%% Default Call timeout for all cross node gen_server:call
-define(RAFT_RPC_CALL_TIMEOUT(), ?RAFT_CONFIG(raft_rpc_call_timeout, 10000)).
%% Default call timeout for storage related operation (we need bigger default since storage can be slower)
-define(RAFT_STORAGE_CALL_TIMEOUT(), ?RAFT_CONFIG(raft_storage_call_timeout, 60000)).

%% Maximum number of concurrent catchups by bulk log transfer
-define(RAFT_MAX_CONCURRENT_LOG_CATCHUP(), ?RAFT_CONFIG(raft_max_log_catchup, 5)).
%% Maximum number of concurrent catchups by snapshot transfer
-define(RAFT_MAX_CONCURRENT_SNAPSHOT_CATCHUP(), ?RAFT_CONFIG(raft_max_snapshot_catchup, 5)).
%% Maximum number of incoming snapshots by snapshot transfer.
-define(RAFT_MAX_CONCURRENT_INCOMING_SNAPSHOT_TRANSFERS(), ?RAFT_CONFIG(raft_max_incoming_snapshot_transfers, 10)).
%% Maximum number of incoming witness snapshots by snapshot transfer.
-define(RAFT_MAX_CONCURRENT_INCOMING_WITNESS_SNAPSHOT_TRANSFERS(), ?RAFT_CONFIG(raft_max_incoming_witness_snapshot_transfers, 16)).

%% Default cross-node call timeout for heartbeats made for bulk logs catchup
-define(RAFT_CATCHUP_HEARTBEAT_TIMEOUT(), ?RAFT_CONFIG(raft_catchup_rpc_timeout_ms, 5000)).

%% Number of workers to use for transports
-define(RAFT_TRANSPORT_THREADS(), ?RAFT_CONFIG(raft_transport_threads, 1)).
%% Time in seconds after which a transport that has not made progress should be considered failed
-define(RAFT_TRANSPORT_IDLE_TIMEOUT(), ?RAFT_CONFIG(transport_idle_timeout_secs, 30)).

%% Maximum number of previous inactive transports to retain info for.
-define(RAFT_TRANSPORT_INACTIVE_INFO_LIMIT(), ?RAFT_CONFIG(raft_transport_inactive_info_limit, 30)).

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
%% Maximum number of total log entries from the leader's current log that a
%% peer has not yet confirmed to be applied. This limit helps prevent nodes who
%% may have already received all the current log entries but are behind in
%% actually applying them to the underlying storage from becoming leader due to
%% handover before they are ready. This defaults to equal to the maximum number
%% of missing log entries. (See `?RAFT_HANDOVER_MAX_ENTRIES`.)
-define(RAFT_HANDOVER_MAX_UNAPPLIED_ENTRIES, raft_handover_max_unapplied_entries).
-define(RAFT_HANDOVER_MAX_UNAPPLIED_ENTRIES(App), ?RAFT_APP_CONFIG(App, ?RAFT_HANDOVER_MAX_UNAPPLIED_ENTRIES, undefined)).
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
% Time to wait before retrying snapshot transport to a overloaded peer.
-define(RAFT_SNAPSHOT_CATCHUP_OVERLOADED_BACKOFF_MS, snapshot_catchup_overloaded_backoff_ms).
-define(RAFT_SNAPSHOT_CATCHUP_OVERLOADED_BACKOFF_MS(App), ?RAFT_APP_CONFIG(App, ?RAFT_SNAPSHOT_CATCHUP_OVERLOADED_BACKOFF_MS, 1000)).
% Time to wait before allowing a rerun of a completed snapshot transport.
-define(RAFT_SNAPSHOT_CATCHUP_COMPLETED_BACKOFF_MS, raft_snapshot_catchup_completed_backoff_ms).
-define(RAFT_SNAPSHOT_CATCHUP_COMPLETED_BACKOFF_MS(App), ?RAFT_APP_CONFIG(App, ?RAFT_SNAPSHOT_CATCHUP_COMPLETED_BACKOFF_MS, 20 * 1000)).
% Time to wait before allowing a rerun of a failed snapshot transport.
-define(RAFT_SNAPSHOT_CATCHUP_FAILED_BACKOFF_MS, raft_snapshot_catchup_failed_backoff_ms).
-define(RAFT_SNAPSHOT_CATCHUP_FAILED_BACKOFF_MS(App), ?RAFT_APP_CONFIG(App, ?RAFT_SNAPSHOT_CATCHUP_FAILED_BACKOFF_MS, 10 * 1000)).

%% Number of omitted log entries to skip actually applying to storage when
%% operating as a witness.
-define(RAFT_STORAGE_WITNESS_APPLY_INTERVAL, raft_storage_witness_apply_interval).
-define(RAFT_STORAGE_WITNESS_APPLY_INTERVAL(App), ?RAFT_APP_CONFIG(App, ?RAFT_STORAGE_WITNESS_APPLY_INTERVAL, 100)).

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

%% Log handle.
-record(raft_log, {
    name :: wa_raft_log:log_name(),
    application :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition(),
    provider :: module()
}).

%% This record represents the identity of a RAFT replica, usable to
%% distinguish different RAFT replicas from one another. This record
%% is not guaranteed to remain structurally compatible between versions
%% of RAFT and so should not be persisted between runtimes nor sent
%% between RAFT servers. It is generally allowed to inspect the fields
%% of this record, however, similarly, this record is subject to change
%% at any time.
-record(raft_identity, {
    % The service name (registered name) of the RAFT server that this
    % identity record refers to.
    name :: atom(),
    % The node that the RAFT server that this identity record refers
    % to is located on.
    node :: node()
}).

%% This record represents a RAFT instance identifier.
-record(raft_identifier, {
    application :: atom(),
    table :: wa_raft:table(),
    partition :: wa_raft:partition()
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
    self :: #raft_identity{},
    identifier :: #raft_identifier{},
    database :: file:filename(),

    % Acceptor options
    acceptor_name :: atom(),

    % Distribution options
    distribution_module :: module(),

    % Label options
    label_module :: undefined | module(),

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
    %% Application
    application :: atom(),
    %% Service name
    name :: atom(),
    %% Self identity
    self :: #raft_identity{},
    %% Raft instance identifier
    identifier :: #raft_identifier{},
    %% Table name
    table :: wa_raft:table(),
    %% Partition
    partition :: wa_raft:partition(),
    %% Data dir
    data_dir :: string(),
    %% Log handle and view
    log_view :: wa_raft_log:view(),
    %% Module for distribution
    distribution_module :: module(),
    %% Module for log labeling
    label_module :: module() | undefined,
    %% Storage service name
    storage :: atom(),
    %% Catchup service name
    catchup :: atom(),

    %% The index of the latest log entry that is committed by the cluster
    commit_index = 0 :: non_neg_integer(),

    %% The index of the latest log entry that has been sent to storage to be applied
    last_applied = 0 :: non_neg_integer(),

    %% currently cached RAFT configuration and its index
    %%  * at least the most recently applied RAFT configuration
    cached_config :: undefined | {wa_raft_log:log_index(), wa_raft_server:config()},
    % Log label from the last log entry submitted (pending) / appended (persisted)
    % to RAFT log (the field is only relevant in leader state).
    last_label :: undefined | term(),
    % Timestamp of last heartbeat from leader
    leader_heartbeat_ts :: undefined | integer(),

    %% The current RAFT term as locally determined
    current_term = 0 :: non_neg_integer(),
    %% The peer that got my vote in the current term
    voted_for :: undefined | node(),
    %% The affirmative votes this replica received from the cluster in the current term
    votes = #{} :: #{node() => true},
    %% The leader of the current RAFT term if known
    leader_id :: undefined | node(),

    %% Timestamp in milliseconds (monotonic) of the start of the current state
    state_start_ts :: non_neg_integer(),

    %% [Leader] Mapping from peer to the index of the first log entry to send in the next heartbeat
    next_indices = #{} :: #{node() => wa_raft_log:log_index()},
    %% [Leader] Mapping from peer to the index of the latest log entry in the peer's log known to match the leader's log
    match_indices = #{} :: #{node() => wa_raft_log:log_index()},
    %% [Leader] Mapping from peer to the index of the latest log entry that the peer has applied
    last_applied_indices = #{} :: #{node() => wa_raft_log:log_index()},

    %% last timestamp in ms when we send heartbeat
    last_heartbeat_ts = #{} :: #{node() => integer()},
    %% Timestamps in milliseconds of last time each follower responded successfully to a heartbeat
    heartbeat_response_ts = #{} :: #{node() => integer()},
    first_current_term_log_index = 0 :: wa_raft_log:log_index(),
    handover :: undefined | {node(), reference(), integer()},

    %% disabled
    disable_reason :: term()
}).
