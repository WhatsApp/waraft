%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% OTP Supervisor for monitoring RAFT processes. Correctness of RAFT
%%% relies on the consistency of the signaling between the processes,
%%% this supervisor is configured to restart all RAFT processes
%%% when any of them exits abnormally.

-module(wa_raft_part_sup).
-compile(warn_missing_spec_all).
-behaviour(supervisor).

%% OTP Supervision
-export([
    child_spec/1,
    child_spec/2,
    start_link/2
]).

%% Internal API
-export([
    default_name/2,
    default_partition_path/3,
    registered_name/2,
    registered_partition_path/2
]).

%% Internal API
-export([
    options/2
]).

%% Supervisor callbacks
-export([
    init/1
]).

%% Test API
-export([
    prepare_spec/2
]).

-include_lib("wa_raft/include/wa_raft.hrl").
-include_lib("wa_raft/include/wa_raft_logger.hrl").

%% Key in persistent_term for the options associated with a RAFT partition.
-define(OPTIONS_KEY(Table, Partition), {?MODULE, Table, Partition}).

%%-------------------------------------------------------------------
%% OTP supervision
%%-------------------------------------------------------------------

%% Returns a spec suitable for use with a `simple_one_for_one` supervisor.
-spec child_spec(Application :: atom()) -> supervisor:child_spec().
child_spec(Application) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Application]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec child_spec(Application :: atom(), Spec :: wa_raft:args()) -> supervisor:child_spec().
child_spec(Application, Spec) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Application, Spec]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link(Application :: atom(), Spec :: wa_raft:args()) -> supervisor:startlink_ret().
start_link(Application, Spec) ->
    %% First normalize the provided specification into a full options record.
    Options = #raft_options{table = Table, partition = Partition, supervisor_name = Name} = normalize_spec(Application, Spec),

    %% Then put the declared options for the current RAFT partition into
    %% persistent term for access by shared resources and other services.
    %% The RAFT options for a table are not expected to change during the
    %% runtime of the RAFT application and so repeated updates should not
    %% result in any GC load. Warn if this is case.
    PrevOptions = wa_pt:get(?OPTIONS_KEY(Table, Partition), Options),
    PrevOptions =/= Options andalso
        ?RAFT_LOG_WARNING(
            ?MODULE_STRING " storing changed options for RAFT partitition ~0p/~0p",
            [Table, Partition]
        ),
    ok = wa_pt:put(?OPTIONS_KEY(Table, Partition), Options),

    supervisor:start_link({local, Name}, ?MODULE, Options).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT partition supervisor associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the default location for the database directory associated with the
%% provided RAFT partition given the database of the RAFT root.
-spec default_partition_path(Root :: file:filename(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Database :: file:filename().
default_partition_path(Root, Table, Partition) ->
    filename:join(Root, atom_to_list(Table) ++ "." ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT partition supervisor associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.supervisor_name
    end.

%% Get the registered database directory for the provided RAFT partition. An
%% error is raised if no registration exists.
-spec registered_partition_path(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Database :: file:filename().
registered_partition_path(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> error({not_registered, Table, Partition});
        Options   -> Options#raft_options.database
    end.

-spec options(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> #raft_options{} | undefined.
options(Table, Partition) ->
    wa_pt:get(?OPTIONS_KEY(Table, Partition), undefined).

-spec normalize_spec(Application :: atom(), Spec :: wa_raft:args()) -> #raft_options{}.
normalize_spec(Application, #{table := Table, partition := Partition} = Spec) ->
    Root = wa_raft_env:database_path(Application),
    Database = default_partition_path(Root, Table, Partition),
    ServerName = wa_raft_server:default_name(Table, Partition),
    LogName = wa_raft_log:default_name(Table, Partition),
    StorageName = wa_raft_storage:default_name(Table, Partition),
    AcceptorName = wa_raft_acceptor:default_name(Table, Partition),
    #raft_options{
        application = Application,
        table = Table,
        partition = Partition,
        % RAFT identity always uses the default RAFT server name for the partition
        self = #raft_identity{name = wa_raft_server:default_name(Table, Partition), node = node()},
        identifier = #raft_identifier{application = Application, table = Table, partition = Partition},
        database = Database,
        acceptor_name = AcceptorName,
        distribution_module = maps:get(distribution_module, Spec, wa_raft_env:get_env(Application, raft_distribution_module, ?RAFT_DEFAULT_DISTRIBUTION_MODULE)),
        log_name = LogName,
        log_module = maps:get(log_module, Spec, wa_raft_env:get_env(Application, raft_log_module, ?RAFT_DEFAULT_LOG_MODULE)),
        label_module = maps:get(label_module, Spec, wa_raft_env:get_env(Application, raft_label_module, ?RAFT_DEFAULT_LABEL_MODULE)),
        log_catchup_name = wa_raft_log_catchup:default_name(Table, Partition),
        queue_name = wa_raft_queue:default_name(Table, Partition),
        queue_counters = wa_raft_queue:default_counters(),
        queue_reads = wa_raft_queue:default_read_queue_name(Table, Partition),
        server_name = ServerName,
        storage_name = StorageName,
        storage_module = maps:get(storage_module, Spec, wa_raft_env:get_env(Application, raft_storage_module, ?RAFT_DEFAULT_STORAGE_MODULE)),
        supervisor_name = default_name(Table, Partition),
        transport_cleanup_name = wa_raft_transport_cleanup:default_name(Table, Partition),
        transport_directory = wa_raft_transport:default_directory(Database),
        transport_module = maps:get(transport_module, Spec, wa_raft_env:get_env(Application, {raft_transport_module, transport_module}, ?RAFT_DEFAULT_TRANSPORT_MODULE))
    }.

%%-------------------------------------------------------------------
%% Test API
%%-------------------------------------------------------------------

-spec prepare_spec(Application :: atom(), Spec :: wa_raft:args()) -> #raft_options{}.
prepare_spec(Application, Spec) ->
    Options = #raft_options{table = Table, partition = Partition} = normalize_spec(Application, Spec),
    ok = wa_pt:put(?OPTIONS_KEY(Table, Partition), Options),
    Options.

%%-------------------------------------------------------------------
%% Supervisor callbacks
%%-------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    ChildSpecs = [
        wa_raft_queue:child_spec(Options),
        wa_raft_storage:child_spec(Options),
        wa_raft_log:child_spec(Options),
        wa_raft_log_catchup:child_spec(Options),
        wa_raft_server:child_spec(Options),
        wa_raft_acceptor:child_spec(Options),
        wa_raft_transport_cleanup:child_spec(Options)
    ],
    {ok, {#{strategy => one_for_all, intensity => 10, period => 1}, ChildSpecs}}.
