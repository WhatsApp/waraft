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
-compile(warn_missing_spec).
-behaviour(supervisor).

%% OTP Supervision
-export([
    child_spec/1,
    start_link/2
]).

%% Internal API
-export([
    default_name/2,
    registered_name/2
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

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

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

-spec start_link(Application :: atom(), Spec :: wa_raft:args()) -> supervisor:startlink_ret().
start_link(Application, Spec) ->
    %% First normalize the provided specification into a full options record.
    Options = #raft_options{table = Table, partition = Partition, supervisor_name = Name} = normalize_spec(Application, Spec),

    %% Then put the declared options for the current RAFT partition into
    %% persistent term for access by shared resources and other services.
    %% The RAFT options for a table are not expected to change during the
    %% runtime of the RAFT application and so repeated updates should not
    %% result in any GC load. Warn if this is case.
    PrevOptions = persistent_term:get(?OPTIONS_KEY(Table, Partition), Options),
    PrevOptions =/= Options andalso
        ?LOG_WARNING(?MODULE_STRING " storing changed options for RAFT partitition ~0p/~0p",
            [Table, Partition], #{domain => [whatsapp, wa_raft]}),
    ok = persistent_term:put(?OPTIONS_KEY(Table, Partition), Options),

    supervisor:start_link({local, Name}, ?MODULE, Options).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT partition supervisor associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT partition supervisor associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.supervisor_name
    end.

-spec options(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> #raft_options{} | undefined.
options(Table, Partition) ->
    persistent_term:get(?OPTIONS_KEY(Table, Partition), undefined).

-spec normalize_spec(Application :: atom(), Spec :: wa_raft:args()) -> #raft_options{}.
normalize_spec(Application, #{table := Table, partition := Partition} = Spec) ->
    % TODO(hsun324) - T133215915: Application-specific default log/storage module
    #raft_options{
        application = Application,
        table = Table,
        partition = Partition,
        witness = maps:get(witness, Spec, false),
        database = ?ROOT_DIR(Table, Partition),
        acceptor_name = wa_raft_acceptor:default_name(Table, Partition),
        log_name = wa_raft_log:default_name(Table, Partition),
        log_module = maps:get(log_module, Spec, application:get_env(?APP, raft_log_module, wa_raft_log_ets)),
        log_catchup_name = wa_raft_log_catchup:default_name(Table, Partition),
        queue_name = wa_raft_queue:default_name(Table, Partition),
        queue_counters = wa_raft_queue:default_counters(),
        queue_commits = wa_raft_queue:default_commit_queue_name(Table, Partition),
        queue_reads = wa_raft_queue:default_read_queue_name(Table, Partition),
        server_name = wa_raft_server:default_name(Table, Partition),
        storage_name = wa_raft_storage:default_name(Table, Partition),
        storage_module = maps:get(storage_module, Spec, application:get_env(?APP, raft_storage_module, wa_raft_storage_ets)),
        supervisor_name = default_name(Table, Partition)
    }.

%%-------------------------------------------------------------------
%% Test API
%%-------------------------------------------------------------------

-spec prepare_spec(Application :: atom(), Spec :: wa_raft:args()) -> #raft_options{}.
prepare_spec(Application, Spec) ->
    Options = #raft_options{table = Table, partition = Partition} = normalize_spec(Application, Spec),
    ok = persistent_term:put(?OPTIONS_KEY(Table, Partition), Options),
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
        wa_raft_acceptor:child_spec(Options)
    ],
    {ok, {#{strategy => one_for_all, intensity => 10, period => 1}, ChildSpecs}}.
