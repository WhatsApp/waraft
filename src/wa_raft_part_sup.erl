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
    raft_sup/2
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

-spec raft_sup(wa_raft:table(), wa_raft:partition()) -> atom().
raft_sup(Table, Partition) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

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
        acceptor_name = ?RAFT_ACCEPTOR_NAME(Table, Partition),
        log_name = ?RAFT_LOG_NAME(Table, Partition),
        log_module = maps:get(log_module, Spec, application:get_env(?APP, raft_log_module, wa_raft_log_ets)),
        log_catchup_name = ?RAFT_LOG_CATCHUP(Table, Partition),
        queue_name = wa_raft_queue:name(Table, Partition),
        queue_commits = ?RAFT_COMMIT_QUEUE_TABLE(Table, Partition),
        queue_reads = ?RAFT_READ_QUEUE_TABLE(Table, Partition),
        server_name = ?RAFT_SERVER_NAME(Table, Partition),
        storage_name = ?RAFT_STORAGE_NAME(Table, Partition),
        storage_module = maps:get(storage_module, Spec, application:get_env(?APP, raft_storage_module, wa_raft_storage_ets)),
        supervisor_name = raft_sup(Table, Partition)
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
