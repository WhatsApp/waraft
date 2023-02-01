%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Supervisor for supervising RAFT partitions started by a client application.
%%% As a `simple_one_for_one` supervisor, this supervisor can dynamically
%%% start and stop partitions and will stop partitions in parallel during
%%% shutdown.

-module(wa_raft_sup).
-compile(warn_missing_spec).
-behaviour(supervisor).

%% OTP supervision
-export([
    child_spec/1,
    child_spec/2,
    child_spec/3,
    start_link/3
]).

%% API
-export([
    start_partition/2,
    start_partition_under_application/2,
    stop_partition/2,
    stop_partition/3,
    stop_partition_under_application/2,
    stop_partition_under_application/3
]).

%% Internal API
-export([
    default_name/1,
    default_config_apps/1,
    registered_config_apps/1
]).

%% Internal API
-export([
    options/1
]).

%% Test API
-export([
    prepare_application/1,
    prepare_application/2
]).

%% Supervisor callbacks
-export([
    init/1
]).

-include("wa_raft.hrl").

%% Key in persistent_term for the application options associated with an
%% application that has started a RAFT supervisor.
-define(OPTIONS_KEY(Application), {?MODULE, Application}).

%% Options for RAFT client applications
-type options() :: #{
    % RAFT will search for environment variables from applications in this order
    config_search_apps => [atom()]
}.

%%-------------------------------------------------------------------
%% OTP supervision
%%-------------------------------------------------------------------

-spec child_spec(Specs :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(Specs) ->
    {ok, Application} = application:get_application(),
    child_spec(Application, Specs, #{}).

-spec child_spec(Application :: atom(), Specs :: [wa_raft:args()]) -> supervisor:child_spec().
child_spec(Application, RaftArgs) when is_list(RaftArgs) ->
    child_spec(Application, RaftArgs, #{});
child_spec(RaftArgs, Options) ->
    {ok, Application} = application:get_application(),
    child_spec(Application, RaftArgs, Options).

-spec child_spec(Application :: atom(), Specs :: [wa_raft:args()], Options :: options()) -> supervisor:child_spec().
child_spec(Application, RaftArgs, Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Application, RaftArgs, Options]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_link(Application :: atom(), Specs :: [wa_raft:args()], Options :: options()) -> supervisor:startlink_ret().
start_link(Application, RaftArgs, Options) ->
    ok = persistent_term:put(?OPTIONS_KEY(Application), normalize_spec(Application, Options)),
    case supervisor:start_link({local, default_name(Application)}, ?MODULE, Application) of
        {ok, Pid} = Result ->
            lists:foreach(fun (Spec) -> start_partition(Pid, Spec) end, RaftArgs),
            Result;
        Else ->
            Else
    end.

%%-------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------

-spec start_partition(Supervisor :: atom() | pid(), Spec :: wa_raft:args()) -> supervisor:startchild_ret().
start_partition(Supervisor, Spec) ->
    supervisor:start_child(Supervisor, [Spec]).

-spec start_partition_under_application(Application :: atom(), Spec :: wa_raft:args()) -> supervisor:startchild_ret().
start_partition_under_application(Application, Spec) ->
    start_partition(default_name(Application), Spec).

-spec stop_partition(Supervisor :: atom() | pid(), Pid :: pid()) -> ok | {error, atom()}.
stop_partition(Supervisor, Pid) ->
    supervisor:terminate_child(Supervisor, Pid).

-spec stop_partition(Supervisor :: atom() | pid(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok | {error, atom()}.
stop_partition(Supervisor, Table, Partition) ->
    case whereis(wa_raft_part_sup:registered_name(Table, Partition)) of
        Pid when is_pid(Pid) -> stop_partition(Supervisor, Pid);
        _                    -> {error, not_found}
    end.

-spec stop_partition_under_application(Application :: atom(), Pid :: pid()) -> ok | {error, atom()}.
stop_partition_under_application(Application, Pid) ->
    stop_partition(default_name(Application), Pid).

-spec stop_partition_under_application(Application :: atom(), Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> ok | {error, atom()}.
stop_partition_under_application(Application, Table, Partition) ->
    stop_partition(default_name(Application), Table, Partition).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

-spec default_name(Application :: atom()) -> atom().
default_name(Application) ->
    list_to_atom("raft_sup_" ++ atom_to_list(Application)).

-spec default_config_apps(Application :: atom()) -> [atom()].
default_config_apps(Application) ->
    [Application, ?APP].

-spec registered_config_apps(Application :: atom()) -> [atom()].
registered_config_apps(Application) ->
    case options(Application) of
        undefined -> error({raft_not_started, Application});
        Options   -> Options#raft_application.config_search_apps
    end.

-spec options(Application :: atom()) -> #raft_application{} | undefined.
options(Application) ->
    persistent_term:get(?OPTIONS_KEY(Application), undefined).

-spec normalize_spec(Application :: atom(), Options :: options()) -> #raft_application{}.
normalize_spec(Application, Options) ->
    #raft_application{
        name = Application,
        config_search_apps = maps:get(config_search_apps, Options, [Application])
    }.

%%-------------------------------------------------------------------
%% Test API
%%-------------------------------------------------------------------

-spec prepare_application(Application :: atom()) -> ok.
prepare_application(Application) ->
    prepare_application(Application, #{}).

-spec prepare_application(Application :: atom(), Options :: options()) -> ok.
prepare_application(Application, Options) ->
    RaftApplication = normalize_spec(Application, Options),
    ok = persistent_term:put(?OPTIONS_KEY(Application), RaftApplication).

%%-------------------------------------------------------------------
%% Supervisor callbacks
%%-------------------------------------------------------------------

-spec init(Application :: atom()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Application) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 1}, [wa_raft_part_sup:child_spec(Application)]}}.
