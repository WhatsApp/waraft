%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_cleanup).
-compile(warn_missing_spec).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

%% OTP supervision
-export([
    child_spec/1,
    start_link/1
]).

%% Internal API
-export([
    default_name/2,
    registered_name/2
]).

%% Server Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(RAFT_TRANSPORT_CLEANUP_SCAN_INTERVAL_SECS, 30).

-record(state, {
    application :: atom(),
    name :: atom(),
    directory :: file:filename()
}).

%%-------------------------------------------------------------------
%% OTP Supervision
%%-------------------------------------------------------------------

-spec child_spec(Options :: #raft_options{}) -> supervisor:child_spec().
child_spec(Options) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Options]},
        restart => permanent,
        shutdown => 5000,
        modules => [?MODULE]
    }.

-spec start_link(Options :: #raft_options{}) ->  gen_server:start_ret().
start_link(#raft_options{transport_cleanup_name = Name} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%%-------------------------------------------------------------------
%% Internal API
%%-------------------------------------------------------------------

%% Get the default name for the RAFT acceptor server associated with the
%% provided RAFT partition.
-spec default_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
default_name(Table, Partition) ->
    list_to_atom("raft_transport_cleanup_" ++ atom_to_list(Table) ++ "_" ++ integer_to_list(Partition)).

%% Get the registered name for the RAFT acceptor server associated with the
%% provided RAFT partition or the default name if no registration exists.
-spec registered_name(Table :: wa_raft:table(), Partition :: wa_raft:partition()) -> Name :: atom().
registered_name(Table, Partition) ->
    case wa_raft_part_sup:options(Table, Partition) of
        undefined -> default_name(Table, Partition);
        Options   -> Options#raft_options.transport_cleanup_name
    end.

%%-------------------------------------------------------------------
%% Server Callbacks
%%-------------------------------------------------------------------

-spec init(Options :: #raft_options{}) -> {ok, State :: #state{}}.
init(#raft_options{application = Application, transport_directory = Directory, transport_cleanup_name = Name}) ->
    process_flag(trap_exit, true),
    schedule_scan(),
    {ok, #state{application = Application, name = Name, directory = Directory}}.

-spec handle_call(Request :: term(), From :: gen_server:from(), State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_call(Request, From, #state{name = Name} = State) ->
    ?LOG_WARNING("~p received unrecognized call ~0P from ~0p",
        [Name, Request, 25, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_cast(Request, #state{name = Name} = State) ->
    ?LOG_NOTICE("~p got unrecognized cast ~0P",
        [Name, Request, 25], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_info(Info :: term(), State :: #state{}) -> {noreply, NewState :: #state{}}.
handle_info(scan, #state{} = State) ->
    maybe_cleanup(State),
    schedule_scan(),
    {noreply, State};
handle_info(Info, #state{name = Name} = State) ->
    ?LOG_NOTICE("~p got unrecognized info ~p",
        [Name, Info], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec maybe_cleanup(State :: #state{}) -> ok | {error, term()}.
maybe_cleanup(#state{application = App, name = Name, directory = Directory} = State) ->
    case prim_file:list_dir(Directory) of
        {ok, Files} ->
            RetainMillis = ?RAFT_TRANSPORT_RETAIN_INTERVAL(App) * 1000,
            NowMillis = erlang:system_time(millisecond),
            lists:foreach(
                fun (Filename) ->
                    Path = filename:join(Directory, Filename),
                    ID = list_to_integer(Filename),
                    case wa_raft_transport:transport_info(ID) of
                        {ok, #{end_ts := EndTs}} when NowMillis - EndTs > RetainMillis ->
                            ?LOG_NOTICE("~p deleting ~p due to expiring after transport ended",
                                [Name, Filename], #{domain => [whatsapp, wa_raft]}),
                            cleanup(ID, Path, State);
                        {ok, _Info} ->
                            ok;
                        not_found ->
                            ?LOG_NOTICE("~p deleting ~p due to having no associated transport",
                                [Name, Filename], #{domain => [whatsapp, wa_raft]}),
                            cleanup(ID, Path, State)
                    end
                end, Files);
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING("~p failed to list transports for cleanup due to ~p",
                [Name, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    end.

-spec cleanup(non_neg_integer(), string(), #state{}) -> ok | {error, term()}.
cleanup(ID, Path, #state{name = Name}) ->
    case file:del_dir_r(Path) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING("~p failed to cleanup transport ~p due to ~p",
                [Name, ID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    end.

-spec schedule_scan() -> reference().
schedule_scan() ->
    erlang:send_after(?RAFT_TRANSPORT_CLEANUP_SCAN_INTERVAL_SECS * 1000, self(), scan).
