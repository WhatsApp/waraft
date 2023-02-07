%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements transport interface by using erlang OTP dist.

-module(wa_raft_dist_transport).
-compile(warn_missing_spec).
-behaviour(gen_server).
-behaviour(wa_raft_transport).

-export([
    child_spec/0,
    start_link/0
]).

-export([
    transport_init/1,
    transport_send/3
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

-record(sender_state, {
}).
-record(receiver_state, {
    fds = #{} :: #{{ID :: wa_raft_transport:transport_id(), FileID :: wa_raft_transport:file_id()} => Fd :: file:fd()}
}).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => transient,
        shutdown => 5000,
        modules => [?MODULE]
    }.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec transport_init(Node :: node()) -> {ok, State :: #sender_state{}}.
transport_init(_Node) ->
    {ok, #sender_state{}}.

-spec transport_send(ID :: wa_raft_transport:transport_id(), FileID :: wa_raft_transport:file_id(), State :: #sender_state{}) ->
    {ok, NewState :: #sender_state{}} |
    {stop, Reason :: term(), NewState :: #sender_state{}}.
transport_send(ID, FileID, State) ->
    ?LOG_DEBUG("wa_raft_dist_transport starting to send file ~p/~p",
        [ID, FileID], #{domain => [whatsapp, wa_raft]}),
    case wa_raft_transport:transport_info(ID) of
        {ok, #{peer := Peer}} ->
            case wa_raft_transport:file_info(ID, FileID) of
                {ok, #{name := File, path := Path}} ->
                    case prim_file:open(Path, [binary, read]) of
                        {ok, Fd} ->
                            try
                                catch prim_file:advise(Fd, 0, 0, sequential),
                                case transport_send_loop(ID, FileID, Fd, Peer, State) of
                                    {ok, NewState} ->
                                        {ok, NewState};
                                    {error, Reason, NewState} ->
                                        {stop, Reason, NewState}
                                end
                            after
                                prim_file:close(Fd)
                            end;
                        {error, Reason} ->
                            ?LOG_ERROR("wa_raft_dist_transport failed to open file ~p/~p (~s) due to ~p",
                                [ID, FileID, File, Reason], #{domain => [whatsapp, wa_raft]}),
                            {stop, {failed_to_open_file, ID, FileID, Reason}, State}
                    end;
                _ ->
                    {stop, {invalid_file, ID, FileID}, State}
            end;
        _ ->
            {stop, {invalid_transport, ID}, State}
    end.

transport_send_loop(ID, FileID, Fd, Peer, State) ->
    ChunkSize = ?RAFT_DIST_TRANSPORT_CHUNK_SIZE(),
    MaxInflight = ?RAFT_DIST_TRANSPORT_MAX_INFLIGHT(),
    transport_send_loop(ID, FileID, Fd, 0, Peer, [], ChunkSize, MaxInflight, State).

transport_send_loop(ID, FileID, _Fd, eof, Peer, [], _ChunkSize, _MaxInflight, State) ->
    gen_server:cast({?MODULE, Peer}, {complete, ID, FileID}),
    {ok, State};
transport_send_loop(ID, FileID, Fd, Offset, Peer, [RequestId | Chunks], ChunkSize, MaxInflight, State)
        when Offset =:= eof orelse length(Chunks) >= MaxInflight ->
    case gen_server:wait_response(RequestId, 5000) of
        {reply, ok} ->
            transport_send_loop(ID, FileID, Fd, Offset, Peer, Chunks, ChunkSize, MaxInflight, State);
        {reply, {error, Reason}} ->
            ?LOG_ERROR("wa_raft_dist_transport failed to send file ~p/~p due to receiver error ~p",
                [ID, FileID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, {receiver_error, ID, FileID, Reason}, State};
        timeout ->
            ?LOG_ERROR("wa_raft_dist_transport timed out while sending file ~p/~p",
                [ID, FileID], #{domain => [whatsapp, wa_raft]}),
            {error, {send_timed_out, ID, FileID}, State};
        {error, {Reason, _}} ->
            ?LOG_ERROR("wa_raft_dist_transport failed to send file ~p/~p due to ~p",
                [ID, FileID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, {send_failed, ID, FileID, Reason}, State}
    end;
transport_send_loop(ID, FileID, Fd, Offset, Peer, Chunks, ChunkSize, MaxInflight, State) when is_integer(Offset) ->
    case prim_file:read(Fd, ChunkSize) of
        {ok, Data} ->
            RequestId = gen_server:send_request({?MODULE, Peer}, {chunk, ID, FileID, Offset, Data}),
            wa_raft_transport:update_file_info(ID, FileID,
                fun (#{completed_bytes := Completed} = Info) ->
                    Info#{completed_bytes => Completed + byte_size(Data)}
                end),
            transport_send_loop(ID, FileID, Fd, Offset + byte_size(Data), Peer, Chunks ++ [RequestId], ChunkSize, MaxInflight, State);
        eof ->
            transport_send_loop(ID, FileID, Fd, eof, Peer, Chunks, ChunkSize, MaxInflight, State);
        {error, Reason} ->
            ?LOG_ERROR("wa_raft_dist_transport failed to read file ~p/~p due to ~p",
                [ID, FileID, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, {read_failed, ID, FileID, Reason}, State}
    end.

-spec init(Args :: list()) -> {ok, State :: #receiver_state{}}.
init([]) ->
    process_flag(trap_exit, true),
    {ok, #receiver_state{}}.

-spec handle_call(Request, From :: term(), State :: #receiver_state{}) ->
    {reply, Reply :: term(), NewState :: #receiver_state{}} | {noreply, NewState :: #receiver_state{}}
    when Request :: {chunk, wa_raft_transport:transport_id(), wa_raft_transport:file_id(), integer(), binary()}.
handle_call({chunk, ID, FileID, Offset, Data}, _From, #receiver_state{} = State0) ->
    {Reply, NewState} = case open_file(ID, FileID, State0) of
        {ok, Fd, State1} ->
            case prim_file:pwrite(Fd, Offset, Data) of
                ok ->
                    wa_raft_transport:update_file_info(ID, FileID,
                        fun (#{completed_bytes := Completed} = Info) ->
                            Info#{completed_bytes => Completed + byte_size(Data)}
                        end),

                    {ok, State1};
                {error, Reason} ->
                    ?LOG_WARNING("wa_raft_dist_transport receiver failed to write file chunk ~p/~p @ ~p due to ~p",
                        [ID, FileID, Offset, Reason], #{domain => [whatsapp, wa_raft]}),
                    {{write_failed, Reason}, State1}
            end;
        {error, Reason, State1} ->
            ?LOG_WARNING("wa_raft_dist_transport receiver failed to handle file chunk ~p/~p @ ~p due to open failing due to ~p",
                [ID, FileID, Offset, Reason], #{domain => [whatsapp, wa_raft]}),
            {{open_failed, Reason}, State1}
    end,
    {reply, Reply, NewState};
handle_call(Request, From, #receiver_state{} = State) ->
    ?LOG_NOTICE("wa_raft_dist_transport got unrecognized call ~p from ~p",
        [Request, From], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec handle_cast(Request, State :: #receiver_state{}) -> {noreply, NewState :: #receiver_state{}}
    when Request :: {complete,  wa_raft_transport:transport_id(), wa_raft_transport:file_id()}.
handle_cast({complete, ID, FileID}, #receiver_state{} = State0) ->
    case open_file(ID, FileID, State0) of
        {ok, _Fd, State1} ->
            {ok, State2} = close_file(ID, FileID, State1),
            wa_raft_transport:complete(ID, FileID, ok),
            {noreply, State2};
        {error, Reason, State1} ->
            ?LOG_WARNING("wa_raft_dist_transport receiver failed to handle file complete ~p/~p due to open failing due to ~p",
                [ID, FileID, Reason], #{domain => [whatsapp, wa_raft]}),
            {noreply, State1}
    end;
handle_cast(Request, #receiver_state{} = State) ->
    ?LOG_NOTICE("wa_raft_dist_transport got unrecognized cast ~p",
        [Request], #{domain => [whatsapp, wa_raft]}),
    {noreply, State}.

-spec terminate(Reason :: term(), State :: #receiver_state{}) -> term().
terminate(Reason, #receiver_state{}) ->
    ?LOG_NOTICE("wa_raft_dist_transport terminating due to ~p",
        [Reason], #{domain => [whatsapp, wa_raft]}),
    ok.

-spec open_file(ID :: wa_raft_transport:transport_id(), FileID :: wa_raft_transport:file_id(), State :: #receiver_state{}) ->
    {ok, Fd :: file:fd(), NewState :: #receiver_state{}} | {error, Reason :: term(), NewState :: #receiver_state{}}.
open_file(ID, FileID, #receiver_state{fds = Fds} = State0) ->
    case Fds of
        #{{ID, FileID} := Fd} ->
            {ok, Fd, State0};
        #{} ->
            case wa_raft_transport:file_info(ID, FileID) of
                {ok, #{name := File, path := Path}} ->
                    catch filelib:ensure_dir(Path),
                    case prim_file:open(Path, [binary, write]) of
                        {ok, Fd} ->
                            State1 = State0#receiver_state{fds = Fds#{{ID, FileID} => Fd}},
                            {ok, Fd, State1};
                        {error, Reason} ->
                            ?LOG_WARNING("wa_raft_dist_transport receiver failed to open file ~p/~p (~p) due to ~p",
                                [ID, FileID, File, Reason], #{domain => [whatsapp, wa_raft]}),
                            {error, {open_failed, Reason}, State0}
                    end;
                _ ->
                    {error, invalid_file, State0}
            end
    end.

-spec close_file(ID :: wa_raft_transport:transport_id(), FileID :: wa_raft_transport:file_id(), State :: #receiver_state{}) ->
    {ok, NewState :: #receiver_state{}}.
close_file(ID, FileID, #receiver_state{fds = Fds} = State0) ->
    case Fds of
        #{{ID, FileID} := Fd} ->
            catch prim_file:close(Fd),
            State1 = State0#receiver_state{fds = maps:remove({ID, FileID}, Fds)},
            {ok, State1};
        _ ->
            {ok, State0}
    end.
