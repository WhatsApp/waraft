%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements functions for storing / loading persistent state.

-module(wa_raft_durable_state).
-compile(warn_missing_spec).

-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

-export([
    load/1,
    store/1,
    sync/1
]).

-spec load(StateIn :: #raft_state{}) -> {ok, StateOut :: #raft_state{}} | no_state | wa_raft:error().
load(#raft_state{name = Name, data_dir = RootDir} = State) ->
    StateItems = [
        {current_term,   fun is_integer/1, fun (V, S) -> S#raft_state{current_term = V} end,   required},
        {voted_for,      fun is_atom/1,    fun (V, S) -> S#raft_state{voted_for = V} end,      required},
        {disable_reason, undefined,        fun (V, S) -> S#raft_state{disable_reason = V} end, undefined}
    ],
    StateFile = filename:join(RootDir, ?STATE_FILE_NAME),
    case file:consult(StateFile) of
        {ok, [{crc, CRC} | StateTerms]} ->
            case erlang:crc32(term_to_binary(StateTerms)) of
                CRC ->
                    try
                        {ok, lists:foldl(
                            fun ({Item, Validator, Updater, Default}, StateN) ->
                                    case proplists:lookup(Item, StateTerms) of
                                        none when Default =:= required ->
                                            ?LOG_ERROR("~p read state file but cannot find ~p.",
                                                [Name, Item], #{domain => [whatsapp, wa_raft]}),
                                            throw({error, {missing, Item}});
                                        none ->
                                            Updater(Default, StateN);
                                        {Item, Value} ->
                                            case Validator =:= undefined orelse Validator(Value) of
                                                true ->
                                                    Updater(Value, StateN);
                                                false ->
                                                    ?LOG_ERROR("~p read state file but ~p has an invalid value `~p`.",
                                                        [Name, Item, Value], #{domain => [whatsapp, wa_raft]}),
                                                    throw({error, {invalid, Item}})
                                            end
                                    end
                            end, State, StateItems)}
                    catch
                        throw:{error, Reason} -> {error, Reason}
                    end;
                InvalidCRC ->
                    ?LOG_ERROR("~p read state file but CRCs did not match. (saved crc: ~p, computed crc: ~p)",
                        [Name, InvalidCRC, CRC], #{domain => [whatsapp, wa_raft]}),
                    {error, invalid_crc}
            end;
        {ok, _} ->
            ?LOG_ERROR("~p read state file but no CRC was found", [Name], #{domain => [whatsapp, wa_raft]}),
            {error, no_crc};
        {error, enoent} ->
            ?LOG_NOTICE("~p is not loading non-existant state file.", [Name], #{domain => [whatsapp, wa_raft]}),
            no_state;
        {error, Reason} ->
            ?LOG_ERROR("~p could not read state file due to ~p.", [Name, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, Reason}
    end.

-spec store(#raft_state{}) -> ok | wa_raft:error().
store(#raft_state{name = Name, data_dir = RootDir, current_term = CurrentTerm, voted_for = VotedFor, disable_reason = DisableReason}) ->
    StateList = [
        {current_term, CurrentTerm},
        {voted_for, VotedFor},
        {disable_reason, DisableReason}
    ],
    StateListWithCRC = [{crc, erlang:crc32(term_to_binary(StateList))} | StateList],
    StateIO = [io_lib:format("~p.~n", [Term]) || Term <- StateListWithCRC],
    StateFile = filename:join(RootDir, ?STATE_FILE_NAME),
    StateFileTemp = [StateFile, ".temp"],
    case filelib:ensure_dir(StateFile) of
        ok ->
            case prim_file:write_file(StateFileTemp, StateIO) of
                ok ->
                    case file:rename(StateFileTemp, StateFile) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            ?RAFT_COUNT({'raft.server.persist_state.error.rename', Reason}),
                            ?LOG_ERROR("~p failed to rename temporary state file due to ~p.", [Name, Reason], #{domain => [whatsapp, wa_raft]}),
                            {error, {rename, Reason}}
                    end;
                {error, Reason} ->
                    ?RAFT_COUNT({'raft.server.persist_state.error.write', Reason}),
                    ?LOG_ERROR("~p failed to write current state to temporary file due to ~p.", [Name, Reason], #{domain => [whatsapp, wa_raft]}),
                    {error, {write, Reason}}
            end;
        {error, Reason} ->
            ?RAFT_COUNT({'raft.server.persist_state.error.ensure_dir', Reason}),
            ?LOG_ERROR("~p failed to ensure directory exists due to ~p.", [Name, Reason], #{domain => [whatsapp, wa_raft]}),
            {error, {ensure_dir, Reason}}
    end.

-spec sync(StateIn :: #raft_state{}) -> ok.
sync(#raft_state{data_dir = RootDir}) ->
    StateFile = filename:join(RootDir, ?STATE_FILE_NAME),
    case prim_file:open(StateFile, [read, binary]) of
        {ok, Fd} ->
            prim_file:sync(Fd),
            prim_file:close(Fd),
            ok;
        _ ->
            ok
    end.
