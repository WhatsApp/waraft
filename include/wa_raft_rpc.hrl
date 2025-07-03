%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file contains macros defining the form of all RPCs and API
%%% calls used as part of the RAFT protocol and RAFT server and storage API.

%%-------------------------------------------------------------------
%% RAFT Server RPC Formats
%%-------------------------------------------------------------------
%% As the RAFT process that is intended to performs the cross-node
%% communication required to provide durability against failure,
%% RAFT servers across nodes must agree on the RPC formats in use.
%% This means that RPC formats should not be changed once created.
%%-------------------------------------------------------------------

-define(RAFT_NAMED_RPC(Type, Term, SenderName, SenderNode, Payload), {rpc, Type, Term, SenderName, SenderNode, Payload}).

%% These two RPCs are used by RAFT catchup to receive the status of
%% the RAFT server being sent to and should not change.
-define(LEGACY_RAFT_RPC(Type, Term, SenderId, Payload), {rpc, Type, Term, SenderId, Payload}).
-define(LEGACY_APPEND_ENTRIES_RESPONSE_RPC(Term, SenderId, PrevLogIndex, Success, LastIndex),
    ?LEGACY_RAFT_RPC(append_entries_response, Term, SenderId, {PrevLogIndex, Success, LastIndex})).

%%-------------------------------------------------------------------
%% RAFT Server Procedures
%%-------------------------------------------------------------------
%% An RPC received from a peer is intended to trigger one of the
%% procedures listed below.
%%-------------------------------------------------------------------

-define(APPEND_ENTRIES,          append_entries).
-define(APPEND_ENTRIES_RESPONSE, append_entries_response).
-define(REQUEST_VOTE,            request_vote).
-define(VOTE,                    vote).
-define(HANDOVER,                handover).
-define(HANDOVER_FAILED,         handover_failed).
-define(NOTIFY_TERM,             notify_term).

%% Definitions of each of the standard procedures.
-define(PROCEDURE(Type, Payload), {procedure, Type, Payload}).
-define(APPEND_ENTRIES(PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex),   ?PROCEDURE(?APPEND_ENTRIES, {PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex})).
-define(APPEND_ENTRIES_RESPONSE(PrevLogIndex, Success, MatchIndex, LastAppliedIndex), ?PROCEDURE(?APPEND_ENTRIES_RESPONSE, {PrevLogIndex, Success, MatchIndex, LastAppliedIndex})).
-define(REQUEST_VOTE(ElectionType, LastLogIndex, LastLogTerm),                        ?PROCEDURE(?REQUEST_VOTE, {ElectionType, LastLogIndex, LastLogTerm})).
-define(VOTE(Vote),                                                                   ?PROCEDURE(?VOTE, {Vote})).
-define(HANDOVER(Ref, PrevLogIndex, PrevLogTerm, Entries),                            ?PROCEDURE(?HANDOVER, {Ref, PrevLogIndex, PrevLogTerm, Entries})).
-define(HANDOVER_FAILED(Ref),                                                         ?PROCEDURE(?HANDOVER_FAILED, {Ref})).
-define(NOTIFY_TERM(),                                                                ?PROCEDURE(?NOTIFY_TERM, {})).

%% A request to execute a particular procedure. This request could
%% have been issued locally or as a result of a remote procedure
%% call. The peer (if exists and could be oneself) that issued the
%% procedure call will be provided as the sender.
-define(REMOTE(Sender, Call), {remote, Sender, Call}).

%%-------------------------------------------------------------------
%% RAFT Server Internal Events
%%-------------------------------------------------------------------
%% An event produced internally within the RAFT server.
%%-------------------------------------------------------------------

-define(ADVANCE_TERM(Term), {advance_term, Term}).
-define(FORCE_ELECTION(Term), {force_election, Term}).

%%-------------------------------------------------------------------
%% RAFT Server API
%%-------------------------------------------------------------------
%% The RAFT server also accepts commands issued from other processes
%% on the local node. These commands are not guaranteed to have the
%% same format between versions and so should only be used locally.
%% Prefer to use `wa_raft_server` module exports when possible.
%%-------------------------------------------------------------------

-define(RAFT_COMMAND(Type, Payload), {command, Type, Payload}).

-define(COMMIT_COMMAND(From, Op),                               ?RAFT_COMMAND(commit, {From, Op})).
-define(READ_COMMAND(Op),                                       ?RAFT_COMMAND(read, Op)).

-define(STATUS_COMMAND,                                         ?RAFT_COMMAND(status, undefined)).
-define(TRIGGER_ELECTION_COMMAND(TermOrOffset),                 ?RAFT_COMMAND(trigger_election, {TermOrOffset})).
-define(PROMOTE_COMMAND(TermOrOffset, Force),                   ?RAFT_COMMAND(promote, {TermOrOffset, Force})).
-define(RESIGN_COMMAND,                                         ?RAFT_COMMAND(resign, undefined)).

-define(ADJUST_MEMBERSHIP_COMMAND(Action, Peer, ConfigIndex),   ?RAFT_COMMAND(adjust_membership, {Action, Peer, ConfigIndex})).

-define(SNAPSHOT_AVAILABLE_COMMAND(Root, Position),             ?RAFT_COMMAND(snapshot_available, {Root, Position})).

-define(HANDOVER_CANDIDATES_COMMAND,                            ?RAFT_COMMAND(handover_candidates, undefined)).
-define(HANDOVER_COMMAND(Peer),                                 ?RAFT_COMMAND(handover, Peer)).

-define(ENABLE_COMMAND,                                         ?RAFT_COMMAND(enable, undefined)).
-define(DISABLE_COMMAND(Reason),                                ?RAFT_COMMAND(disable, Reason)).

-define(BOOTSTRAP_COMMAND(Position, Config, Data),              ?RAFT_COMMAND(bootstrap, {Position, Config, Data})).

-define(NOTIFY_COMPLETE_COMMAND(),                              ?RAFT_COMMAND(notify_complete, undefined)).
