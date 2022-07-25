%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file contains macros defining the form of all RPCs and API
%%% calls used as part of the RAFT protocol and RAFT server and storage API.

-author('hsun324@fb.com').

-define(RAFT_RPC(Type, Term, SenderId, Payload), {rpc, Type, Term, SenderId, Payload}).
-define(RAFT_NAMED_RPC(Type, Term, SenderName, SenderNode, Payload), {rpc, Type, Term, SenderName, SenderNode, Payload}).

-define(APPEND_ENTRIES_RPC(Term, SenderId, PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex),
    ?RAFT_RPC(append_entries, Term, SenderId, {PrevLogIndex, PrevLogTerm, Entries, CommitIndex, TrimIndex})).
-define(APPEND_ENTRIES_RESPONSE_RPC(Term, SenderId, PrevLogIndex, Success, LastIndex),
    ?RAFT_RPC(append_entries_response, Term, SenderId, {PrevLogIndex, Success, LastIndex})).

-define(REQUEST_VOTE_RPC_OLD(Term, SenderId, LastLogIndex, LastLogTerm),
    ?RAFT_RPC(request_vote, Term, SenderId, {LastLogIndex, LastLogTerm})).
-define(REQUEST_VOTE_RPC(Term, SenderId, ElectionType, LastLogIndex, LastLogTerm),
    ?RAFT_RPC(request_vote, Term, SenderId, {ElectionType, LastLogIndex, LastLogTerm})).
-define(VOTE_RPC(Term, SenderId, Vote),
    ?RAFT_RPC(vote, Term, SenderId, {Vote})).

-define(HANDOVER_RPC(Term, SenderId, Ref, PrevLogIndex, PrevLogTerm, Entries),
    ?RAFT_RPC(handover, Term, SenderId, {Ref, PrevLogIndex, PrevLogTerm, Entries})).
-define(HANDOVER_FAILED_RPC(Term, SenderId, Ref),
    ?RAFT_RPC(handover_failed, Term, SenderId, {Ref})).

-define(NOTIFY_TERM_RPC(Term, SenderId),
    ?RAFT_RPC(notify_term, Term, SenderId, undefined)).

%% ==================================================
%%  RAFT server API definitions
%% ==================================================

-define(RAFT_COMMAND(Type, Payload), {command, Type, Payload}).

-define(COMMIT_COMMAND(Op),                         ?RAFT_COMMAND(commit, Op)).
-define(READ_COMMAND(Op),                           ?RAFT_COMMAND(read, Op)).

-define(STATUS_COMMAND,                             ?RAFT_COMMAND(status, undefined)).
-define(PROMOTE_COMMAND(Term, Force, Config),       ?RAFT_COMMAND(promote, {Term, Force, Config})).
-define(RESIGN_COMMAND,                             ?RAFT_COMMAND(resign, undefined)).

-define(ADJUST_MEMBERSHIP_COMMAND(Action, Peer),    ?RAFT_COMMAND(adjust_membership, {Action, Peer})).

-define(SNAPSHOT_AVAILABLE_COMMAND(Root, Position), ?RAFT_COMMAND(snapshot_available, {Root, Position})).

-define(HANDOVER_CANDIDATES_COMMAND,                ?RAFT_COMMAND(handover_candidates, undefined)).
-define(HANDOVER_COMMAND(Peer),                     ?RAFT_COMMAND(handover, Peer)).

-define(ENABLE_COMMAND,                             ?RAFT_COMMAND(enable, undefined)).
-define(DISABLE_COMMAND(Reason),                    ?RAFT_COMMAND(disable, Reason)).
-define(WITNESS_COMMAND(),                          ?RAFT_COMMAND(witness, undefined)).

-define(SET_PEER_OFFLINE_COMMAND(Peer, IsOffline),  ?RAFT_COMMAND(set_peer_offline, {Peer, IsOffline})).
