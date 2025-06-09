%% @format
%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

%%%
%%% This module offers APIs to access the storage.
%%%
-module(kvstore_client).
-compile(warn_missing_spec_all).

-export([
    read/1,
    write/2,
    delete/1
]).

-include_lib("wa_raft/include/wa_raft.hrl").

-define(CALL_TIMEOUT, 5000).
-define(TABLE, kvstore).
-define(NUM_PARTITIONS, 4).

%% Read value for a given key. It's a blocking call.
-spec read(term()) -> {ok, term()} | wa_raft_acceptor:read_error().
read(Key) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, partition(Key)),
    wa_raft_acceptor:read(Acceptor, {read, ?TABLE, Key}, ?CALL_TIMEOUT).

%% Write a key/value pair to storage. It's a blocking call.
-spec write(term(), term()) -> ok | wa_raft_acceptor:commit_error().
write(Key, Value) ->
    commit(Key, {write, ?TABLE, Key, Value}).

%% Delete a key/value pair. It's a blocking call.
-spec delete(term()) -> ok | wa_raft_acceptor:commit_error().
delete(Key) ->
    commit(Key, {delete, ?TABLE, Key}).

-spec commit(term(), term()) -> term() | wa_raft_acceptor:commit_error().
commit(Key, Command) ->
    Acceptor = ?RAFT_ACCEPTOR_NAME(?TABLE, partition(Key)),
    wa_raft_acceptor:commit(Acceptor, {make_ref(), Command}, ?CALL_TIMEOUT).

-spec partition(term()) -> number().
partition(Key) ->
    erlang:phash2(Key, ?NUM_PARTITIONS) + 1.
