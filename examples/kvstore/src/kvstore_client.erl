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

-export_type([
    read_result/0,
    commit_result/0
]).

-include_lib("kernel/include/logger.hrl").

-define(CALL_TIMEOUT, 5000).
-define(TABLE, kvstore).
-define(NUM_PARTITIONS, 4).
-define(PARTITION(P), list_to_atom(lists:concat(["raft_acceptor_", ?TABLE, "_" , P]))).

-type read_result() :: {ok, term()} | not_found | wa_raft_acceptor:read_error() | wa_raft_acceptor:call_error().
-type commit_result() :: ok | wa_raft_acceptor:commit_error() | wa_raft_acceptor:call_error().

%% Read value for a given key. It's a blocking call.
-spec read(term()) -> read_result().
read(Key) ->
    Partition = ?PARTITION(partition(Key)),
    wa_raft_acceptor:read(Partition, {read, ?TABLE, Key}, ?CALL_TIMEOUT).

%% Write a key/value pair to storage. It's a blocking call.
-spec write(term(), term()) -> commit_result().
write(Key, Value) ->
    execute(Key, {write, ?TABLE, Key, Value}).

%% Delete a key/value pair. It's a blocking call.
-spec delete(term()) -> commit_result().
delete(Key) ->
    execute(Key, {delete, ?TABLE, Key}).

-spec execute(term(), term()) -> commit_result().
execute(Key, Command) ->
    Partition = ?PARTITION(partition(Key)),
    wa_raft_acceptor:commit(Partition, {make_ref(), Command}, ?CALL_TIMEOUT).

-spec partition(term()) -> number().
partition(Key) ->
    erlang:phash2(Key, ?NUM_PARTITIONS) + 1.

