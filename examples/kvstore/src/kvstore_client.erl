%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.

%%%
%%% This module offers APIs to access the storage.
%%%
-module(kvstore_client).
-compile(warn_missing_spec).

-export([
    read/1,
    write/2,
    delete/1
]).

-include_lib("kernel/include/logger.hrl").

-define(CALL_TIMEOUT, 5000).
-define(TABLE, kvstore).
-define(NUM_PARTITIONS, 4).
-define(PARTITION(P), list_to_atom(lists:concat(["raft_acceptor_", ?TABLE, "_" , P]))).

%% Read value for a given key. It's a blocking call.
-spec read(term()) ->  {ok, {term(), map(), number()}} | {error, term()}.
read(Key) ->
    execute(Key, {read, ?TABLE, Key}).

%% Write a key/value pair to storage. It's a blocking call.
-spec write(term(), term()) ->  {ok, number()} | {error, term()}.
write(Key, Value) ->
    execute(Key, {write, ?TABLE, Key, Value}).

%% Delete a key/value pair. It's a blocking call.
-spec delete(term()) ->  ok | {error, term()}.
delete(Key) ->
    execute(Key, {delete, ?TABLE, Key}).

-spec execute(term(), term()) -> term().
execute(Key, Command) ->
    Partition = ?PARTITION(partition(Key)), 
    gen_server:call(Partition, {commit, {make_ref(), Command}}, ?CALL_TIMEOUT).

-spec partition(term()) -> number().
partition(Key) ->
    erlang:phash2(Key, ?NUM_PARTITIONS) + 1.

