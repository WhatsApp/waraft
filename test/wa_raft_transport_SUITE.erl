% @format
%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_transport_SUITE).
-oncall("whatsapp_kvdb").
-compile(warn_missing_spec_all).

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0
]).

%% Unit tests
-export([
    safe_relative_paths_accepted/1,
    traversal_paths_rejected/1,
    absolute_paths_rejected/1
]).

-include_lib("assert/include/assert.hrl").

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 30}}].

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, unit}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {unit, [
            safe_relative_paths_accepted,
            traversal_paths_rejected,
            absolute_paths_rejected
        ]}
    ].

%%--------------------------------------------------------------------
%% UNIT TESTS

-define(ROOT, "/var/data/transport/1").

-spec safe_relative_paths_accepted(Config :: ct_suite:ct_config()) -> ok.
safe_relative_paths_accepted(_Config) ->
    Files = [
        {1, "000000.dat", 10},
        {2, "subdir/000000.dat", 20},
        {3, "a/b/c/file", 30}
    ],
    ?assertEqual(
        {ok, [
            {1, "000000.dat", filename:join(?ROOT, "000000.dat"), 10},
            {2, "subdir/000000.dat", filename:join(?ROOT, "subdir/000000.dat"), 20},
            {3, "a/b/c/file", filename:join(?ROOT, "a/b/c/file"), 30}
        ]},
        wa_raft_transport:resolve_transport_files(?ROOT, Files)
    ),
    ok.

-spec traversal_paths_rejected(Config :: ct_suite:ct_config()) -> ok.
traversal_paths_rejected(_Config) ->
    [
        ?assertEqual(
            {error, invalid_file_path},
            wa_raft_transport:resolve_transport_files(?ROOT, [{1, Path, 10}])
        )
     || Path <- ["..", "../x", "../../../../tmp/x", "a/../../b", "subdir/../../etc/passwd"]
    ],
    %% A safe file mixed with an unsafe file still fails the whole transport.
    ?assertEqual(
        {error, invalid_file_path},
        wa_raft_transport:resolve_transport_files(?ROOT, [
            {1, "ok.dat", 10},
            {2, "../escape", 20}
        ])
    ),
    ok.

-spec absolute_paths_rejected(Config :: ct_suite:ct_config()) -> ok.
absolute_paths_rejected(_Config) ->
    [
        ?assertEqual(
            {error, invalid_file_path},
            wa_raft_transport:resolve_transport_files(?ROOT, [{1, Path, 10}])
        )
     || Path <- ["/tmp/x", "/etc/passwd", "/"]
    ],
    ok.
