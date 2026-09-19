%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%
%% This source code is licensed under the Apache 2.0 license found in
%% the LICENSE file in the root directory of this source tree.

-module(wa_raft_log_SUITE).
-oncall("whatsapp_msgd").
-compile(warn_missing_spec_all).

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test helpers
-export([
    setup_log/1,
    teardown_log/1
]).

%% Test cases
-export([
    open/1,
    open_nonzero/1,
    open_continue/1,
    open_reset/1,
    open_mismatch/1,
    append/1,
    check/1,
    check_out_of_range/1,
    check_conflict/1,
    get/1,
    get_terms/1,
    reset/1,
    truncate/1,
    trim/1,
    rotate_durable/1,
    rotate/1,
    config/1,
    config_trim/1,
    config_truncate/1,
    config_reopen/1,
    config_reset/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 300}}] ++ wa_raft_test_helper:suite().

-spec init_per_suite(Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_suite(Config) ->
    wa_raft_test_helper:setup_environment(Config).

-spec end_per_suite(Config :: ct_suite:ct_config()) -> ok.
end_per_suite(Config) ->
    wa_raft_test_helper:teardown_environment(Config).

-spec init_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_group(Group, Config) ->
    [{group, Group} | Config].

-spec end_per_group(Group :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_group(_, _) ->
    ok.

-spec init_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ct_suite:ct_config().
init_per_testcase(Testcase, Config) ->
    maybe_mock_durable_position(Testcase),
    Group = proplists:get_value(group, Config),
    Table = list_to_atom(atom_to_list(Group) ++ "." ++ atom_to_list(Testcase)),
    [{table, Table}, {partition, 1}, {testcase, Testcase} | Config].

-spec maybe_mock_durable_position(Testcase :: atom()) -> ok.
maybe_mock_durable_position(Testcase) when
    Testcase =:= trim;
    Testcase =:= rotate_durable;
    Testcase =:= rotate;
    Testcase =:= config_trim;
    Testcase =:= config_cleanup
->
    meck:new(wa_raft_storage, [passthrough]),
    meck:expect(wa_raft_storage, durable_position, fun(_, _) -> unbounded end);
maybe_mock_durable_position(_Testcase) ->
    ok.

-spec end_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_, _) ->
    ok.

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, api}].

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {api, [
            open,
            open_nonzero,
            open_continue,
            open_reset,
            open_mismatch,
            append,
            check,
            check_out_of_range,
            check_conflict,
            get,
            get_terms,
            reset,
            truncate,
            trim,
            rotate_durable,
            rotate,
            config,
            config_trim,
            config_truncate,
            config_reopen,
            config_reset
        ]}
    ].

-spec setup_log(Config :: ct_suite:ct_config()) -> {ok, #raft_log{}, pid()}.
% elp:ignore unreachable_test - Not a test case, but a helper function used by other test suites
setup_log(Config) ->
    Table = proplists:get_value(table, Config),
    Partition = proplists:get_value(partition, Config),
    Options = wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, #{
        table => Table,
        partition => Partition
    }),
    {ok, Pid} = wa_raft_log:start_link(Options),
    Log = #raft_log{
        name = wa_raft_log:registered_name(Table, Partition),
        application = wa_raft,
        table = Table,
        partition = Partition,
        provider = Options#raft_options.log_module
    },
    {ok, Log, Pid}.

-spec teardown_log(Pid :: pid()) -> ok.
% elp:ignore unreachable_test - Not a test case, but a helper function used by other test suites
teardown_log(Pid) ->
    case is_process_alive(Pid) of
        true  -> gen_server:stop(Pid);
        false -> ok
    end.

%%--------------------------------------------------------------------
%% TEST CASES

%% Open the log in the log instance
-spec open(Config :: ct_suite:ct_config()) -> ok.
open(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View} = wa_raft_log:open(Name, #raft_log_pos{}),

    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(0, wa_raft_log:last_index(Log)),

    ?assertEqual(0, wa_raft_log:first_index(View)),
    ?assertEqual(0, wa_raft_log:last_index(View)),

    ok = teardown_log(Pid).

%% Open the log at a non-zero starting position
-spec open_nonzero(Config :: ct_suite:ct_config()) -> ok.
open_nonzero(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{term = 2, index = 10}),

    ?assertEqual(10, wa_raft_log:first_index(View0)),
    ?assertEqual(10, wa_raft_log:last_index(View0)),
    ?assertEqual(10, wa_raft_log:first_index(Log)),
    ?assertEqual(10, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(View0, 10)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(Log, 10)),

    % append some entries starting at non-zero position
    Entries0 = [{2, {I, noop}} || I <- lists:seq(11, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),
    ?assertEqual(10, wa_raft_log:first_index(View1)),
    ?assertEqual(20, wa_raft_log:last_index(View1)),
    ?assertEqual(10, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {2, {14, noop}}}, wa_raft_log:get(View1, 14)),
    ?assertEqual({ok, {2, {15, noop}}}, wa_raft_log:get(View1, 15)),
    ?assertEqual(not_found, wa_raft_log:get(View1, 25)),
    ?assertEqual({ok, [{2, {I, noop}} || I <- lists:seq(16, 18)]}, wa_raft_log:entries(View1, 16, 3)),
    ?assertEqual({ok, [{2, {I, noop}} || I <- lists:seq(16, 20)]}, wa_raft_log:entries(View1, 16, 10)),

    % append some more entries
    Entries1 = [{2, {I, noop}} || I <- lists:seq(21, 25)],
    {ok, View2} = wa_raft_log:append(View1, Entries1),
    ?assertEqual(10, wa_raft_log:first_index(View2)),
    ?assertEqual(25, wa_raft_log:last_index(View2)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 8)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 9)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(View2, 10)),
    ?assertEqual({ok, {2, {11, noop}}}, wa_raft_log:get(View2, 11)),
    ?assertEqual({ok, [{2, {I, noop}} || I <- lists:seq(16, 18)]}, wa_raft_log:entries(View2, 16, 3)),
    ?assertEqual({ok, [{2, {I, noop}} || I <- lists:seq(16, 25)]}, wa_raft_log:entries(View2, 16, 12)),
    ?assertEqual({ok, [{2, {I, noop}} || I <- lists:seq(19, 25)]}, wa_raft_log:entries(View2, 19, 18)),

    ok = teardown_log(Pid).

%% An open is triggered at a log position that already exists in the log
-spec open_continue(Config :: ct_suite:ct_config()) -> ok.
open_continue(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    Entries = [{1, {I, noop}} || I <- lists:seq(1, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries),
    ?assertEqual({ok, Entries}, wa_raft_log:entries(View1, 1, 30)),

    {ok, View2} = wa_raft_log:open(Name, #raft_log_pos{term = 1, index = 5}),
    ?assertEqual(0, wa_raft_log:first_index(View2)),
    ?assertEqual(20, wa_raft_log:last_index(View2)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, Entries}, wa_raft_log:entries(View2, 1, 30)),

    {ok, View3} = wa_raft_log:open(Name, #raft_log_pos{term = 1, index = 10}),
    ?assertEqual(0, wa_raft_log:first_index(View3)),
    ?assertEqual(20, wa_raft_log:last_index(View3)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, Entries}, wa_raft_log:entries(View3, 1, 30)),

    {ok, View4} = wa_raft_log:open(Name, #raft_log_pos{term = 1, index = 2}),
    ?assertEqual(0, wa_raft_log:first_index(View4)),
    ?assertEqual(20, wa_raft_log:last_index(View4)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, Entries}, wa_raft_log:entries(View4, 1, 30)),

    ok = teardown_log(Pid).

%% An open triggered on a non-existent log entry resets the log.
-spec open_reset(Config :: ct_suite:ct_config()) -> ok.
open_reset(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),
    ?assertEqual({ok, Entries0}, wa_raft_log:entries(View1, 1, 30)),

    {ok, View2} = wa_raft_log:open(Name, #raft_log_pos{term = 1, index = 25}),
    ?assertEqual(25, wa_raft_log:first_index(View2)),
    ?assertEqual(25, wa_raft_log:last_index(View2)),
    ?assertEqual(25, wa_raft_log:first_index(Log)),
    ?assertEqual(25, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 5)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 15)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 5)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 15)),
    ?assertEqual({ok, {1, undefined}}, wa_raft_log:get(View2, 25)),
    ?assertEqual({ok, {1, undefined}}, wa_raft_log:get(Log, 25)),

    Entries2 = [{1, {I, noop}} || I <- lists:seq(26, 35)],
    {ok, View3} = wa_raft_log:append(View2, Entries2),
    ?assertEqual({ok, Entries2}, wa_raft_log:entries(View3, 26, 20)),

    ok = teardown_log(Pid).

%% An open triggered on a log entry with a mismatching term resets the log.
-spec open_mismatch(Config :: ct_suite:ct_config()) -> ok.
open_mismatch(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),
    ?assertEqual({ok, Entries0}, wa_raft_log:entries(View1, 1, 30)),

    {ok, View2} = wa_raft_log:open(Name, #raft_log_pos{term = 2, index = 15}),
    ?assertEqual(15, wa_raft_log:first_index(View2)),
    ?assertEqual(15, wa_raft_log:last_index(View2)),
    ?assertEqual(15, wa_raft_log:first_index(Log)),
    ?assertEqual(15, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 5)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 10)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 5)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 10)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(View2, 15)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(Log, 15)),

    Entries2 = [{2, {I, noop}} || I <- lists:seq(16, 25)],
    {ok, View3} = wa_raft_log:append(View2, Entries2),
    ?assertEqual({ok, Entries2}, wa_raft_log:entries(View3, 16, 20)),

    ok = teardown_log(Pid).

%% Append entries to an empty log
-spec append(Config :: ct_suite:ct_config()) -> ok.
append(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    Entry1 = {1, {1, noop}},
    Entry2 = {1, {2, noop}},
    Entry3 = {1, {3, noop}},

    % append one entry
    {ok, View1} = wa_raft_log:append(View0, [Entry1]),
    ?assertEqual(0, wa_raft_log:first_index(View1)),
    ?assertEqual(1, wa_raft_log:last_index(View1)),
    ?assertEqual({ok, Entry1}, wa_raft_log:get(View1, 1)),
    ?assertEqual({ok, 1}, wa_raft_log:term(View1, 1)),

    % append a list of entries
    {ok, View2} = wa_raft_log:append(View1, [Entry2, Entry3]),
    ?assertEqual(0, wa_raft_log:first_index(View2)),
    ?assertEqual(3, wa_raft_log:last_index(View2)),
    ?assertEqual({ok, Entry2}, wa_raft_log:get(View2, 2)),
    ?assertEqual({ok, Entry3}, wa_raft_log:get(View2, 3)),
    ?assertEqual({ok, 1}, wa_raft_log:term(View2, 2)),
    ?assertEqual({ok, 1}, wa_raft_log:term(View2, 3)),
    ?assertEqual({ok, [Entry1, Entry2]}, wa_raft_log:entries(View2, 1, 2)),
    ?assertEqual({ok, [Entry1, Entry2, Entry3]}, wa_raft_log:entries(View2, 1, 10)),
    ?assertEqual({ok, [Entry2, Entry3]}, wa_raft_log:entries(View2, 2, 10)),
    ?assertEqual({ok, []}, wa_raft_log:entries(View2, 4, 10)),

    ok = teardown_log(Pid).

%% Check heartbeat entries
-spec check(Config :: ct_suite:ct_config()) -> ok.
check(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % append some entries
    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 10)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),

    % check heartbeat with complete overlap
    Entries1A = [{1, {I, noop}} || I <- lists:seq(6, 8)],
    ?assertEqual({ok, []}, wa_raft_log:check_heartbeat(View1, 6, Entries1A)),

    % check heartbeat with partial overlap
    Entries1B = [{1, {I, noop}} || I <- lists:seq(8, 20)],
    ?assertEqual({ok, [{1, {I, noop}} || I <- lists:seq(11, 20)]}, wa_raft_log:check_heartbeat(View1, 8, Entries1B)),

    % check normal heartbeat
    Entries1C = [{1, {I, noop}} || I <- lists:seq(10, 20)],
    ?assertEqual({ok, [{1, {I, noop}} || I <- lists:seq(11, 20)]}, wa_raft_log:check_heartbeat(View1, 10, Entries1C)),

    % append some more entries
    Entries1 = [{1, {I, noop}} || I <- lists:seq(11, 20)],
    {ok, View2} = wa_raft_log:append(View1, Entries1),

    % check heartbeat with complete overlap
    Entries2A = [{1, {I, noop}} || I <- lists:seq(16, 18)],
    ?assertEqual({ok, []}, wa_raft_log:check_heartbeat(View2, 16, Entries2A)),

    % check heartbeat with partial overlap
    Entries2B = [{1, {I, noop}} || I <- lists:seq(18, 30)],
    ?assertEqual({ok, [{1, {I, noop}} || I <- lists:seq(21, 30)]}, wa_raft_log:check_heartbeat(View2, 18, Entries2B)),

    % check normal heartbeat
    Entries2C = [{1, {I, noop}} || I <- lists:seq(20, 30)],
    ?assertEqual({ok, [{1, {I, noop}} || I <- lists:seq(21, 30)]}, wa_raft_log:check_heartbeat(View2, 20, Entries2C)),

    ok = teardown_log(Pid).

%% Check heartbeat entries that are completely out of range
-spec check_out_of_range(Config :: ct_suite:ct_config()) -> ok.
check_out_of_range(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{term = 1, index = 10}),

    % append some entries
    Entries0 = [{1, {I, noop}} || I <- lists:seq(11, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),

    % check heartbeat that is completely before the start of the log
    Entries1A = [{1, {I, noop}} || I <- lists:seq(4, 8)],
    ?assertEqual({invalid, out_of_range}, wa_raft_log:check_heartbeat(View1, 4, Entries1A)),

    % check heartbeat that is partially before the start of the log
    Entries1B = [{1, {I, noop}} || I <- lists:seq(8, 12)],
    ?assertEqual({invalid, out_of_range}, wa_raft_log:check_heartbeat(View1, 8, Entries1B)),

    % check heartbeat that is partially past the end of the log
    Entries1C = [{1, {I, noop}} || I <- lists:seq(18, 22)],
    ?assertEqual({ok, [{1, {I, noop}} || I <- lists:seq(21, 22)]}, wa_raft_log:check_heartbeat(View1, 18, Entries1C)),

    % check heartbeat that is at the end of the log
    Entries1D = [{1, {I, noop}} || I <- lists:seq(21, 24)],
    ?assertEqual({invalid, out_of_range}, wa_raft_log:check_heartbeat(View1, 21, Entries1D)),

    % check heartbeat that is completely past the end of the log
    Entries1E = [{1, {I, noop}} || I <- lists:seq(22, 26)],
    ?assertEqual({invalid, out_of_range}, wa_raft_log:check_heartbeat(View1, 22, Entries1E)),

    ok = teardown_log(Pid).

%% Check heartbeat entries with conflicting terms
-spec check_conflict(Config :: ct_suite:ct_config()) -> ok.
check_conflict(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % append some entries
    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 10)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),

    % check heartbeat that is all term mismatches
    Entries1A = [{2, {I, noop}} || I <- lists:seq(6, 12)],
    ?assertEqual({conflict, 6, Entries1A}, wa_raft_log:check_heartbeat(View1, 6, Entries1A)),

    % check heartbeat that has some term mismatches
    Entries1B1 = [{1, {I, noop}} || I <- lists:seq(6, 8)],
    Entries1B2 = [{2, {I, noop}} || I <- lists:seq(9, 14)],
    ?assertEqual({conflict, 9, Entries1B2}, wa_raft_log:check_heartbeat(View1, 6, Entries1B1 ++ Entries1B2)),

    ok = teardown_log(Pid).

-spec get_terms(Config :: ct_suite:ct_config()) -> ok.
get_terms(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % submit some large entries
    Entries = [{I, {I, <<I:1024>>}} || I <- lists:seq(1, 100)],
    ExpectedTerms = [I || I <- lists:seq(1, 100)],
    {ok, View1} = wa_raft_log:append(View0, Entries),

    % ensure that get always returns at least 1 entry when limit > 0
    ?assertEqual({ok, lists:sublist(ExpectedTerms, 1, 1)}, wa_raft_log:get_terms(View1, 1, 1)),
    ?assertEqual({ok, lists:sublist(ExpectedTerms, 1, 1)}, wa_raft_log:get_terms(Log, 1, 1)),

    % ensure that the correct number of entries is returned
    [
        begin
            ?assertEqual({ok, lists:sublist(ExpectedTerms, 1, N)}, wa_raft_log:get_terms(View1, 1, N)),
            ?assertEqual({ok, lists:sublist(ExpectedTerms, 1, N)}, wa_raft_log:get_terms(Log, 1, N))
        end || N <- lists:seq(1, 70, 5)
    ],

    ok = teardown_log(Pid).

%% Get with limits works correctly
-spec get(Config :: ct_suite:ct_config()) -> ok.
get(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % submit some large entries
    Entries = [{1, {I, <<I:1024>>}} || I <- lists:seq(1, 100)],
    {ok, View1} = wa_raft_log:append(View0, Entries),

    % size of a single entry
    EntryBytes = erlang:external_size(hd(Entries)),

    % ensure that get always returns at least 1 entry when limit > 0
    ?assertEqual({ok, lists:sublist(Entries, 1, 1)}, wa_raft_log:entries(View1, 1, 1, infinity)),
    ?assertEqual({ok, lists:sublist(Entries, 1, 1)}, wa_raft_log:entries(Log, 1, 1, infinity)),
    ?assertEqual({ok, lists:sublist(Entries, 1, 1)}, wa_raft_log:entries(View1, 1, 1, 1)),
    ?assertEqual({ok, lists:sublist(Entries, 1, 1)}, wa_raft_log:entries(Log, 1, 1, 1)),
    ?assertEqual({ok, lists:sublist(Entries, 1, 1)}, wa_raft_log:entries(View1, 1, 10, EntryBytes)),
    ?assertEqual({ok, lists:sublist(Entries, 1, 1)}, wa_raft_log:entries(Log, 1, 10, EntryBytes)),

    % ensure that the correct number of entries is returned
    [
        begin
            ?assertEqual(
                {ok, lists:sublist(Entries, 1, min(N, 30))},
                wa_raft_log:entries(View1, 1, 30, EntryBytes * N)
            ),
            ?assertEqual({ok, lists:sublist(Entries, 1, min(N, 30))}, wa_raft_log:entries(Log, 1, 30, EntryBytes * N))
        end || N <- lists:seq(1, 70, 5)
    ],

    ok = teardown_log(Pid).

%% Reset clears the log and sets a marker at the specified position
-spec reset(Config :: ct_suite:ct_config()) -> ok.
reset(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % Reset on empty data
    {ok, View1} = wa_raft_log:reset(View0, #raft_log_pos{term = 2, index = 15}),
    ?assertEqual(15, wa_raft_log:first_index(View1)),
    ?assertEqual(15, wa_raft_log:first_index(Log)),
    ?assertEqual(15, wa_raft_log:last_index(View1)),
    ?assertEqual(15, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(View1, 15)),
    ?assertEqual({ok, {2, undefined}}, wa_raft_log:get(Log, 15)),

    % Add some data
    Entries1 = [{2, {I, noop}} || I <- lists:seq(16, 35)],
    {ok, View2} = wa_raft_log:append(View1, Entries1),

    % Reset with data
    {ok, View3} = wa_raft_log:reset(View2, #raft_log_pos{term = 3, index = 45}),
    ?assertEqual(45, wa_raft_log:first_index(View3)),
    ?assertEqual(45, wa_raft_log:first_index(Log)),
    ?assertEqual(45, wa_raft_log:last_index(View3)),
    ?assertEqual(45, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View3, 44)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 44)),
    ?assertEqual({ok, {3, undefined}}, wa_raft_log:get(View3, 45)),
    ?assertEqual({ok, {3, undefined}}, wa_raft_log:get(Log, 45)),
    ?assertEqual(not_found, wa_raft_log:get(View3, 46)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 46)),
    ?assertEqual({ok, [{3, undefined}]}, wa_raft_log:entries(View3, 45, 5)),
    ?assertEqual({ok, [{3, undefined}]}, wa_raft_log:entries(Log, 45, 5)),

    % Reset to zero with non-zero term should be rejected
    ?assertEqual({error, invalid_position}, wa_raft_log:reset(View3, #raft_log_pos{term = 1, index = 0})),
    ?assertEqual(45, wa_raft_log:first_index(View3)),
    ?assertEqual(45, wa_raft_log:first_index(Log)),
    ?assertEqual(45, wa_raft_log:last_index(View3)),
    ?assertEqual(45, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View3, 44)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 44)),
    ?assertEqual({ok, {3, undefined}}, wa_raft_log:get(View3, 45)),
    ?assertEqual({ok, {3, undefined}}, wa_raft_log:get(Log, 45)),
    ?assertEqual(not_found, wa_raft_log:get(View3, 46)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 46)),
    ?assertEqual({ok, [{3, undefined}]}, wa_raft_log:entries(View3, 45, 5)),
    ?assertEqual({ok, [{3, undefined}]}, wa_raft_log:entries(Log, 45, 5)),

    % Reset to zero with zero term
    {ok, View4} = wa_raft_log:reset(View3, #raft_log_pos{term = 0, index = 0}),
    ?assertEqual(0, wa_raft_log:first_index(View4)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(0, wa_raft_log:last_index(View4)),
    ?assertEqual(0, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {0, undefined}}, wa_raft_log:get(View4, 0)),
    ?assertEqual({ok, {0, undefined}}, wa_raft_log:get(Log, 0)),
    ?assertEqual(not_found, wa_raft_log:get(View4, 1)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 1)),

    ok = teardown_log(Pid).

%% Truncate clears the tail of the log.
-spec truncate(Config :: ct_suite:ct_config()) -> ok.
truncate(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % Add some data
    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),

    % Truncate a bit
    {ok, View2} = wa_raft_log:truncate(View1, 15),
    ?assertEqual(0, wa_raft_log:first_index(View2)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(14, wa_raft_log:last_index(View2)),
    ?assertEqual(14, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {1, {14, noop}}}, wa_raft_log:get(View2, 14)),
    ?assertEqual({ok, {1, {14, noop}}}, wa_raft_log:get(Log, 14)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 15)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 15)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 16)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 16)),
    ?assertEqual({ok, lists:sublist(Entries0, 11, 4)}, wa_raft_log:entries(View2, 11, 20)),
    ?assertEqual({ok, lists:sublist(Entries0, 11, 4)}, wa_raft_log:entries(Log, 11, 20)),

    % Truncate past the end of the log (noop)
    {ok, View3} = wa_raft_log:truncate(View2, 18),
    ?assertEqual(0, wa_raft_log:first_index(View3)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(14, wa_raft_log:last_index(View3)),
    ?assertEqual(14, wa_raft_log:last_index(Log)),

    % Truncate again
    {ok, View4} = wa_raft_log:truncate(View3, 10),
    ?assertEqual(0, wa_raft_log:first_index(View4)),
    ?assertEqual(0, wa_raft_log:first_index(Log)),
    ?assertEqual(9, wa_raft_log:last_index(View4)),
    ?assertEqual(9, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {1, {9, noop}}}, wa_raft_log:get(View4, 9)),
    ?assertEqual({ok, {1, {9, noop}}}, wa_raft_log:get(Log, 9)),
    ?assertEqual(not_found, wa_raft_log:get(View4, 10)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 10)),
    ?assertEqual(not_found, wa_raft_log:get(View4, 11)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 11)),
    ?assertEqual({ok, lists:sublist(Entries0, 6, 4)}, wa_raft_log:entries(View4, 6, 20)),
    ?assertEqual({ok, lists:sublist(Entries0, 6, 4)}, wa_raft_log:entries(Log, 6, 20)),

    % Reset log at some non-zero location
    {ok, View5} = wa_raft_log:reset(View4, #raft_log_pos{term = 3, index = 40}),

    % Add some data
    Entries5 = [{3, {I, noop}} || I <- lists:seq(41, 60)],
    {ok, View6} = wa_raft_log:append(View5, Entries5),

    % Truncate a bit again
    {ok, View7} = wa_raft_log:truncate(View6, 55),
    ?assertEqual(40, wa_raft_log:first_index(View7)),
    ?assertEqual(40, wa_raft_log:first_index(Log)),
    ?assertEqual(54, wa_raft_log:last_index(View7)),
    ?assertEqual(54, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {3, {54, noop}}}, wa_raft_log:get(View7, 54)),
    ?assertEqual({ok, {3, {54, noop}}}, wa_raft_log:get(Log, 54)),
    ?assertEqual(not_found, wa_raft_log:get(View7, 55)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 55)),
    ?assertEqual(not_found, wa_raft_log:get(View7, 56)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 56)),
    ?assertEqual({ok, lists:sublist(Entries5, 11, 4)}, wa_raft_log:entries(View7, 51, 20)),
    ?assertEqual({ok, lists:sublist(Entries5, 11, 4)}, wa_raft_log:entries(Log, 51, 20)),

    % Truncation that would delete the log is disallowed
    ?assertEqual({error, invalid_position}, wa_raft_log:truncate(View7, 40)),
    ?assertEqual(40, wa_raft_log:first_index(View7)),
    ?assertEqual(40, wa_raft_log:first_index(Log)),
    ?assertEqual(54, wa_raft_log:last_index(View7)),
    ?assertEqual(54, wa_raft_log:last_index(Log)),
    ?assertEqual({ok, {3, {54, noop}}}, wa_raft_log:get(View7, 54)),
    ?assertEqual({ok, {3, {54, noop}}}, wa_raft_log:get(Log, 54)),
    ?assertEqual(not_found, wa_raft_log:get(View7, 55)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 55)),
    ?assertEqual(not_found, wa_raft_log:get(View7, 56)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 56)),
    ?assertEqual({ok, lists:sublist(Entries5, 11, 4)}, wa_raft_log:entries(View7, 51, 20)),
    ?assertEqual({ok, lists:sublist(Entries5, 11, 4)}, wa_raft_log:entries(Log, 51, 20)),

    ok = teardown_log(Pid).

-spec trim(Config :: ct_suite:ct_config()) -> ok.
trim(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % Add some data
    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),

    % Trim a bit
    {ok, View2} = wa_raft_log:trim(View1, 5),
    sys:get_state(Name),
    ?assertEqual(5, wa_raft_log:first_index(View2)),
    ?assertEqual(5, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(View2)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View2, 4)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 4)),
    ?assertEqual({ok, {1, {5, noop}}}, wa_raft_log:get(View2, 5)),
    ?assertEqual({ok, {1, {5, noop}}}, wa_raft_log:get(Log, 5)),
    ?assertEqual({ok, {1, {6, noop}}}, wa_raft_log:get(View2, 6)),
    ?assertEqual({ok, {1, {6, noop}}}, wa_raft_log:get(Log, 6)),

    % Trim again (noop from wa_raft_log perspective)
    {ok, View3} = wa_raft_log:trim(View2, 3),
    sys:get_state(Name),
    ?assertEqual(5, wa_raft_log:first_index(View3)),
    ?assertEqual(5, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(View3)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),

    % Trim a bit more
    {ok, View4} = wa_raft_log:trim(View3, 15),
    sys:get_state(Name),
    ?assertEqual(15, wa_raft_log:first_index(View4)),
    ?assertEqual(15, wa_raft_log:first_index(Log)),
    ?assertEqual(20, wa_raft_log:last_index(View4)),
    ?assertEqual(20, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View4, 14)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 14)),
    ?assertEqual({ok, {1, {15, noop}}}, wa_raft_log:get(View4, 15)),
    ?assertEqual({ok, {1, {15, noop}}}, wa_raft_log:get(Log, 15)),
    ?assertEqual({ok, {1, {16, noop}}}, wa_raft_log:get(View4, 16)),
    ?assertEqual({ok, {1, {16, noop}}}, wa_raft_log:get(Log, 16)),

    % Reset log at some non-zero location
    {ok, View5} = wa_raft_log:reset(View4, #raft_log_pos{term = 3, index = 40}),

    % Add some data
    Entries5 = [{3, {I, noop}} || I <- lists:seq(41, 60)],
    {ok, View6} = wa_raft_log:append(View5, Entries5),

    % Trim a bit again
    {ok, View7} = wa_raft_log:trim(View6, 55),
    sys:get_state(Name),
    ?assertEqual(55, wa_raft_log:first_index(View7)),
    ?assertEqual(55, wa_raft_log:first_index(Log)),
    ?assertEqual(60, wa_raft_log:last_index(View7)),
    ?assertEqual(60, wa_raft_log:last_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(View7, 54)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 54)),
    ?assertEqual({ok, {3, {55, noop}}}, wa_raft_log:get(View7, 55)),
    ?assertEqual({ok, {3, {55, noop}}}, wa_raft_log:get(Log, 55)),
    ?assertEqual({ok, {3, {56, noop}}}, wa_raft_log:get(View7, 56)),
    ?assertEqual({ok, {3, {56, noop}}}, wa_raft_log:get(Log, 56)),

    ok = teardown_log(Pid).

-spec rotate_durable(Config :: ct_suite:ct_config()) -> ok.
rotate_durable(Config) ->
    {ok, Log, LogPid} = setup_log(Config),
    meck:expect(wa_raft_storage, durable_position, fun(_, _) -> #raft_log_pos{term = 1, index = 50} end),

    Entries = [{1, {N, noop}} || N <- lists:seq(1, 400)],
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),
    {ok, View1} = wa_raft_log:append(View0, Entries),

    {ok, View2} = wa_raft_log:rotate(View1, 200, 50, 50),
    sys:get_state(Name),
    ?assertEqual(150, wa_raft_log:first_index(View2)),
    ?assertEqual(50, wa_raft_log:first_index(Log)),

    meck:expect(wa_raft_storage, durable_position, fun(_, _) -> #raft_log_pos{term = 1, index = 400} end),
    {ok, _View3} = wa_raft_log:rotate(View2, 350, 50, 50),
    sys:get_state(Name),
    ?assertEqual(300, wa_raft_log:first_index(Log)),
    ?assertEqual(not_found, wa_raft_log:get(Log, 299)),
    ?assertEqual({ok, {1, {300, noop}}}, wa_raft_log:get(Log, 300)),

    ok = teardown_log(LogPid).

-spec rotate(Config :: ct_suite:ct_config()) -> ok.
rotate(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),

    % Add some data
    Entries0 = [{1, {I, noop}} || I <- lists:seq(1, 20)],
    {ok, View1} = wa_raft_log:append(View0, Entries0),

    % Rotate noop (interval 5 is not yet reached at index 7 with keep 4)
    {ok, View2} = wa_raft_log:rotate(View1, 7, 5, 4),
    ?assertEqual(0, wa_raft_log:first_index(View2)),
    ?assertEqual(20, wa_raft_log:last_index(View2)),

    % Rotate at 6 (interval 5 is reached at index 10 with keep 4)
    {ok, View3} = wa_raft_log:rotate(View2, 10, 5, 4),
    ?assertEqual(6, wa_raft_log:first_index(View3)),
    ?assertEqual(20, wa_raft_log:last_index(View3)),

    % Rotate noop (interval 5 is not yet reached at index 11 with keep 4)
    {ok, View4} = wa_raft_log:rotate(View3, 11, 5, 4),
    ?assertEqual(6, wa_raft_log:first_index(View4)),
    ?assertEqual(20, wa_raft_log:last_index(View4)),

    % Rotate at 14 (interval 5 is reached at index 16 with keep 2)
    {ok, View5} = wa_raft_log:rotate(View4, 16, 5, 2),
    ?assertEqual(14, wa_raft_log:first_index(View5)),
    ?assertEqual(20, wa_raft_log:last_index(View5)),
    ?assertEqual(not_found, wa_raft_log:get(View5, 13)),
    ?assertEqual({ok, {1, {14, noop}}}, wa_raft_log:get(View5, 14)),
    ?assertEqual({ok, {1, {15, noop}}}, wa_raft_log:get(View5, 15)),

    ok = teardown_log(Pid).

-spec config(Config :: ct_suite:ct_config()) -> ok.
config(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),
    % A new log with no config should report no config found.
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),
    ?assertEqual(not_found, wa_raft_log:config(View0)),
    ?assertEqual(not_found, wa_raft_log:config(Log)),

    % Appending without a new config should not cache a new config.
    {ok, View1} = wa_raft_log:append(View0, [{1, {1, noop}}, {1, {2, noop}}]),
    ?assertEqual(not_found, wa_raft_log:config(View1)),
    ?assertEqual(not_found, wa_raft_log:config(Log)),

    % Appending with a new config should cache a new config.
    Config2 = wa_raft_server:make_config([#raft_identity{node = node2, name = name}]),
    {ok, View2} = wa_raft_log:append(View1, [{1, {3, {config, Config2}}}, {1, {4, noop}}]),
    ?assertEqual({ok, 3, Config2}, wa_raft_log:config(View2)),
    ?assertEqual({ok, 3, Config2}, wa_raft_log:config(Log)),

    % Appending another config should cache new latest config.
    Config3 = wa_raft_server:make_config([#raft_identity{node = node3, name = name}]),
    {ok, View3} = wa_raft_log:append(View2, [{1, {5, {config, Config3}}}, {1, {6, noop}}]),
    ?assertEqual({ok, 5, Config3}, wa_raft_log:config(View3)),
    ?assertEqual({ok, 5, Config3}, wa_raft_log:config(Log)),

    ok = teardown_log(Pid).

-spec config_trim(Config :: ct_suite:ct_config()) -> ok.
config_trim(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),

    % Set up initial log with some config entries
    Config1 = wa_raft_server:make_config([#raft_identity{node = node1, name = name}]),
    Config2 = wa_raft_server:make_config([#raft_identity{node = node2, name = name}]),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),
    {ok, View1} = wa_raft_log:append(View0, [{1, {1, {config, Config1}}}, {1, {2, {config, Config2}}}, {1, {3, noop}}]),

    % Trim should not affect config info unless it reaches latest config
    {ok, View2} = wa_raft_log:trim(View1, 1),
    sys:get_state(Name), % trim cast barrier
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(View2)),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(Log)),

    % Trim should not affect config info unless it reaches latest config
    {ok, View3} = wa_raft_log:trim(View2, 2),
    sys:get_state(Name), % trim cast barrier
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(View3)),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(Log)),

    % Trimming index with latest config should clear latest config cache
    {ok, View4} = wa_raft_log:trim(View3, 3),
    sys:get_state(Name), % trim cast barrier
    ?assertEqual(not_found, wa_raft_log:config(View4)),
    % Here, the result of calling config directly with the log name is not
    % defined here because log trim API does not require underlying log
    % providers to immediately delete trimmed log entries.
    % ?assertEqual(not_found, wa_raft_log:config(Name)),

    % Ensure that continuing to add configs works
    Config3 = wa_raft_server:make_config([#raft_identity{node = node3, name = name}]),
    {ok, View5} = wa_raft_log:append(View4, [{1, {4, {config, Config3}}}, {1, {5, noop}}]),
    ?assertEqual({ok, 4, Config3}, wa_raft_log:config(View5)),
    ?assertEqual({ok, 4, Config3}, wa_raft_log:config(Log)),

    ok = teardown_log(Pid).

-spec config_truncate(Config :: ct_suite:ct_config()) -> ok.
config_truncate(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),

    % Set up initial log with some config entries
    Config1 = wa_raft_server:make_config([#raft_identity{node = node1, name = name}]),
    Config2 = wa_raft_server:make_config([#raft_identity{node = node2, name = name}]),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),
    {ok, View1} = wa_raft_log:append(View0, [{1, {1, {config, Config1}}}, {1, {2, {config, Config2}}}, {1, {3, noop}}]),

    % Truncate that does not delete latest config should not affect latest config cache
    {ok, View2} = wa_raft_log:truncate(View1, 3),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(View2)),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(Log)),

    % Truncate should update config cache to next latest config when latest is truncated
    {ok, View3} = wa_raft_log:truncate(View2, 2),
    ?assertEqual({ok, 1, Config1}, wa_raft_log:config(View3)),
    ?assertEqual({ok, 1, Config1}, wa_raft_log:config(Log)),

    % Truncate may end up removing all config entries in log
    {ok, View4} = wa_raft_log:truncate(View3, 1),
    ?assertEqual(not_found, wa_raft_log:config(View4)),
    ?assertEqual(not_found, wa_raft_log:config(Log)),

    % Ensure that continuing to add configs works
    Config3 = wa_raft_server:make_config([#raft_identity{node = node3, name = name}]),
    {ok, View5} = wa_raft_log:append(View4, [{1, {1, {config, Config3}}}, {1, {2, noop}}]),
    ?assertEqual({ok, 1, Config3}, wa_raft_log:config(View5)),
    ?assertEqual({ok, 1, Config3}, wa_raft_log:config(Log)),

    ok = teardown_log(Pid).

-spec config_reopen(Config :: ct_suite:ct_config()) -> ok.
config_reopen(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),

    % Set up initial log with some config entries
    Config1 = wa_raft_server:make_config([#raft_identity{node = node1, name = name}]),
    Config2 = wa_raft_server:make_config([#raft_identity{node = node2, name = name}]),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),
    {ok, View1} = wa_raft_log:append(View0, [{1, {1, {config, Config1}}}, {1, {2, {config, Config2}}}, {1, {3, noop}}]),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(View1)),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(Log)),

    % Reopening the log should reload latest config cache
    {ok, View2} = wa_raft_log:open(Name, #raft_log_pos{index = 2, term = 1}),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(View2)),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(Log)),

    % Ensure that continuing to add configs works
    Config3 = wa_raft_server:make_config([#raft_identity{node = node3, name = name}]),
    {ok, View3} = wa_raft_log:append(View2, [{1, {4, {config, Config3}}}, {1, {5, noop}}]),
    ?assertEqual({ok, 4, Config3}, wa_raft_log:config(View3)),
    ?assertEqual({ok, 4, Config3}, wa_raft_log:config(Log)),

    ok = teardown_log(Pid).

-spec config_reset(Config :: ct_suite:ct_config()) -> ok.
config_reset(Config) ->
    {ok, Log, Pid} = setup_log(Config),
    Name = wa_raft_log:log_name(Log),

    % Set up initial log with some config entries
    Config1 = wa_raft_server:make_config([#raft_identity{node = node1, name = name}]),
    Config2 = wa_raft_server:make_config([#raft_identity{node = node2, name = name}]),
    {ok, View0} = wa_raft_log:open(Name, #raft_log_pos{}),
    {ok, View1} = wa_raft_log:append(View0, [{1, {1, {config, Config1}}}, {1, {2, {config, Config2}}}, {1, {3, noop}}]),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(View1)),
    ?assertEqual({ok, 2, Config2}, wa_raft_log:config(Log)),

    % Resetting the log will delete all entries, so latest config cache should be cleared
    {ok, View2} = wa_raft_log:reset(View1, #raft_log_pos{index = 1, term = 1}),
    ?assertEqual(not_found, wa_raft_log:config(View2)),
    ?assertEqual(not_found, wa_raft_log:config(Log)),

    % Ensure that continuing to add configs works
    Config3 = wa_raft_server:make_config([#raft_identity{node = node3, name = name}]),
    {ok, View3} = wa_raft_log:append(View2, [{1, {2, {config, Config3}}}, {1, {3, noop}}]),
    ?assertEqual({ok, 2, Config3}, wa_raft_log:config(View3)),
    ?assertEqual({ok, 2, Config3}, wa_raft_log:config(Log)),

    ok = teardown_log(Pid).
