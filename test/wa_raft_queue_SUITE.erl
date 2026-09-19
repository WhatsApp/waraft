-module(wa_raft_queue_SUITE).
-oncall("whatsapp_msgd").
-compile(warn_missing_spec_all).

%% TEST SERVER CALLBACKS
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

%% Unit tests
-export([
    commit/1,
    commit_multiple/1,
    commit_queue_full/1,
    commit_apply_queue_full/1,
    read/1,
    read_early/1,
    read_multiple/1,
    read_fulfill_all/1,
    read_queue_full/1,
    read_queue_full_early/1,
    read_apply_queue_full/1,
    apply/1
]).

%% Queue server tests
-export([
    server/1,
    server_reset/1,
    missing/1
]).

-include_lib("assert/include/assert.hrl").
-include_lib("wa_raft/include/wa_raft.hrl").

%%-------------------------------------------------------------------
%% TEST SERVER CALLBACKS
%%-------------------------------------------------------------------

-spec suite() -> [term()].
suite() ->
    [{timetrap, {seconds, 30}}] ++ wa_raft_test_helper:suite().

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
init_per_testcase(Testcase, Config0) ->
    % Set small limits for queue sizes for testing purposes
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_applies, 5),
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_high_priority_commits, 5),
    ok = application:set_env(?RAFT_APPLICATION, raft_max_pending_reads, 5),

    Config1 = [{testcase, Testcase} | Config0],
    case proplists:get_value(group, Config1) of
        unit -> [{server, start_server(Config1)} | Config1];
        server -> Config1
    end.

-spec end_per_testcase(Testcase :: atom(), Config :: ct_suite:ct_config()) -> ok.
end_per_testcase(_, Config) ->
    Server = proplists:get_value(server, Config, undefined),
    Server =/= undefined andalso stop_server(Server),
    ok.

-spec groups() -> [ct_suite:ct_group_def()].
groups() ->
    [
        {unit, [
            commit,
            commit_multiple,
            commit_queue_full,
            commit_apply_queue_full,
            read,
            read_early,
            read_multiple,
            read_fulfill_all,
            read_queue_full,
            read_queue_full_early,
            read_apply_queue_full,
            apply
        ]},
        {server, [
            server,
            server_reset,
            missing
        ]}
    ].

-spec all() -> [ct_suite:ct_test_def()].
all() ->
    [{group, unit}, {group, server}].

%%-------------------------------------------------------------------
%% HELPER FUNCTIONS AND UTILITIES
%%-------------------------------------------------------------------

-define(TABLE, ?FUNCTION_NAME).
-define(TABLE(Config), proplists:get_value(testcase, Config)).

-spec start_server(ct_suite:ct_config()) -> pid().
start_server(Config) ->
    Table = ?TABLE(Config),
    Partition = 1,
    Options =
        case wa_raft_part_sup:options(Table, Partition) of
            undefined ->
                Spec = #{table => Table, partition => Partition},
                wa_raft_part_sup:prepare_spec(?RAFT_APPLICATION, Spec);
            Opts ->
                Opts
        end,
    {ok, Server} = wa_raft_queue:start_link(Options),
    Server.

-spec stop_server(pid()) -> ok.
stop_server(Server) ->
    gen_server:stop(Server, normal, 1000).

-define(assertReceive(Pattern), ?assertReceive(Pattern, 100)).
-define(assertReceive(Pattern, Timeout),
    (fun () ->
        receive
            Pattern = __Message__ -> __Message__
        after
            Timeout ->
                error({assertReceive, [
                    {module, ?MODULE},
                    {line, ?LINE},
                    {reason, "expected message was not received within timeout"},
                    {pattern, ??Pattern},
                    {timeout, Timeout},
                    {messages_in_queue, process_info(self(), messages)}
                ]})
        end
    end)()).

-spec make_from() -> {reference(), {pid(), reference()}}.
make_from() ->
    CallRef = make_ref(),
    {CallRef, {self(), CallRef}}.

%%-------------------------------------------------------------------
%% UNIT TESTS
%%-------------------------------------------------------------------

-spec commit(ct_suite:ct_config()) -> term().
commit(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    {CallRef, From} = make_from(),
    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)),
    ?assertEqual(1, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(1, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertEqual(ok, wa_raft_queue:commit_completed(Queue, From, reply, high)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertReceive({CallRef, reply}).

-spec commit_multiple(ct_suite:ct_config()) -> term().
commit_multiple(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    {CallRefA, FromA} = make_from(),
    {CallRefB, FromB} = make_from(),
    {CallRefC, FromC} = make_from(),
    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)),
    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)),
    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)),
    ?assertEqual(3, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(3, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertEqual(ok, wa_raft_queue:commit_completed(Queue, FromC, reply_c, high)),
    ?assertEqual(2, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(2, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertEqual(ok, wa_raft_queue:commit_completed(Queue, FromA, reply_a, high)),
    ?assertEqual(1, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(1, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertEqual(ok, wa_raft_queue:commit_completed(Queue, FromB, reply_b, high)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertReceive({CallRefA, reply_a}),
    ?assertReceive({CallRefB, reply_b}),
    ?assertReceive({CallRefC, reply_c}).

-spec commit_queue_full(ct_suite:ct_config()) -> ok.
commit_queue_full(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertNot(wa_raft_queue:commit_queue_full(Queue, high)),
    ?assertNot(wa_raft_queue:commit_queue_full(?TABLE, 1, high)),
    ok = wa_raft_queue:commit_started(Queue, high),
    ok = wa_raft_queue:commit_started(Queue, high),
    ok = wa_raft_queue:commit_started(Queue, high),
    ok = wa_raft_queue:commit_started(Queue, high),
    ?assertEqual(4, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(4, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertNot(wa_raft_queue:commit_queue_full(Queue, high)),
    ?assertNot(wa_raft_queue:commit_queue_full(?TABLE, 1, high)),
    ok = wa_raft_queue:commit_started(Queue, high),
    ?assertEqual(5, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(5, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assert(wa_raft_queue:commit_queue_full(Queue, high)),
    ?assert(wa_raft_queue:commit_queue_full(?TABLE, 1, high)),
    ?assertEqual(commit_queue_full, wa_raft_queue:commit_started(Queue, high)),

    {_, FromA} = make_from(),
    ok = wa_raft_queue:commit_completed(Queue, FromA, reply, high),
    ?assertEqual(4, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(4, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertNot(wa_raft_queue:commit_queue_full(Queue, high)),
    ?assertNot(wa_raft_queue:commit_queue_full(?TABLE, 1, high)),
    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)).

-spec commit_apply_queue_full(ct_suite:ct_config()) -> ok.
commit_apply_queue_full(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),

    ?assertEqual(apply_queue_full, wa_raft_queue:commit_started(Queue, high)),

    ok = wa_raft_queue:fulfill_apply(Queue, 1),

    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)),
    ?assertEqual(ok, wa_raft_queue:commit_started(Queue, high)).

-spec read(ct_suite:ct_config()) -> term().
read(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    {CallRef, From} = make_from(),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:submit_read(Queue, 1, From, command_a)),
    ?assertMatch([{_, command_a}], wa_raft_queue:query_reads(Queue, 1)),
    [{ReadRef, _}] = wa_raft_queue:query_reads(Queue, 1),
    ?assertEqual(ok, wa_raft_queue:fulfill_read(Queue, ReadRef, reply)),
    ?assertReceive({CallRef, reply}).

-spec read_early(ct_suite:ct_config()) -> term().
read_early(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    {CallRef, From} = make_from(),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:fulfill_incomplete_read(Queue, From, {error, read_queue_full})),
    ?assertReceive({CallRef, {error, read_queue_full}}).

-spec read_multiple(ct_suite:ct_config()) -> term().
read_multiple(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    {CallRefA, FromA} = make_from(),
    {CallRefB, FromB} = make_from(),
    {CallRefC, FromC} = make_from(),
    {CallRefD, FromD} = make_from(),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:submit_read(Queue, 1, FromA, command_a)),
    ?assertEqual(ok, wa_raft_queue:submit_read(Queue, 2, FromB, command_b)),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:submit_read(Queue, 3, FromC, command_c)),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:submit_read(Queue, 3, FromD, command_d)),
    ?assertMatch([{_, command_a}], wa_raft_queue:query_reads(Queue, 1)),
    ?assertMatch([{_, command_a}, {_, command_b}], wa_raft_queue:query_reads(Queue, 2)),
    ?assertMatch([{_, command_a}, {_, command_b}, {_, command_c}, {_, command_d}], wa_raft_queue:query_reads(Queue, 3)),
    [{ReadRefA, _}, {ReadRefB, _}, {ReadRefC, _}, {ReadRefD, _}] = wa_raft_queue:query_reads(Queue, 3),
    ?assertEqual(ok, wa_raft_queue:fulfill_read(Queue, ReadRefA, reply_a)),
    ?assertEqual(ok, wa_raft_queue:fulfill_read(Queue, ReadRefC, reply_c)),
    ?assertEqual(ok, wa_raft_queue:fulfill_read(Queue, ReadRefB, reply_b)),
    ?assertEqual(ok, wa_raft_queue:fulfill_read(Queue, ReadRefD, reply_d)),
    ?assertReceive({CallRefA, reply_a}),
    ?assertReceive({CallRefB, reply_b}),
    ?assertReceive({CallRefC, reply_c}),
    ?assertReceive({CallRefD, reply_d}).

-spec read_fulfill_all(ct_suite:ct_config()) -> term().
read_fulfill_all(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    {CallRefA, FromA} = make_from(),
    {CallRefB, FromB} = make_from(),
    {CallRefC, FromC} = make_from(),
    {CallRefD, FromD} = make_from(),
    {CallRefE, FromE} = make_from(),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:submit_read(Queue, 1, FromA, command_a),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:submit_read(Queue, 2, FromB, command_b),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:submit_read(Queue, 3, FromC, command_c),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:submit_read(Queue, 3, FromD, command_d),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:submit_read(Queue, 4, FromE, command_e),
    ?assertEqual(ok, wa_raft_queue:fulfill_all_reads(Queue, {error, read_queue_full})),
    ?assertReceive({CallRefA, {error, read_queue_full}}),
    ?assertReceive({CallRefB, {error, read_queue_full}}),
    ?assertReceive({CallRefC, {error, read_queue_full}}),
    ?assertReceive({CallRefD, {error, read_queue_full}}),
    ?assertReceive({CallRefE, {error, read_queue_full}}).

-spec read_queue_full(ct_suite:ct_config()) -> ok.
read_queue_full(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),

    {_, FromA} = make_from(),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:submit_read(Queue, 1, FromA, command_a),

    ?assertEqual(read_queue_full, wa_raft_queue:reserve_read(Queue)),

    [{ReadRefA, _}] = wa_raft_queue:query_reads(Queue, 1),
    ok = wa_raft_queue:fulfill_read(Queue, ReadRefA, reply_a),

    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)).

-spec read_queue_full_early(ct_suite:ct_config()) -> ok.
read_queue_full_early(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),

    ?assertEqual(read_queue_full, wa_raft_queue:reserve_read(Queue)),

    {_, From} = make_from(),
    ok = wa_raft_queue:fulfill_incomplete_read(Queue, From, {error, read_queue_full}),

    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)).

-spec read_apply_queue_full(ct_suite:ct_config()) -> ok.
read_apply_queue_full(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),
    ok = wa_raft_queue:reserve_apply(Queue, 1),

    ?assertEqual(apply_queue_full, wa_raft_queue:reserve_read(Queue)),

    ok = wa_raft_queue:fulfill_apply(Queue, 1),

    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)),
    ?assertEqual(ok, wa_raft_queue:reserve_read(Queue)).

-spec apply(ct_suite:ct_config()) -> ok.
apply(_Config) ->
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ?assertEqual(0, wa_raft_queue:apply_queue_size(Queue)),
    ?assertEqual(0, wa_raft_queue:apply_queue_size(?TABLE, 1)),
    ?assertNot(wa_raft_queue:apply_queue_full(Queue)),
    ?assertNot(wa_raft_queue:apply_queue_full(?TABLE, 1)),
    ?assertEqual(ok, wa_raft_queue:reserve_apply(Queue, 10)),
    ?assertEqual(1, wa_raft_queue:apply_queue_size(Queue)),
    ?assertEqual(1, wa_raft_queue:apply_queue_size(?TABLE, 1)),
    ?assertEqual(10, wa_raft_queue:apply_queue_byte_size(Queue)),
    ?assertEqual(10, wa_raft_queue:apply_queue_byte_size(?TABLE, 1)),
    ?assertNot(wa_raft_queue:apply_queue_full(Queue)),
    ?assertNot(wa_raft_queue:apply_queue_full(?TABLE, 1)),
    ?assertEqual(ok, wa_raft_queue:reserve_apply(Queue, 10)),
    ?assertEqual(ok, wa_raft_queue:reserve_apply(Queue, 10)),
    ?assertEqual(ok, wa_raft_queue:reserve_apply(Queue, 10)),
    ?assertEqual(4, wa_raft_queue:apply_queue_size(Queue)),
    ?assertEqual(4, wa_raft_queue:apply_queue_size(?TABLE, 1)),
    ?assertEqual(40, wa_raft_queue:apply_queue_byte_size(Queue)),
    ?assertEqual(40, wa_raft_queue:apply_queue_byte_size(?TABLE, 1)),
    ?assertNot(wa_raft_queue:apply_queue_full(Queue)),
    ?assertNot(wa_raft_queue:apply_queue_full(?TABLE, 1)),
    ?assertEqual(ok, wa_raft_queue:reserve_apply(Queue, 10)),
    ?assertEqual(5, wa_raft_queue:apply_queue_size(Queue)),
    ?assertEqual(5, wa_raft_queue:apply_queue_size(?TABLE, 1)),
    ?assertEqual(50, wa_raft_queue:apply_queue_byte_size(Queue)),
    ?assertEqual(50, wa_raft_queue:apply_queue_byte_size(?TABLE, 1)),
    ?assert(wa_raft_queue:apply_queue_full(Queue)),
    ?assert(wa_raft_queue:apply_queue_full(?TABLE, 1)),
    ?assertEqual(ok, wa_raft_queue:fulfill_apply(Queue, 10)),
    ?assertEqual(4, wa_raft_queue:apply_queue_size(Queue)),
    ?assertEqual(4, wa_raft_queue:apply_queue_size(?TABLE, 1)),
    ?assertEqual(40, wa_raft_queue:apply_queue_byte_size(Queue)),
    ?assertEqual(40, wa_raft_queue:apply_queue_byte_size(?TABLE, 1)),
    ?assertNot(wa_raft_queue:apply_queue_full(Queue)),
    ?assertNot(wa_raft_queue:apply_queue_full(?TABLE, 1)).

%%-------------------------------------------------------------------
%% SERVER UNIT TESTS
%%-------------------------------------------------------------------

-spec server(ct_suite:ct_config()) -> ok.
server(Config) ->
    Server = start_server(Config),
    ?assertNotEqual(undefined, whereis(wa_raft_queue:registered_name(?TABLE, 1))),
    ok = stop_server(Server).

-spec server_reset(ct_suite:ct_config()) -> ok.
server_reset(Config) ->
    ServerA = start_server(Config),
    Queue = wa_raft_queue:queues(?TABLE, 1),
    ?assert(Queue =/= undefined),
    ok = wa_raft_queue:commit_started(Queue, high),
    ok = wa_raft_queue:commit_started(Queue, high),
    ok = wa_raft_queue:commit_started(Queue, high),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_read(Queue),
    ok = wa_raft_queue:reserve_apply(Queue, 10),
    ok = wa_raft_queue:reserve_apply(Queue, 10),
    ok = wa_raft_queue:reserve_apply(Queue, 10),
    ok = stop_server(ServerA),

    ServerB = start_server(Config),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(Queue, high)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size(?TABLE, 1, high)),
    ?assertEqual(0, wa_raft_queue:apply_queue_size(Queue)),
    ?assertEqual(0, wa_raft_queue:apply_queue_size(?TABLE, 1)),
    ?assertEqual(0, wa_raft_queue:apply_queue_byte_size(Queue)),
    ?assertEqual(0, wa_raft_queue:apply_queue_byte_size(?TABLE, 1)),
    ok = wa_raft_queue:fulfill_all_reads(Queue, {error, read_queue_full}),
    ?assertError(_, ?assertReceive(_)),
    ok = stop_server(ServerB).

-spec missing(ct_suite:ct_config()) -> ok.
missing(_Config) ->
    ?assertEqual(undefined, wa_raft_queue:queues('$not_a_table', 1)),
    ?assertEqual(0, wa_raft_queue:commit_queue_size('$not_a_table', 1, high)),
    ?assertNot(wa_raft_queue:commit_queue_full('$not_a_table', 1, high)),
    ?assertEqual(0, wa_raft_queue:apply_queue_size('$not_a_table', 1)),
    ?assertNot(wa_raft_queue:apply_queue_full('$not_a_table', 1)).
