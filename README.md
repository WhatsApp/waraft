# WhatsApp Raft - WARaft

WARaft is an Erlang implementation of the Raft consensus algorithm for building replicated state machines. It has served as a consensus component in WhatsApp's large-scale, strongly consistent message-storage systems.

## Features

- Raft-based consensus with leader election, replicated logs, strongly consistent reads, snapshots, and membership changes, including support for non-voting participants and witnesses.
- Pluggable implementations for the replicated state machine, log, Raft RPC distribution, snapshot transport, log labels, and metrics.
- Storage-oriented controls such as command batching, high- and low-priority queues, configurable backpressure, and optional leader read leases.
- An illustrative partitioned key-value store showing how to build a storage service on WARaft.

The default ETS log and storage providers are intended for examples and tests. They are not durable across VM restarts; production deployments must supply durable providers.

## Get Started

WARaft requires Erlang/OTP 28. Build it and run its checks with Rebar3:

```shell
rebar3 compile
rebar3 ct
rebar3 do dialyzer, xref
```

The following Erlang shell session starts a single-node WARaft cluster, then writes and reads a value. It uses a unique temporary data directory and the non-durable ETS providers, so it is suitable only as a local example.

```erlang
% Load the WARaft records used below and start the application.
rr(wa_raft_server).
application:ensure_all_started(wa_raft).

% Give the host application a unique data directory for this run.
application:set_env(
    test_app,
    raft_database,
    filename:join(
        "/tmp",
        "wa_raft_quick_start_" ++ integer_to_list(erlang:system_time(microsecond))
    )
).

% Start the WARaft supervisor without partitions, then add partition 1 of
% table "test". Production applications should place the WARaft supervisor
% under their own supervision tree rather than under kernel_sup.
{ok, RaftSup} = supervisor:start_child(
    kernel_sup,
    wa_raft_sup:child_spec(test_app, [])
).
wa_raft_sup:start_partition(RaftSup, #{table => test, partition => 1}).

% A new partition remains stalled until it receives its initial configuration.
wa_raft_server:status(raft_server_test_1, state).
Config = wa_raft_server:make_config([
    #raft_identity{name = raft_server_test_1, node = node()}
]).
wa_raft_server:bootstrap(
    raft_server_test_1,
    #raft_log_pos{index = 1, term = 1},
    Config,
    #{}
).

% A successful single-member bootstrap makes this server the leader.
wa_raft_server:status(raft_server_test_1, state).

% Commit a write through the leader, then perform a strongly consistent read.
wa_raft_acceptor:commit(
    raft_acceptor_test_1,
    {make_ref(), {write, test, key, 1000}}
).
wa_raft_acceptor:read(raft_acceptor_test_1, {read, test, key}).
```

A run produces output like this (process identifiers vary):

```erlang
1> rr(wa_raft_server).
[raft_application,raft_identifier,raft_identity,raft_log,
 raft_log_pos,raft_options,raft_state]
2> application:ensure_all_started(wa_raft).
{ok,[wa_raft]}
3> application:set_env(test_app, raft_database, ...).
ok
4> {ok, RaftSup} = supervisor:start_child(kernel_sup, wa_raft_sup:child_spec(test_app, [])).
{ok,<0.89.0>}
5> wa_raft_sup:start_partition(RaftSup, #{table => test, partition => 1}).
{ok,<0.90.0>}
6> wa_raft_server:status(raft_server_test_1, state).
stalled
7> Config = wa_raft_server:make_config([
       #raft_identity{name = raft_server_test_1, node = node()}
   ]).
#{version => 1,
  membership => [{raft_server_test_1,nonode@nohost}],
  witness => [],
  participants => [{raft_server_test_1,nonode@nohost}]}
8> wa_raft_server:bootstrap(
       raft_server_test_1,
       #raft_log_pos{index = 1, term = 1},
       Config,
       #{}
   ).
ok
9> wa_raft_server:status(raft_server_test_1, state).
leader
10> wa_raft_acceptor:commit(
        raft_acceptor_test_1,
        {make_ref(), {write, test, key, 1000}}
    ).
ok
11> wa_raft_acceptor:read(raft_acceptor_test_1, {read, test, key}).
{ok,1000}
```

The `wa_raft` application starts services shared by all partitions. A host application owns a `wa_raft_sup` supervisor, and each partition runs a one-for-all process tree containing its queue, storage, log, Raft server, client acceptor, and transport cleanup worker. Applications submit reads and commits through `wa_raft_acceptor`, perform membership and lifecycle operations through `wa_raft_server`, and use `wa_raft_info` for local leader and health lookups.

A multi-node deployment starts the same partition on every participating node, installs the same membership configuration on each replica, and then triggers an election. The [key-value store example](./examples/kvstore) illustrates partition routing and the storage callbacks; it is a teaching example rather than a production-ready distributed database.

## License

WARaft is licensed under the [Apache License 2.0](./LICENSE).
