# WhatsApp Raft - WARaft

WARaft is a Raft library in Erlang by WhatsApp. It provides an Erlang implementation to obtain consensus among replicated state machines. Consensus is a fundamental problem in fault-tolerant distributed systems. WARaft has been used as consensus provider in WhatsApp message storage, which is a large scale strongly consistent storage system across 5+ datacenters.

## Features

* Full implementation of Raft consensus algorithm defined in https://raft.github.io/
* Extensible framework. It offers pluggable component interface for log, state machines and transport layer. Users are also allowed provide their own implementation to customize .
* Performant. It is highly optimized for large volume transactions user cases. It could support up to 200K/s transactions with in a 5 node cluster.
* Distributed key value store. WARaft provides components needed to build a distributed key-value storage.

## Get Started

The following code snippet gives a quick glance about how WARaft works. It creates a single-node WARaft cluster and writes and reads a record.

```erlang
% Setup the WARaft application and the host application
rr(wa_raft_server).
application:ensure_all_started(wa_raft).
application:set_env(test_app, raft_database, ".").
% Create a spec for partition 1 of the RAFT table "test" and start it.
Spec = wa_raft_sup:child_spec(test_app, [#{table => test, partition => 1}]).
% Here we add WARaft to the kernel's supervisor, but you should place WARaft's
% child spec underneath your application's supervisor in a real deployment.
supervisor:start_child(kernel_sup, Spec).
% Check that the RAFT server started successfully
wa_raft_server:status(raft_server_test_1).
% Make a cluster configuration with the current node as the only member
Config = wa_raft_server:make_config([#raft_identity{name = raft_server_test_1, node = node()}]).
% Bootstrap the RAFT server to get it started
wa_raft_server:bootstrap(raft_server_test_1, #raft_log_pos{index = 1, term = 1}, Config, #{}).
% Wait for the RAFT server to become the leader
wa_raft_server:status(raft_server_test_1).
% Read and write against a key
wa_raft_acceptor:commit(raft_acceptor_test_1, {make_ref(), {write, test, key, 1000}}).
wa_raft_acceptor:read(raft_acceptor_test_1, {read, test, key}).
```

A typical output would look like the following:

```erlang
1> % Setup the WARaft application and the host application
   rr(wa_raft_server).
[raft_application,raft_identifier,raft_identity,raft_log,
 raft_log_pos,raft_options,raft_state]
2> application:ensure_all_started(wa_raft).
{ok,[wa_raft]}
3> application:set_env(test_app, raft_database, ".").
ok
4> % Create a spec for partition 1 of the RAFT table "test" and start it.
   Spec = wa_raft_sup:child_spec(test_app, [#{table => test, partition => 1}]).
#{id => wa_raft_sup,restart => permanent,shutdown => infinity,
  start =>
      {wa_raft_sup,start_link,
                   [test_app,[#{table => test,partition => 1}],#{}]},
  type => supervisor,
  modules => [wa_raft_sup]}
5> % Here we add WARaft to the kernel's supervisor, but you should place WARaft's
   % child spec underneath your application's supervisor in a real deployment.
   supervisor:start_child(kernel_sup, Spec).
{ok,<0.101.0>}
6> % Check that the RAFT server started successfully
   wa_raft_server:status(raft_server_test_1).
[{state,stalled},
 {id,nonode@nohost},
 {table,test},
 {partition,1},
 {data_dir,"./test.1"},
 {current_term,0},
 {voted_for,undefined},
 {commit_index,0},
 {last_applied,0},
 {leader_id,undefined},
 {next_index,#{}},
 {match_index,#{}},
 {log_module,wa_raft_log_ets},
 {log_first,0},
 {log_last,0},
 {votes,#{}},
 {inflight_applies,0},
 {disable_reason,undefined},
 {config,#{version => 1,membership => [],witness => []}},
 {config_index,0},
 {witness,false}]
7> % Make a cluster configuration with the current node as the only member
   Config = wa_raft_server:make_config([#raft_identity{name = raft_server_test_1, node = node()}]).
#{version => 1,
  membership => [{raft_server_test_1,nonode@nohost}],
  witness => []}
8> % Bootstrap the RAFT server to get it started
   wa_raft_server:bootstrap(raft_server_test_1, #raft_log_pos{index = 1, term = 1}, Config, #{}).
ok
9> % Wait for the RAFT server to become the leader
   wa_raft_server:status(raft_server_test_1).
[{state,leader},
 {id,nonode@nohost},
 {table,test},
 {partition,1},
 {data_dir,"./test.1"},
 {current_term,1},
 {voted_for,nonode@nohost},
 {commit_index,2},
 {last_applied,2},
 {leader_id,nonode@nohost},
 {next_index,#{}},
 {match_index,#{}},
 {log_module,wa_raft_log_ets},
 {log_first,1},
 {log_last,2},
 {votes,#{}},
 {inflight_applies,0},
 {disable_reason,undefined},
 {config,#{version => 1,
           membership => [{raft_server_test_1,nonode@nohost}],
           witness => []}},
 {config_index,1},
 {witness,false}]
10> % Read and write against a key
    wa_raft_acceptor:commit(raft_acceptor_test_1, {make_ref(), {write, test, key, 1000}}).
ok
11> wa_raft_acceptor:read(raft_acceptor_test_1, {read, test, key}).
{ok,1000}
```

The [example directory](https://github.com/WhatsApp/waraft/tree/main/examples/kvstore/src) contains an example generic key-value store built on top of WARaft.

## License

WARaft is [Apache licensed](./LICENSE).
