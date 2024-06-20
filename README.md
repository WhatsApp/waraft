# WhatsApp Raft - WARaft

WARaft is a Raft library in Erlang by WhatsApp. It provides an Erlang implementation to obtain consensus among replicated state machines. Consensus is a fundamental problem in fault-tolerant distributed systems. WARaft has been used as consensus provider in WhatsApp message storage, which is a large scale strongly consistent storage system across 5+ datacenters.

## Features

* Full implementation of Raft consensus algorithm defined in https://raft.github.io/
* Extensible framework. It offers pluggable component interface for log, state machines and transport layer. Users are also allowed provide their own implementation to customize .
* Performant. It is highly optimized for large volume transactions user cases. It could support up to 200K/s transactions with in a 5 node cluster.
* Distributed key value store. WARaft provides components needed to build a distributed key-value storage.

## Get Started

The following code snippet gives a quick glance about how WARaft works. It creates single node WARaft cluster and write a record.

The [example directory](https://github.com/WhatsApp/waraft/tree/main/examples/kvstore/src) contains a generic key-value store built on top of WARaft.

```
% Remember: This is for getting started with waraft, NOT a deployment guide.

% Init ets tables used by raft
1> wa_raft_info:init_tables().
ok
2> wa_raft_log_catchup:init_tables().
wa_raft_log_catchup

% Set test database storage location in erlang application environment.
% /tmp is used here as this is most likely available on any system to get started easily.
% In case of error clear out raft database before start, e.g. rm -rf /tmp/watest.1/
3> application:set_env(watest, raft_database, '/tmp').
ok

% Cluster config - single node. table name test, partition 1
4> Spec = wa_raft_sup:child_spec(watest, [#{table => watest, partition => 1, nodes => [node()]}]).
#{id => wa_raft_sup,restart => permanent,shutdown => infinity,
  start =>
      {wa_raft_sup,start_link,
                   [watest,
                    [#{table => watest,nodes => [yo@fedora],partition => 1}],
                    #{}]},
  type => supervisor,
  modules => [wa_raft_sup]}

% Start raft processes under kernel_sup as supervisor. It's for demo purpose only. An app supervisor should be used for a real case
5> supervisor:start_child(kernel_sup, Spec).
{ok,<0.219.0>}

% Check raft server status. We expect state to be stalled.
6> wa_raft_server:status(raft_server_watest_1).
[{state,stalled},
 {id,yo@fedora},
 {table,watest},
 {partition,1},
 {data_dir,"/tmp/watest.1"},
 {current_term,1},
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
 {config,#{version => 1}},
 {config_index,0},
 {witness,false}]

% Promote current node as leader ( in case you get {error,rejected} read not for 3> )
7> wa_raft_server:promote(raft_server_watest_1, 1, true, #{version => 1, membership => [{raft_server_watest_1, node()}]}).
ok

% Confirm promotion was successful
8> wa_raft_server:status(raft_server_watest_1).
[{state,leader},
 {id,yo@fedora},
 {table,watest},
 {partition,1},
 {data_dir,"/tmp/watest.1"},
 {current_term,1},
 {voted_for,undefined},
 {commit_index,1},
 {last_applied,1},
 {leader_id,yo@fedora},
 {next_index,#{}},
 {match_index,#{}},
 {log_module,wa_raft_log_ets},
 {log_first,0},
 {log_last,1},
 {votes,#{}},
 {inflight_applies,0},
 {disable_reason,undefined},
 {config,#{version => 1,
           membership => [{raft_server_watest_1,yo@fedora}]}},
 {config_index,1},
 {witness,false}]

% Write {key, 1000} to raft_server_test_1
9> wa_raft_acceptor:commit(raft_acceptor_watest_1, {make_ref(), {write, test, key, 1000}}).
ok

% Read key
10> wa_raft_acceptor:read(raft_acceptor_watest_1, {read, test, key}).
{ok,1000}

% Stop raft
11> supervisor:terminate_child(kernel_sup, wa_raft_sup).
ok
12> supervisor:delete_child(kernel_sup, wa_raft_sup).
ok
```

## License

WARaft is [Apache licensed](./LICENSE).
