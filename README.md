# WhatsApp Raft - WARaft

WARaft is a Raft library in Erlang by WhatsApp. It provides an Erlang implementation to obtain consensus among replicated state machines. Consensus is a fundamental problem in fault-tolerant distributed systems. WARaft has been used as consensus provider in WhatsApp message storage, which is a large scale strongly consistent storage system across 5+ datacenters. 

## Features

* Full implementation of Raft consensus algorithm defined in https://raft.github.io/
* Extensible framework. It offers pluggable component interface for log, state machines and transport layer. Users are also allowed provide their own implementation to customize .
* Performant. It is highly optimized for large volume transactions user cases. It could support up to 200K/s transactions with in a 5 node cluster.
* Distributed key value store. WARaft provides components needed to build a distributed key-value storage.

## Get Started

The following code snippet gives a quick glance about how WARaft works. It creates single node WARaft cluster and write a record.

The [example directory](https://github.com/WhatsApp/waraft/tree/doc/examples/kvstore/src) contains a generic key-value store built on top of WARaft. 

```
% Cluster config - single node. table name test, partition 1
1> Spec = wa_raft_sup:child_spec([#{table => test, partition => 1, nodes => [node()]}]).

% Start raft processes under kernel_sup as supervisor. It's for demo purpose only. An app supervisor should be used for a real case
2> supervisor:start_child(kernel_sup, Spec).
{ok,<0.140.0>}

% Check raft server status
3> wa_raft_server:status(raft_server_test_1).
[{state,stalled},
 {id,nonode@nohost},
 {partition,1},
 {data_dir,"missing/test.1/"},
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
 {offline_peers,[]},
 {config,#{version => 1}}]

% Promote current node as leader
4> wa_raft_server:promote(raft_server_test_1, 1, true, #{version => 1, membership => [{raft_server_test_1, node()}]}).
ok

5> wa_raft_server:status(raft_server_test_1).
[{state,leader},  % leader node
 {id,nonode@nohost},
 {partition,1},
 {data_dir,"missing/test.1/"},
 {current_term,1},
 {voted_for,undefined},
 {commit_index,1},
 {last_applied,1},
 {leader_id,nonode@nohost},
 {next_index,#{}},
 {match_index,#{}},
 {log_module,wa_raft_log_ets},
 {log_first,0},
 {log_last,1},
 {votes,#{}},
 {inflight_applies,0},
 {disable_reason,undefined},
 {offline_peers,[]},
 {config,#{membership => [{raft_server_test_1,nonode@nohost}],
           version => 1}}]

% Write {key, 1000} to raft_server_test_1
6> wa_raft_acceptor:commit(raft_acceptor_test_1, {make_ref(), {write, test, key, 1000}}).
{ok, 2}

% Read key
7> wa_raft_acceptor:commit(raft_acceptor_test_1, {make_ref(), {read, test, key}}).
{ok,{1000,#{},2}}

% Stop raft
8> supervisor:terminate_child(kernel_sup, wa_raft_sup).
ok
9> supervisor:delete_child(kernel_sup, wa_raft_sup).
ok
```

## License

WARaft is [Apache licensed](./LICENSE).

