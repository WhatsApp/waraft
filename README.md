# WhatsApp Raft - WARaft

WARaft is a Raft library in Erlang by WhatsApp. It provides an Erlang implementation to obtain consensus among replicated state machines. Consensus is a fundamental problem in fault-tolerant distributed systems. WARaft has been used as consensus provider in WhatsApp message storage, which is a large scale strongly consistent storage system across 5+ datacenters. 

## Features

* Full implementation of Raft consensus algorithm defined in https://raft.github.io/
* Extensible framework. It offers pluggable component interface for log, state machines and transport layer. Users are also allowed provide their own implementation to customize .
* Performant. It is highly optimized for large volume transactions user cases. It could support up to 200K/s transactions with in a 5 node cluster.
* Distributed key value store. WARaft provides components needed to build a distributed key-value storage.

## Get Started

The following code snippet gives a quick glance about how WARaft works. It creates single node WARaft cluster and write a record.

The example directory contains a generic key-value store built on top of WARaft. 

```
% Cluster config - single node. table name test, partition 1
1> Config = #{table => test, partition => 1, nodes => ['nonode@nohost']}.

% Start raft processes
2> supervisor:start_child(wa_raft_sup, #{id => wa_raft_part_top_sup, start => {wa_raft_part_top_sup, start_link, [[Args]]}, restart =>permanent, type => supervisor, shutdown => infinity, modules => [wa_raft_part_top_sup]}).

% Commit a write to table raft on partition 1
3> wa_raft_acceptor:commit(raft_acceptor_test_1, {make_ref(), {write, test, 1, 1}}).

% Read the record
4> wa_raft_acceptor:commit(raft_acceptor_test_1, {make_ref(), {read, test, 1}}).

```

## License

WARaft is [Apache licensed](./LICENSE).

