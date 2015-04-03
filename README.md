Byzantine Chain Replication Database
====================================

[![Join the chat at https://gitter.im/spawnproc/cr](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/spawnproc/cr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

In banking system demands are very tight. Database
should be at least tripled, stand-by nodes should pick up
master reads from failover node, writes should be
accepted on a reasonble quorum, failover must be followed by recovery, database
should be able to scale even with the RAM/DISC limitations.

No data should be treated as written otherwise that commited to all replicas.
All this circumstances leads us to chain replication protocol as a simple and natural
feedback to this challenge.

Different replication techniques exists to satisfy replication demands.
Master-slave replication is most widely known type of replication
used before in such products like GFS, HDFS, mongodb, etc. Quorum Intersection
is another technique used in databases like Cassandra or Amazon Dynamo.
They mostly provide a consistent distributed repository
for event tables or for file storage. In banking industry
we synchronize account balances and need simple and managable
protocol for storage consistency issuing high demand on system integrity.

There are several classes of error usually implied when dealing with failure detection.
The most weak class is fail-stop events, when the outage is normal or predictable.
The second class is crash-failures, the ubnormal terminations and outages. The most strong
type of failures are byzantine failures resistant to bit-flips,
hacked parties or any types of compromising the transaction objects.
For banking applications the byzantine fault tolerance is desired,
despite it affects the latency.


Features
--------

* Highly-available CP database :-)
* 2N+1 nodes tolerates N failures
* Consistent hashing DHT
* RAFT for managing server configurations timeline
* HMAC signing for Byzantine capabilities
* Various database backends: <b>mnesia</b>, <b>riak</b>, <b>redis</b>, <b>fs</b>, <b>sql</b>
* High-performance non-blocking TCP acceptor
* Separate endpoints for HEART, CLIENT and SERVER protocols
* Pure, clean and understandable codebase

Launch
------

```bash
make console NAME=cr
make console NAME=cr2
make console NAME=cr3
```

Credits
-------

* Maxim Sokhatsky
* Vladimir Kirillov
* Sergey Klimenko
* Valery Maleshkin
* Victor Sovietov

OM A HUM
