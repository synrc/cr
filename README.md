Bizantine Chain Replication Protocol
====================================

In bancking system demands are very tight. Database
should be at least tripled, stand-by nodes should pick up
master reads from failover node, writes should be
accepted on a reasonble quorum, and leter after
recovery and rejoin should be merged, database
should be able to scale even with the RAM/DISC limitations.

All this circumstances leads us to Chain
Replication protocol as a simple and natural
feedback to this challenge.

Chain Replication protocol already used before
in such products like Google FS, HDFS, mongodb, Cassandra, etc.
They mostly provide a consistent distributed repository
for event tables or for file storege. In banking industry
we synchronize account ballance with single end-point provider
and track the transactions history log up to merging and cut-offs.

Features
--------

* Bizantine Chain Replication protocol
* Distributed transaction on replicas sequence
* Separate endpoints for HEART, CLIENT and SERVER protocols
* Quorum N+1 in 2N+1 for accepting writes
* Linear consistent ring hashing
* Automatic stand-by switchover
* Data sync on recovery
* High-performance non-blocking TCP acceptor
* HanoidDB backend database
* Pure and clean codebase

Credits
-------

* Maxim Sokhatsky
