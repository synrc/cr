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

You could start all nodes in separate console sesions or you
can `make start NAME=cr2` nodes and later attach to them with `make attach NAME=cr2`.
Also the start is compatible within single folders, which cause no single problem.

```erlang
> cr:test(500).

=INFO REPORT==== 7-Apr-2015::00:49:09 ===
cr:Already in Database: 12520
New record will be applied: 500
11510

> cr:dump().

(cr2@127.0.0.1)12> cr:dump().
                                               vnode   i  n        top      log        latency
    121791803110908576516973736059690251637994378581   1  1      13009     3181       2/198/64
    243583606221817153033947472119380503275988757162   2  1      13020     3346       2/183/72
    365375409332725729550921208179070754913983135743   3  1      13012     3314       3/195/64
    487167212443634306067894944238761006551977514324   4  1      13007     3179       2/183/53
    608959015554542882584868680298451258189971892905   5  2      12994     3205       2/198/67
    730750818665451459101842416358141509827966271486   6  2      13017     3320       3/184/66
    852542621776360035618816152417831761465960650067   7  2      13019     3313       2/201/75
    974334424887268612135789888477522013103955028648   8  2      13020     3182       3/178/62
   1096126227998177188652763624537212264741949407229   9  3      13021     3195       2/190/68
   1217918031109085765169737360596902516379943785810  10  3      13028     3323       3/206/65
   1339709834219994341686711096656592768017938164391  11  3      13030     3328       2/208/55
   1461501637330902918203684832716283019655932542972  12  3      13031     3174       2/185/58
ok
```

The latency in last column means the moment data is stored on all replicas.

Licenses
--------

rafter and gen_paxos are distributed under the terms of
Apache 2.0 http://www.apache.org/licenses/LICENSE-2.0.html

Credits
-------

* Maxim Sokhatsky
* Vladimir Kirillov
* Sergey Klimenko
* Valery Maleshkin
* Victor Sovietov

OM A HUM
