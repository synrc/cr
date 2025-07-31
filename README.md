Byzantine Chain Replication Database
====================================

[![Join the chat at https://gitter.im/spawnproc/cr](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/spawnproc/cr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

In banking system demands are very tight. Database
should be at least tripled, stand-by nodes should pick up
master reads from failover node, writes should be
accepted on a reasonable quorum, failover must be followed by recovery, database
should be able to scale even with the RAM/DISC limitations.

No data should be treated as written otherwise that committed to all replicas.
All this circumstances leads us to chain replication protocol as a simple and natural
feedback to this challenge.

Different replication techniques exists to satisfy replication demands.
Master-slave replication is most widely known type of replication
used before in such products like GFS, HDFS, mongodb, etc. Quorum Intersection
is another technique used in databases like Cassandra or Amazon Dynamo.
They mostly provide a consistent distributed repository
for event tables or for file storage. In banking industry
we synchronize account balances and need simple and manageable
protocol for storage consistency issuing high demand on system integrity.

There are several classes of error usually implied when dealing with failure detection.
The most weak class is fail-stop events, when the outage is normal or predictable.
The second class is crash-failures, the abnormal terminations and outages. The most strong
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
* Article about CR implementation details: http://synrc.space/apps/cr/doc/cr.htm
* Business Processing Erlang book: http://synrc.space/apps/bpe/doc/book.pdf

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
> timer:tc(cr,test,[500]).

=INFO REPORT==== 7-Apr-2015::00:56:34 ===
cr:Already in Database: 14020
New record will be applied: 500
{214369,{transactions,11510}}
```

Fore generating sample data, let say 500 transactions you may run with `cr:test(500)`.
By measuring accepting performance it's like `2000 Req/s`.

```erlang
> cr:dump().

                                               vnode   i  n        top     latency
    121791803110908576516973736059690251637994378581   1  1        391    2/198/64
    243583606221817153033947472119380503275988757162   2  1        400    2/183/72
    365375409332725729550921208179070754913983135743   3  1        388    3/195/64
    487167212443634306067894944238761006551977514324   4  1        357    2/183/53
    608959015554542882584868680298451258189971892905   5  2      12994    2/198/67
    730750818665451459101842416358141509827966271486   6  2      13017    3/184/66
    852542621776360035618816152417831761465960650067   7  2      13019    2/201/75
    974334424887268612135789888477522013103955028648   8  2      13020    3/178/62
   1096126227998177188652763624537212264741949407229   9  3      13021    2/190/68
   1217918031109085765169737360596902516379943785810  10  3      13028    3/206/65
   1339709834219994341686711096656592768017938164391  11  3      13030    2/208/55
   1461501637330902918203684832716283019655932542972  12  3      13031    2/185/58
ok
```

The latency in last column `~70 ms` means the moment data is stored on all `mnesia` replicas.
The latency in a given example is for storing async_dirty using KVS
chain linking (from `1 to 3` msg per write operation, from `1 to 2` msg for lookups)
clustered in `3 nodes` with same replicas number.

Let's say we want to see all the operations log of a given replica `391`.

```erlang
> cr:dump(391).
                                         operation         id       prev    i       size
                      transaction:389:feed::false:        391        387    1        480
                      transaction:399:feed::false:        387        382    1        500
                      transaction:375:feed::false:        382        379    1        446
                      transaction:373:feed::false:        379        378    1        446
                      transaction:383:feed::false:        378        376    1        473
                      transaction:392:feed::false:        376        374    1        500
                      transaction:360:feed::false:        374        371    1        446
                      transaction:366:feed::false:        371        370    1        473
                      transaction:370:feed::false:        370        369    1        446
                      transaction:371:feed::false:        369        368    1        446
ok
```

You may check this from the other side. First retrieve the operation and then
retrieve the transaction created during operation.

```erlang
> kvs:get(operation,391).
{ok,#operation{id = 391,version = undefined,container = log,
               feed_id = {121791803110908576516973736059690251637994378581,1},   % VNODE
               prev = 387,next = undefined,feeds = [],guard = false,
               etc = undefined,
               body = {prepare,{<0.41.0>,{1428,358105,840469}},
                               [{121791803110908576516973736059690251637994378581,1},  % SIGNATURES
                                {608959015554542882584868680298451258189971892905,2}],
                               #transaction{id = 389,version = undefined,container = feed,
                                            feed_id = undefined,prev = undefined,next = undefined,
                                            feeds = [],guard = false,etc = undefined,
                                            timestamp = undefined,beneficiary = undefined,...}},
               name = prepare,status = pending}}
```

The transaction. For linking transaction to the link you should use full XA
protocol with two-stage confirmation (1) the PUT operation followed
with (2) LINK operation to some feed, such as user account or customer admin list.

```erlang
> kvs:get(transaction,389).
{ok,#transaction{id = 389,version = undefined,
                 container = feed, feed_id = undefined, prev = undefined,
                 next = undefined, feeds = [], guard = false, etc = undefined,
                 timestamp = [], beneficiary = [],
                 subsidiary = [], amount = [],tax = [],
                 ballance = [], currency = [],
                 description = [], info = [],
                 prevdate = [], rate = [], item = []}}
```

The actiual Erlang business logic, banking transaction from `db` schema
application is stored under 389 id. So you can easily grab it unlinked
as it was stored as atomic PUT.

Licenses
--------

* consensus protols 1) raft and 2) paxos are distributed under the terms of Apache 2.0 http://www.apache.org/licenses/LICENSE-2.0.html
* cr itself is distributed under the DHARMA license: http://5ht.co/license.htm

Credits
-------

Copyright (c) 2015 Synrc Research Center s.r.o.

* Maxim Sokhatsky
* Vladimir Kirillov
* Sergey Klimenko
* Valery Meleshkin
* Victor Sovietov

OM A HUM
