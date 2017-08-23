# SCAMP update

1. The problem: distributed agreement
 1. Solution: Paxos/Raft protocols
   1. What it is: Leader election, full cluster membership knowledge required, transactional cluster agreement
   1. CAP Rating: CA-
   1. Scalability comments: voting rounds require en masse communication from the leader to all followers (larger cluster = slower voting rounds and increasing liklihood of partitioning)
   1. When to use this: if you need Consistency and Availability guarantees and are willing to tolerate downtime in the case of a Partition (network interruption) within the cluster
 1. Solution: Gossip protocols
   1. What it is: the implementation of a metaphor
   1. CAP Rating: -AP
   1. Scalability comments: gossip protocols rely on redundant communication, 
1. Naive implementation (November, 2015)
 1. used TCP for comm, every node had membership list of entire cluster

> He was a scamp; he is a hero!
> - Les Misérables (Signet, p587)



SCAMP = SCAlable Membership Protocol

## Usage

`lein test`

## TODO

. Implement HyParView
. Evaluate results for clumping: https://arxiv.org/pdf/0906.0612.pdf

## License

Copyright © 2016 Matt Oquist <moquist@majen.net>

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
