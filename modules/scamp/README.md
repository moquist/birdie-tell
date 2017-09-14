# scamp

> He was a scamp; he is a hero!
> - Les Misérables (Signet, p587)

SCAMP = SCAlable Membership Protocol

## Usage

`lein test`

## TODO

1. Implement HyParView
1. Evaluate results for clumping: https://arxiv.org/pdf/0906.0612.pdf
1. define invariants...
   1. think of this in terms of sub-strategic goal states
   1. how many iterations of the gossip protocol does it take to get a message to every node (with probability X?)
   1. what must the :downstream size be, in proportion to cluster size?
   1. there is no downstream without a corresponding upstream
   1. invariants about cluster shape (clumping, connectivity, partitionability?)

## License

Copyright © 2016 Matt Oquist <moquist@majen.net>

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
