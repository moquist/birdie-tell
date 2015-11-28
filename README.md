# birdie-tell

This is a small Clojure application/library that implements a simple
Gossip protocol for fun and exploration. At this point, it's very
preliminary and needs careful thought about the following:

1. Wall-clock distribution: bad! Use a vector clock to gossip accurately.
1. Carefully define gossip convergence.
1. Articulate a [better] model of failure detection.
1. Should there be a leader for the cluster?

## Usage

```bash
LIVE_PERCENTAGE=90
MAX_HOTNESS=2
LISTEN_PORT=2235
MY_HOST_PORT=127.0.0.1:2235
PEER_1=127.0.0.1:2236
PEER_2=127.0.0.1:2237
lein run $MAX_HOTNESS $LIVE_PERCENTAGE $LISTEN_PORT $MY_HOST_PORT $PEER_1 $PEER_2
```

Every peer always reports that it, itself, is :alive.

* `LIVE_PERCENTAGE` is the percentage liklihood that I'll try to choose a random gossip peer whom I think is :alive, vs. :dead. Gossipping with :dead peers occasionally is a good way to recover a relationship after a network partition or peer restart.
* `MAX_HOTNESS` is the number of times I'll repeat the news I've been given about a given peer. Increasing this generally increases network traffic and the amount of redundant gossip; decreasing this makes news travel more slowly.
* `LISTEN_PORT` is the port I open to listen for gossip. I might open a local port that is remapped by a firewall or tunnel, so that my peers might see me differently.
* `MY_HOST_PORT` is the host:port pair I use to identify myself to others; this is my caller-id.
* `PEER_n` is a host:port pair to add to my initial list of peers. Any number of host:port pairs may be passed here.

## Example

Open four terminals so you can see them all on one screen. [Divvy](http://mizage.com/divvy/) is great for arranging these. In these four terminals, run the following commands:

### Terminal 1
`lein run 1 90 2237 127.0.0.1:2237 127.0.0.1:2236`

### Terminal 2
`lein run 1 90 2236 127.0.0.1:2236 127.0.0.1:2235`

### Terminal 3
`lein run 1 90 2235 127.0.0.1:2235 127.0.0.1:2234`

### Terminal 4
`lein run 1 90 2234 127.0.0.1:2234 127.0.0.1:2237`

After they all start showing that they're all :alive, `CTRL-c` one or two (or three!) of them, then restart them, and watch the group recover.

## TODO

1. Choose a better name.
1. Describe the protocol in here, or link to it, or something.
1. Switch to transit for comm.
1. Improve documentation, examples, etc.

## License

Copyright © 2015 Matt Oquist

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.