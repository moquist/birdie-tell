# birdie-tell

<img alt="Gossip in action" src="http://blogs.unimelb.edu.au/sciencecommunication/files/2014/10/Screen-shot-2014-10-20-at-6.34.52-PM.png"/>

This is a small Clojure application/library that implements a simple
data-propagation Gossip protocol for fun and exploration.

As a simple use case, consider a router named Zelda that can route to networks A, B, and D. The gossip protocol implemented by this library would allow Zelda to inform her peer routers Yolanda, Xerces, and Wenderson that she can route to networks A, B, and D. Yolanda, Xerces, and Wenderson can then send Zelda traffic for those networks.

Lessons learned:
1. Distributed systems are harder than they seem, and (2) #1 is true even after you've accepted #1.

Meetup todo:
1. write up model: what are its properties?
2. write up problems & correct gossip implementations
3. write up problems to solve

TODOs:
1. detect and :down peers *nobody's* heard from in a while
1. TTL to avoid passing *all* data on every time
1. Implement 'output-stream-handler in order to reply to gossip with gossip, rather than just receiving gossip silently.
1. use inotify lib to watch input-file
1. switch from input-file to listening socket
1. Add error-checking to 'split-hostport.
1. Write some generative tests of this library/application.
1. Use transit (https://github.com/cognitect/transit-clj) for transport.

1. Carefully define gossip convergence.
1. Articulate a [better] model of failure detection.

## Usage

The following example starts a peer that:

1. watches `data00.edn` as the backing data store,
1. has UUID `uuid:0`,
1. is named `yelnats`,
1. listens on `127.0.0.1:2400`, and
1. will try occasionally to talk to `127.0.0.1:2401`.

```bash
lein run -- --input-file data00.edn --uuid uuid:0 --name yelnats --host-port 127.0.0.1:2400 --peer 127.0.0.1:2401
```

While the peer is running, edit the file `data01.edn` and save your changes. Watch the peer's output to see the `:data` update, and the `:version` be incremented.

## Example

Open four terminals so you can see them all on one screen. [Divvy](http://mizage.com/divvy/) is great for arranging these. In these four terminals, run the following commands:

### Terminal A
`./run.sh 0`

### Terminal B
`./run.sh 1`

### Terminal C
`./run.sh 2`

### Terminal D
`./run.sh 3`

After they all start showing that they're all :alive, `CTRL-c` one or two (or three!) of them, then restart them, and watch the group recover.

## Resources

1. https://github.com/michaelklishin/vclock
1. http://courses.csail.mit.edu/6.895/fall02/papers/Ladin/acmtocs.pdf
1. https://code.google.com/p/cassandra-shawn/wiki/GossipProtocol
1. http://doc.akka.io/docs/akka/snapshot/common/cluster.html
1. https://github.com/edwardcapriolo/gossip
1. http://courses.washington.edu/css434/students/Gossip.pdf

## TODO

1. Choose a better name.
1. Describe the protocol in here, or link to it, or something.
1. Switch to transit for comm.
1. Improve documentation, examples, etc.

## License

Copyright © 2015 Matt Oquist

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.