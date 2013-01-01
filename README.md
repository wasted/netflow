![netflow.io](http://netflow.io/images/github/netflow.png)

=======

This project aims to provide an extensible flow collector written in [Scala](http://scala-lang.org), using [Netty](http://netty.io) as IO Framework as well as [Akka](http://akka.io) as Messaging Framework. It is actively being developed by [wasted.io](https://twitter.com/wastedio), so don't forget to follow us on Twitter. :)


## Supported flow-types

- NetFlow v5 - fully
- NetFlow v9 ([RFC3954](http://tools.ietf.org/html/rfc3954)) - almost fully
- NetFlow IPFIX/v10 ([RFC3917](http://tools.ietf.org/html/rfc3917), [RFC3955](http://tools.ietf.org/html/rfc3955)) - untested due to the lack of an exporter


## Supported storage backends

- Currently only the [Redis](http://redis.io) Key-Value-Store is supported


## Future roadmap / what's missing

- NetFlow v1
- NetFlow v6
- NetFlow v7
- NetFlow v9 and v10/IPFIX Sampling and Option flows
- SFlow support
- Cassandra storage support


## What we won't implement

- NetFlow v8 - totally weird format, would take too much time. [Check yourself…](http://netflow.caligare.com/netflow_v8.htm)


## Compiling

You need our utility library [wasted-util](http://wasted.github.com/scala-util) installed in your local sbt repository, since it's not mirrored on any Ivy repository yet. But we are working on that!

```
  ./sbt compile
```


## Configuration

Since we are using a Key-Value backed architecture, we are unable to "search" for things. We are only able to look them up. So we start with a set of allowed sender IP/Port combinations of your NetFlow exporter. If you do not know which IP your router uses to export NetFlows, use tcpdump on the Machine you are sending it to and run [tcpdump](http://www.tcpdump.org/tcpdump_man.html) to find out the IP/Port. If you are not familiar with tcpdump yet (you really should be!), check out our Troubleshooting Section below.

Another suggestion is, if your router supports it, set the exporter's source IP and Port to something static.

Most flow-collectors will use a new port when the daemon restarts or the system reboot. Keep that in mind!

Launch the redis-cli on your shell:

```
$ redis-cli
redis 127.0.0.1:6379> 
```

Now create a Redis Set **senders** and populate it with **10.0.0.1/1337** or whatever your sender might be. Feel free to add multiple!

```
sadd senders 10.0.0.1/1337
```

Now add the networks you would like to monitor from that sender. **Of course we also support IPv6!**

```
sadd sender:10.0.0.1/1337 192.168.0.0/8
sadd sender:10.0.0.1/1337 2001:db8::/32
```

If you are interested in Redis, check out the [commands](http://redis.io/commands) and [data types](http://redis.io/topics/data-types).


## Running

Go inside the project's directory and run the sbt command:

```
  ./sbt ~run
```


## Packaging and Deployment

If you think it's ready for deployment (**we don't yet**), you can make yourself a .jar-file by running:

```
  ./sbt assembly
```

## FAQ - Frequently Asked Questions

#### Q1: Why did you make the source IP/Port combination, not only IP?

Since NetFlow uses UDP, there is no way to verify the sender through ACK like there is with TCP. (Meaning if the connection is established, it's less likely to be a spoofed packet). In any case, having to guess the exporters source IP/Port AND the collectors correct IP/Port, makes it a little bit harder to inject malicious flow-packets into your collector. If your collector is on-site and you are able to VLAN/firewall it accordingly, then i suggest to everybody to do so!


#### Q2: Why did you choose a slash(/)-notation to separate IPs from Ports inside the Key-Value-Store?

Since we did not want to implement two parsers for handling **IPv4**:**Port** and [**IPv6**]:**Port** formats like [**2001:db8::1**]:**80**, we decided to use this notation throughout the product to simplify the code.

#### Q3: I just started the collector with loglevel Debug, it shows 0/24 flows passed, why?

NetFlow consists of two packet types, FlowSet Templates and DataFlows. Templates are defined on a per-router/exporter basis so each has their own. In order to work through DataFlows, you need to have received the Template first to make sense of the data. The issue is usually that your exporter might need a few minutes (10-60) to send you the according Template. If you use IPv4 and IPv6 (NetFlow v9 or IPFIX), the router is likely to send you templates for both protocols. If you want to know more about disecting NetFlow v9, be sure to check out [RFC3954](http://tools.ietf.org/html/rfc3954).

#### Q4: Which NetFlow exporter do you recommend?

Since we are heavy BSD fanatics, we encourage everyone to use [FreeBSD ng_netflow](http://www.freebsd.org/cgi/man.cgi?query=ng_netflow&sektion=4&manpath=FreeBSD-CURRENT). For OpenBSD there is [pflow](http://www.openbsd.org/cgi-bin/man.cgi?query=pflow&apropos=0&sektion=4&manpath=OpenBSD+Current&arch=i386&format=html) (which is a little bit broken in regards to exporting AS-numbers which are in the kernel through OpenBGPd). We **advice against a pcap** based exporter since it tends to drop long-living connections (like WebSockets) which exceed ~10 minutes in time.

#### Q5: I don't have a JunOS, Cisco IOS, FreeBSD or OpenBSD based router, what can i do?

Our suggestion would be to check your Switch's capability for [port mirroring](http://en.wikipedia.org/wiki/Port_mirroring). Mirror your upstream port to a FreeBSD machine which does the actual NetFlow collection and exporting.

**This is also beneficial since the NetFlow collection does not impair your router's performance.**

## Troubleshooting

First rule for debugging: use [tcpdump](http://www.tcpdump.org/) to verify you are accepting from the right IP/Port combination.

If you need a little guidance for using tcpdump, here is what you do as **root** or with **sudo**:

```
# tcpdump -i <your ethernet device> host <your collector ip>
```

As a working example for Linux:

```
# tcpdump -i eth0 host 10.0.0.5
```

If you suspect the UDP Packet coming from a whole network, you can tell tcpdump to filter for it:

```
# tcpdump -i eth0 net 10.0.0.0/24 and port 2250
```

You might want to subtitute the port with the port netflow.io/the collector is running on.

Just grab the source-ip and port where packets are coming from and add it into the database as formatted **IP**/**Port**.

By the way, tcpdump has an awesome [manual](http://www.tcpdump.org/tcpdump_man.html)!


## License


```
  Copyright 2012 wasted.io Ltd <really@wasted.io>

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
```
