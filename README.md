![netflow.io](http://netflow.io/images/github/netflow.png)

=======

This project aims to provide an extensible flow collector written in [Scala](http://scala-lang.org), using [Netty](http://netty.io) as IO Framework as well as [Akka](http://akka.io) as Messaging Framework. It is actively being developed by [wasted.io](https://twitter.com/wastedio), so don't forget to follow us on Twitter. :)

### We do allow pull requests, but please follow the [contribution guidelines](https://github.com/wasted/netflow/blob/master/CONTRIBUTING.md).


## Supported flow-types

- NetFlow v1
- NetFlow v5
- NetFlow v6
- NetFlow v7
- NetFlow v9 ([RFC3954](http://tools.ietf.org/html/rfc3954))
- NetFlow IPFIX/v10 ([RFC3917](http://tools.ietf.org/html/rfc3917), [RFC3955](http://tools.ietf.org/html/rfc3955))


## Supported storage backends

Currently only the [Redis](http://redis.io) Key-Value-Store is supported, but since our [backends/Redis.scala](https://github.com/wasted/netflow/blob/master/src/main/scala/io/netflow/backends/Redis.scala) only has ~100 lines, it should not be a problem to dock onto other databases.


## What we won't implement

- NetFlow v8 - totally weird format, would take too much time. [Check yourself…](http://netflow.caligare.com/netflow_v8.htm)

**If we get a pull-request, we won't refuse it. ;)**

## Roadmap and Bugs

Can both be found in the Issues section up top.


## Compiling

You need our utility library [wasted-util](http://wasted.github.com/scala-util) installed in your local sbt repository, since it's not mirrored on any Ivy repository yet. But we are working on that!

```
  ./sbt compile
```


## Configuration

Since we are using a Key-Value backed architecture, we are unable to "search" for things. We are only able to look them up. So we start with a set of allowed sender IP/Port combinations of your NetFlow exporter.

#### Finding the IP and Port

If you do not know which IP your router uses to export NetFlows, just launch [netflow.io](http://wasted.github.com/netflow) in Loglevel Debug (default) and it will print "ignoring flow"-lines which will tell you about the IP/Port combination.

If that does not work, use [tcpdump](http://www.tcpdump.org/tcpdump_man.html) to find out the IP and Port. If you are not familiar with tcpdump yet (you really should be!), check out our Troubleshooting Section below.

As a last resort, we implemented the possibility to use **Port 0** in the Database as a **wildcard** which will match any Port.

#### Our suggestion

If your router supports it, set the exporter's source IP and Port to something static.

Most flow-collectors will use a new port when the daemon restarts or the system reboot. Keep that in mind!

#### Setting up the Database

Launch the redis-cli on your shell:

```
$ redis-cli
redis 127.0.0.1:6379> 
```

Now create a Redis Set **senders** and populate it with **10.0.0.1/1337** or whatever your sender might be. Feel free to add multiple!

Remember that **Port 0** will disable Port-checking for that particular host!

```
sadd senders 10.0.0.1/1337
sadd senders 10.0.0.2/0
```

Now add the networks you would like to monitor from that sender. **Of course we also support IPv6!**

```
sadd sender:10.0.0.1/1337 192.168.0.0/24
sadd sender:10.0.0.1/1337 2001:db8::/32

sadd sender:10.0.0.2/0 192.168.0.0/24
sadd sender:10.0.0.2/0 2001:db8::/32
```

If you are interested in Redis, check out the [commands](http://redis.io/commands) and [data types](http://redis.io/topics/data-types).


## Running

Go inside the project's directory and run the sbt command:

```
  ./sbt ~run
```


## Packaging

If you think it's ready for deployment, you can make yourself a .jar-file by running:

```
  ./sbt assembly
```

## Deployment

There are paramters for the [configuration file](https://raw.github.com/wasted/netflow/master/src/main/resources/sample.conf) and the [logback config](https://raw.github.com/wasted/netflow/master/src/main/resources/logback.production.xml). To run the application, try something like ths:

```
java -server      								\
	-XX:+AggressiveOpts      					\
	-Dconfig.file=application.conf				\
	-Dlogback.configurationFile=logback.xml		\
	-Dio.netty.epollBugWorkaround=true			\ # only useful on Linux as it is bugged
	-jar netflow.jar
```

A more optimized version can be found in the [run shellscript](https://raw.github.com/wasted/netflow/master/run).

We are open to suggestions for some more optimal JVM parameters. Please consider opening a pull request if you think you've got an optimization.


## FAQ - Frequently Asked Questions

#### Q1: Why did you make the source IP/Port combination, not only IP?

Since NetFlow uses UDP, there is no way to verify the sender through ACK like there is with TCP. (Meaning if the connection is established, it's less likely to be a spoofed packet). In any case, having to guess the exporters source IP/Port AND the collectors correct IP/Port, makes it a little bit harder to inject malicious flow-packets into your collector. If your collector is on-site and you are able to VLAN/firewall it accordingly, then i suggest to everybody to do so!

As mentioned in the guide above, there is **Port 0** which can be used as a wildcard. Simply using:

```
sadd senders 10.0.0.2/0

sadd sender:10.0.0.2/0 192.168.0.0/24
sadd sender:10.0.0.2/0 2001:db8::/32
```

However, this comes with the downside that your NetFlow sender/exporter can only have one exporting process. While this is not a problem with Juniper, Cisco or Routers in general, **this is inmportant** if you run a NetFlow Aggregator/Collector-Redistributor.

#### Q2: Why did you choose a slash(/)-notation to separate IPs from Ports inside the Key-Value-Store?

Since we did not want to implement two parsers for handling **IPv4:Port** and **[IPv6]:Port** formats like **[2001:db8::1]:80**, we decided to use this notation throughout the product to simplify the code.

#### Q3: I just started the collector with loglevel Debug, it shows 0/24 flows passed, why?

NetFlow v9 and v10 (IPFIX) consist of two packet types, FlowSet Templates and DataFlows. Templates are defined on a per-router/exporter basis so each has their own. In order to work through DataFlows, you need to have received the Template first to make sense of the data. The issue is usually that your exporter might need a few minutes (10-60) to send you the according Template. If you use IPv4 and IPv6 (NetFlow v9 or IPFIX), the router is likely to send you templates for both protocols. If you want to know more about disecting NetFlow v9, be sure to check out [RFC3954](http://tools.ietf.org/html/rfc3954).

#### Q4: I just started the collector, it shows an **IllegalFlowDirectionException** or **None.get**, why?

Basically the same as above, while your collector was down, your sender/exporter might have updated its template. If that happens, your netflow.io misses the update and cannot parse current packets. You will have to wait until the next template arrives.

#### Q5: Which NetFlow exporter do you recommend?

We encourage everyone to use [FreeBSD ng_netflow](http://www.freebsd.org/cgi/man.cgi?query=ng_netflow&sektion=4&manpath=FreeBSD-CURRENT) or [OpenBSD pflow](http://www.openbsd.org/cgi-bin/man.cgi?query=pflow&apropos=0&sektion=4&manpath=OpenBSD+Current&arch=i386&format=html) (which is a little bit broken in regards to exporting AS-numbers which are in the kernel through OpenBGPd). We **advice against all pcap** based exporters and collectors since they tend to drop long-living connections (like WebSockets) which exceed ~10 minutes in time.

#### Q6: I don't have a JunOS, Cisco IOS, FreeBSD or OpenBSD based router, what can i do?

Our suggestion would be to check your Switch's capability for [port mirroring](http://en.wikipedia.org/wiki/Port_mirroring).

Mirror your upstream port to a FreeBSD machine which does the actual NetFlow collection and exporting.

**This is also beneficial since the NetFlow collection does not impact your router's performance.**

#### Q7: Is it stable and ready for production?

Our patchlevel internally used might be production level, but we are heavily developing towards our first stable public release!

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

If you suspect the UDP Packet coming from a whole network, you can tell tcpdump to filter for it.

You might want to subtitute the default port 2250 with the port your [netflow.io](http://wasted.github.com/netflow) collector is running on.

```
# tcpdump -i eth0 net 10.0.0.0/24 and port 2250
```

Just grab the source-ip and port where packets are coming from and add it into the database as formatted **IP/Port**.

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
