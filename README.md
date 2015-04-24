![netflow.io](http://netflow.io/images/github/netflow.png)

=======

This project aims to provide an extensible flow collector written in [Scala](http://scala-lang.org), 
using [Netty](http://netty.io) as IO Framework. It is actively being developed by [wasted.io](https://twitter.com/wastedio),
so don't forget to follow us on Twitter. :)

### We do allow pull requests, but please follow the [contribution guidelines](https://github.com/wasted/netflow/blob/master/CONTRIBUTING.md).


## Supported flow-types

- NetFlow v1
- NetFlow v5
- NetFlow v6
- NetFlow v7
- NetFlow v9 ([RFC3954](http://tools.ietf.org/html/rfc3954)) - options not handled yet

## To be done

- NetFlow IPFIX/v10 ([RFC3917](http://tools.ietf.org/html/rfc3917), [RFC3955](http://tools.ietf.org/html/rfc3955))


## Supported storage backends

Databases supported:
- [Redis](http://redis.io)
- [Apache Cassandra](https://cassandra.apache.org)

## What we won't implement

- NetFlow v8 - totally weird format, would take too much time. [Check yourself…](http://netflow.caligare.com/netflow_v8.htm)

**If we get a pull-request, we won't refuse it. ;)**

## Roadmap and Bugs

Can both be found in the Issues section up top.


## Compiling

```
  ./sbt compile
```


## Running

```
  ./sbt run
```


## Configuration

#### Setting up the Database

First, setup Redis or Cassandra in the [configuration file](https://raw.github.com/wasted/netflow/master/src/main/resources/reference.conf).
After, start netflow to create the keyspace and required tables.

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

There are paramters for the [configuration file](https://raw.github.com/wasted/netflow/master/src/main/resources/reference.conf)
and the [logback config](https://raw.github.com/wasted/netflow/master/src/main/resources/logback.production.xml).
To run the application, try something like ths:

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


## REST API

Once it has successfully started, you can start adding authorized senders using the HTTP REST API.
To do this, you need to use the admin.authKey and admin.signKey provided by the config.
In case you haven't configured them, netflow.io will generate random-keys on every start.
The authKey is like a public-key, the signKey more like a private key, so keep it safe.

In order to authenticate against the API, you have 2 possibilities:
- Generate a SHA256 HMAC of the authKey using the signKey
- Generate a SHA256 HMAC of the payload using the signKey (only if you have a payload)

To generate the SHA256 HMAC on your console:

```shell
echo -n "<authKey>" | openssl dgst -sha256 -hmac "<signKey>"
```      
     
It's easiest to use like this:

```shell
export NF_AUTH_KEY='<authKey>'
export NF_SIGN_KEY='<signKey>'
export NF_SIGNED_KEY=$( echo -n "${NF_AUTH_KEY}" | openssl dgst -sha256 -hmac "${NF_SIGN_KEY}" )
```

This will setup the NetFlow sender 172.16.1.1 which is monitoring 172.16.1.0/24 and 10.0.0.0/24
    
```shell
curl -X PUT -d '{"prefixes":[{"prefix":"172.16.1.0","prefixLen":24}]}' \
    -H "X-Io-Auth: ${NF_AUTH_KEY}" \
    -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
    -v http://127.0.0.1:8080/senders/172.16.1.1
```


**Of course we also support IPv6!**

```shell
curl -X PUT -d '{"prefixes":[{"prefix":"2001:db8::","prefixLen":32}]}' \
    -H "X-Io-Auth: ${NF_AUTH_KEY}" \
    -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
    -v http://127.0.0.1:8080/senders/172.16.1.1
```

**Please make sure to always use the first Address of the prefix (being 0 or whatever matches your lowest bit).**

To remove a subnet from the sender, just issue a DELETE instead of PUT with the subnet you want to delete from this sender. This also works with multiples, just like PUT.
                           
```shell
curl -X DELETE -d '{"prefixes":[{"prefix":"2001:db8::","prefixLen":32}]}' \
    -H "X-Io-Auth: ${NF_AUTH_KEY}" \
    -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
    -v http://127.0.0.1:8080/senders/172.16.1.1
```

To remove a whole sender, just issue a DELETE without any subnet.
                               
```shell
curl -X DELETE \
    -H "X-Io-Auth: ${NF_AUTH_KEY}" \
    -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
    -v http://127.0.0.1:8080/senders/172.16.1.1
```

For a list of all configured senders
                               
```shell
curl -X GET \
  -H "X-Io-Auth: ${NF_AUTH_KEY}" \
  -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
  -v http://127.0.0.1:8080/senders
```

For information about a specific sender
                               
```shell
curl -X GET \
  -H "X-Io-Auth: ${NF_AUTH_KEY}" \
  -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
  -v http://127.0.0.1:8080/senders/172.16.1.1
```
    
Querying for statistics (more to come here)
                               
```shell
curl -X POST -d '{
  "2001:db8::/32":{ 
    "years":["2013", "2014"], 
    "months":["2013-01", "2013-02"], 
    "days":["2014-02-02"], 
    "hours":["2014-02-02 05"],
    "minutes":[
      "2014-02-02 05:00",
      "2014-02-02 05:01",
      "2014-02-02 05:02",
      "2014-02-02 05:03",
      "2014-02-02 05:04"
    ]
  }}' \
  -H "X-Io-Auth: ${NF_AUTH_KEY}" \
  -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
  -v http://127.0.0.1:8080/stats/172.16.1.1
```

Querying for more detailed statistics (about dst AS 34868 and src AS 34868)

Tracked fields:
- all
- proto (6 for TCP, 17 for UDP, see /etc/protocols)
- srcip, srcport, srcas
- dstip, dstport, dstas

```shell
curl -X POST -d '{
  "2001:db8::/32":{ 
    "years":["2013", "2014"], 
    "months":["2013-01", "2013-02"], 
    "days":["2014-02-02"], 
    "hours":["2014-02-02 05"],
    "minutes":[
      {"2014-02-02 05:00": ["all", "dstas:34868", "srcas:34868"]},
      {"2014-02-02 05:01": ["all", "dstas:34868", "srcas:34868"]},
      {"2014-02-02 05:02": ["all", "dstas:34868", "srcas:34868"]},
      {"2014-02-02 05:03": ["all", "dstas:34868", "srcas:34868"]},
      {"2014-02-02 05:04": ["all", "dstas:34868", "srcas:34868"]}
    ]
  }}' \
  -H "X-Io-Auth: ${NF_AUTH_KEY}" \
  -H "X-Io-Sign: ${NF_SIGNED_KEY}" \
  -v http://127.0.0.1:8080/stats/172.16.1.1
```


## FAQ - Frequently Asked Questions

#### Q1: I just started the collector with loglevel Debug, it shows 0/24 flows passed, why?

NetFlow v9 and v10 (IPFIX) consist of two packet types, FlowSet Templates and DataFlows. Templates are defined on a
per-router/exporter basis so each has their own. In order to work through DataFlows, you need to have received the
Template first to make sense of the data. The issue is usually that your exporter might need a few minutes (10-60)
to send you the according Template. If you use IPv4 and IPv6 (NetFlow v9 or IPFIX), the router is likely to send
you templates for both protocols. If you want to know more about disecting NetFlow v9, be sure to check
out [RFC3954](http://tools.ietf.org/html/rfc3954).

#### Q2: I just started the collector, it shows an **IllegalFlowDirectionException** or **None.get**, why?

Basically the same as above, while your collector was down, your sender/exporter might have updated its template.
 If that happens, your netflow.io misses the update and cannot parse current packets. You will have to wait until
  the next template arrives.

#### Q3: Which NetFlow exporter do you recommend?

We encourage everyone to use [FreeBSD ng_netflow](http://www.freebsd.org/cgi/man.cgi?query=ng_netflow&sektion=4&manpath=FreeBSD-CURRENT) 
or [OpenBSD pflow](http://www.openbsd.org/cgi-bin/man.cgi?query=pflow&apropos=0&sektion=4&manpath=OpenBSD+Current&arch=i386&format=html)
(which is a little bit broken in regards to exporting AS-numbers which are in the kernel through OpenBGPd).
We **advice against all pcap** based exporters and collectors since they tend to drop long-living connections
(like WebSockets) which exceed ~10 minutes in time.

#### Q4: I don't have a JunOS, Cisco IOS, FreeBSD or OpenBSD based router, what can i do?

Our suggestion would be to check your Switch's capability for [port mirroring](http://en.wikipedia.org/wiki/Port_mirroring).

Mirror your upstream port to a FreeBSD machine which does the actual NetFlow collection and exporting.

**This is also beneficial since the NetFlow collection does not impact your router's performance.**

#### Q7: Is it stable and ready for production?

Not yet, but we are heavily developing towards our first stable public release!

#### Q8: Why did you use separate implementations for each NetFlow version?

We had it implemented into two classes before (LegacyFlow and TemplateFlow), but we were unhappy with "per-flow-version" 
debugging. We believe that handling each flow separately gives us more maintainability in the end than having lots of dispatching in between.

## Troubleshooting

First rule for debugging: use [tcpdump](http://www.tcpdump.org/) to verify you are receiving flows on your server.

If you need a little guidance for using tcpdump, here is what you do as **root** or with **sudo**:

```
# tcpdump -i <your ethernet device> host <your collector ip>
```

As a working example for Linux:

```
# tcpdump -i eth0 host 10.0.0.5
```

If you suspect the UDP Packet coming from a whole network, you can tell tcpdump to filter for it.

You might want to subtitute the default port 2055 with the port your [netflow.io](http://netflow.io) collector is running on.

```
# tcpdump -i eth0 net 10.0.0.0/24 and port 2055
```

Just grab the source-ip where packets are coming from and add it into the database.

By the way, tcpdump has an awesome [manual](http://www.tcpdump.org/tcpdump_man.html)!


## License


```
  Copyright 2012, 2013, 2014 wasted.io Ltd <really@wasted.io>

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
