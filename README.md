![netflow.io](http://netflow.io/images/soon/netflow.png)

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


## Running

```
  ./sbt ~run
```


## Packaging

```
  ./sbt assembly
```


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

