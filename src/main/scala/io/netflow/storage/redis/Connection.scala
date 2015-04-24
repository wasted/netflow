package io.netflow.storage.redis

import com.twitter.finagle.redis.Client
import io.netflow.lib.NodeConfig
import io.netflow.storage.{ Connection => ConnectionMeta }

private[storage] object Connection extends ConnectionMeta {
  lazy val client = Client(NodeConfig.values.redis.hosts.mkString(","))
  def start(): Unit = client
  def shutdown(): Unit = ()
}
