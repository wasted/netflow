package io.netflow.lib

import scala.concurrent.duration._
import io.wasted.util.{ Config, Logger }
import java.net.InetSocketAddress

private[netflow] object NodeConfig extends Logger {

  case class ServerConfig(
    statuslog: Duration,
    debugStackTraces: Boolean,
    netflows: Seq[InetSocketAddress],
    sflows: Seq[InetSocketAddress],
    pollInterval: Int,
    flushInterval: Int,
    extraFields: Boolean,
    storage: String,
    redis: RedisConfig,
    cassandra: CassandraConfig)

  case class RedisConfig(
    host: String,
    port: Int,
    maxConns: Int)

  case class CassandraConfig(
    hosts: Seq[String],
    keyspace: String)

  private var config: ServerConfig = load()

  private def load(): ServerConfig = {
    val redis = RedisConfig(
      maxConns = Config.getInt("server.redis.maxConns", 100),
      host = Config.get("server.redis.host", "127.0.0.1"),
      port = Config.getInt("server.redis.port", 6379))

    val cassandra = CassandraConfig(
      keyspace = Config.getString("server.cassandra.keyspace", "netflow"),
      hosts = Config.getStringList("server.cassandra.hosts", List("localhost:9160")))

    val server = ServerConfig(
      statuslog = Config.getDuration("server.statuslog", 10 seconds),
      debugStackTraces = Config.getBool("server.debugStackTraces", true),
      netflows = Config.getInetAddrList("server.netflows", List("0.0.0.0:2055")),
      sflows = Config.getInetAddrList("server.sflows", List("0.0.0.0:6343")),
      pollInterval = Config.getInt("server.pollInterval", 10),
      flushInterval = Config.getInt("server.flushInterval", 5),
      extraFields = Config.getBool("server.extraFields", false),
      storage = Config.getString("server.storage").filter(List("redis", "cassandra").contains).getOrElse("redis"),
      redis = redis,
      cassandra = cassandra)

    //info("Loading Configuration: %s", server)
    //info("Loading Configuration: %s", Serialization.write(server))
    //info("Loading Configuration: %s", Serialization.writePretty(server))
    server
  }

  def reload(): Unit = synchronized(config = load())

  def values = config

}

