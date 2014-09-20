package io.netflow.lib

import java.io.File
import java.net.InetSocketAddress

import io.wasted.util.ssl.{ KeyStoreType, Ssl }
import io.wasted.util.{ Config, Logger }

import scala.concurrent.duration._

private[netflow] object NodeConfig extends Logger {

  case class ServerConfig(
    cores: Int,
    statuslog: Duration,
    debugStackTraces: Boolean,
    netflow: NetFlowConfig,
    sflow: SFlowConfig,
    cassandra: CassandraConfig,
    http: HttpConfig,
    tcp: TcpConfig,
    ssl: SslConfig) {
    def sslEngine = for {
      certPath <- ssl.certPath
      if new File(certPath).exists
      certPass <- ssl.certPass
    } yield {
      val sslE = Ssl.server(certPath, certPass, KeyStoreType.P12)
      val engine = sslE.self
      engine.setNeedClientAuth(false)
      engine.setUseClientMode(false)
      engine.setEnableSessionCreation(true)
      sslE
    }
  }

  case class HttpConfig(
    listen: Seq[InetSocketAddress],
    sendFile: Boolean,
    sendBuffer: Boolean,
    gzip: Boolean,
    maxContentLength: Long,
    maxInitialLineLength: Long,
    maxChunkSize: Long,
    maxHeaderSize: Long)

  case class CassandraConfig(
    hosts: Seq[String],
    keyspace: String,
    minConns: Int,
    maxConns: Int,
    minSimRequests: Int,
    maxSimRequests: Int,
    connectTimeout: Int,
    reconnectTimeout: Int,
    readTimeout: Int,
    keyspaceConfig: String)

  case class TcpConfig(
    sendBufferSize: Option[Long],
    receiveBufferSize: Option[Long],
    noDelay: Boolean,
    keepAlive: Boolean,
    reuseAddr: Boolean,
    soLinger: Int)

  case class SslConfig(
    certPath: Option[String],
    certPass: Option[String])

  case class SFlowConfig(
    listen: Seq[InetSocketAddress],
    persist: Boolean)

  case class NetFlowConfig(
    listen: Seq[InetSocketAddress],
    persist: Boolean,
    extraFields: Boolean)

  private var config: ServerConfig = load()

  private def load(): ServerConfig = {
    val cassandra = CassandraConfig(
      keyspace = Config.getString("server.cassandra.keyspace", "netflow"),
      hosts = Config.getStringList("server.cassandra.hosts", List("localhost")),
      minConns = Config.getInt("server.cassandra.minConns", 5),
      maxConns = Config.getInt("server.cassandra.maxConns", 40),
      minSimRequests = Config.getInt("server.cassandra.minSimRequests", 5),
      maxSimRequests = Config.getInt("server.cassandra.maxSimRequests", 128),
      connectTimeout = Config.getInt("server.cassandra.connectTimeout", 5000),
      reconnectTimeout = Config.getInt("server.cassandra.reconnectTimeout", 5000),
      readTimeout = Config.getInt("server.cassandra.readTimeout", 60000),
      keyspaceConfig = Config.getString("server.cassandra.keyspaceConfig",
        "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}"))

    val cpus = Runtime.getRuntime.availableProcessors() match {
      case 1 => 1
      case x if x > 8 => x - 2
      case x if x > 4 => x - 1
    }

    val netflow = NetFlowConfig(
      listen = Config.getInetAddrList("netflow.listen", List("0.0.0.0:2055")),
      persist = Config.getBool("netflow.persist", true),
      extraFields = Config.getBool("netflow.extraFields", true))

    val sflow = SFlowConfig(
      listen = Config.getInetAddrList("sflow.listen", List("0.0.0.0:6343")),
      persist = Config.getBool("sflow.persist", true))

    val ssl = SslConfig(
      certPath = Config.getString("server.ssl.p12"),
      certPass = Config.getString("server.ssl.pass"))

    val http = HttpConfig(
      listen = Config.getInetAddrList("server.http.listen", List("127.0.0.1:8080")),
      sendFile = Config.getBool("server.http.sendFile", true),
      sendBuffer = Config.getBool("server.http.sendBuffer", true),
      gzip = Config.getBool("server.http.gzip", false),
      maxContentLength = Config.getBytes("server.http.maxContentLength", 1024 * 1024),
      maxInitialLineLength = Config.getBytes("server.http.maxInitialLineLength", 8 * 1024),
      maxChunkSize = Config.getBytes("server.http.maxChunkSize", 128 * 1024),
      maxHeaderSize = Config.getBytes("server.http.maxHeaderSize", 8 * 1024))

    val tcp = TcpConfig(
      sendBufferSize = Config.getBytes("server.tcp.sendBufferSize"),
      receiveBufferSize = Config.getBytes("server.tcp.receiveBufferSize"),
      noDelay = Config.getBool("server.tcp.noDelay", true),
      keepAlive = Config.getBool("server.tcp.keepAlive", true),
      reuseAddr = Config.getBool("server.tcp.reuseAddr", true),
      soLinger = Config.getInt("server.tcp.soLinger", 0))

    val server = ServerConfig(
      cores = Config.getInt("server.cores").getOrElse(Runtime.getRuntime.availableProcessors()),
      statuslog = Config.getDuration("server.statuslog", 10 seconds),
      debugStackTraces = Config.getBool("server.debugStackTraces", true),
      netflow = netflow,
      sflow = sflow,
      cassandra = cassandra,
      ssl = ssl,
      tcp = tcp,
      http = http)
    info("Using %s of %s available cores", server.cores, Runtime.getRuntime.availableProcessors())
    server
  }

  def reload(): Unit = synchronized(config = load())

  def values = config

}

