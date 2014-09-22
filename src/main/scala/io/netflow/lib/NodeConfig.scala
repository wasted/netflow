package io.netflow.lib

import java.io.File
import java.net.InetSocketAddress
import java.util.UUID

import io.wasted.util.ssl.{ KeyStoreType, Ssl }
import io.wasted.util.{ Tryo, Config, Logger }

import scala.concurrent.duration._

private[netflow] object NodeConfig extends Logger {

  case class ServerConfig(
    cores: Int,
    statuslog: Duration,
    debugStackTraces: Boolean,
    admin: AdminConfig,
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

  case class AdminConfig(
    authKey: UUID,
    signKey: UUID)

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
    calculateSamples: Boolean,
    extraFields: Boolean)

  private var config: ServerConfig = load()

  private def load(): ServerConfig = {
    val cassandra = CassandraConfig(
      keyspace = Config.getString("cassandra.keyspace", "netflow"),
      hosts = Config.getStringList("cassandra.hosts", List("localhost")),
      minConns = Config.getInt("cassandra.minConns", 5),
      maxConns = Config.getInt("cassandra.maxConns", 40),
      minSimRequests = Config.getInt("cassandra.minSimRequests", 5),
      maxSimRequests = Config.getInt("cassandra.maxSimRequests", 128),
      connectTimeout = Config.getInt("cassandra.connectTimeout", 5000),
      reconnectTimeout = Config.getInt("cassandra.reconnectTimeout", 5000),
      readTimeout = Config.getInt("cassandra.readTimeout", 60000),
      keyspaceConfig = Config.getString("cassandra.keyspaceConfig",
        "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}"))

    val cpus = Runtime.getRuntime.availableProcessors() match {
      case 1 => 1
      case x if x > 8 => x - 2
      case x if x > 4 => x - 1
    }

    val adminAuthKey = Config.getString("admin.authKey").flatMap(ak => Tryo(UUID.fromString(ak))) match {
      case Some(ak) => ak
      case _ =>
        val authKey = UUID.randomUUID()
        error("Invalid or missing UUID at admin.authKey directive. Generated %s", authKey)
        authKey
    }

    val adminSignKey = Config.getString("admin.signKey").flatMap(sk => Tryo(UUID.fromString(sk))) match {
      case Some(sk) => sk
      case _ =>
        val signKey = UUID.randomUUID()
        error("Invalid or missing UUID at admin.signKey directive. Generated %s", signKey)
        signKey
    }

    val admin = AdminConfig(
      authKey = adminAuthKey,
      signKey = adminSignKey)

    val netflow = NetFlowConfig(
      listen = Config.getInetAddrList("netflow.listen", List("0.0.0.0:2055")),
      persist = Config.getBool("netflow.persist", true),
      calculateSamples = Config.getBool("netflow.calculateSamples", true),
      extraFields = Config.getBool("netflow.extraFields", true))

    val sflow = SFlowConfig(
      listen = Config.getInetAddrList("sflow.listen", List("0.0.0.0:6343")),
      persist = Config.getBool("sflow.persist", true))

    val ssl = SslConfig(
      certPath = Config.getString("http.ssl.p12"),
      certPass = Config.getString("http.ssl.pass"))

    val http = HttpConfig(
      listen = Config.getInetAddrList("http.listen", List("0.0.0.0:8080")),
      sendFile = Config.getBool("http.sendFile", true),
      sendBuffer = Config.getBool("http.sendBuffer", true),
      gzip = Config.getBool("http.gzip", false),
      maxContentLength = Config.getBytes("http.maxContentLength", 1024 * 1024),
      maxInitialLineLength = Config.getBytes("http.maxInitialLineLength", 8 * 1024),
      maxChunkSize = Config.getBytes("http.maxChunkSize", 128 * 1024),
      maxHeaderSize = Config.getBytes("http.maxHeaderSize", 8 * 1024))

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
      admin = admin,
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

