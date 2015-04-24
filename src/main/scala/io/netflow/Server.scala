package io.netflow

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import com.twitter.conversions.storage._
import io.netflow.lib._
import io.netflow.netty._
import io.netty.bootstrap._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket.nio._
import io.netty.handler.codec.http.FullHttpRequest
import io.wasted.util._

import scala.util.{ Failure, Success, Try }

private[netflow] object Server extends Logger { PS =>
  private val _eventLoop = new AtomicReference[NioEventLoopGroup](null)
  private def eventLoop = _eventLoop.get()

  private val _eventLoop2 = new AtomicReference[NioEventLoopGroup](null)
  private def eventLoop2 = _eventLoop2.get()

  def start(): Unit = synchronized {
    if (eventLoop != null) return
    info("Starting up netflow.io version %s", io.netflow.lib.BuildInfo.version)
    _eventLoop.set(new NioEventLoopGroup)
    _eventLoop2.set(new NioEventLoopGroup)
    storage.Connection.start()

    def startListeningFor(what: String, listeners: Seq[InetSocketAddress], handler: Option[ChannelHandler]): Boolean = {
      Try {
        what match {
          case "HTTP" =>
            val codec = http.HttpCodec[FullHttpRequest]()
              .withMaxHeaderSize(NodeConfig.values.http.maxHeaderSize.bytes)
              .withMaxInitialLineLength(NodeConfig.values.http.maxInitialLineLength.bytes)
              .withMaxResponseSize(NodeConfig.values.http.maxContentLength.bytes)
              .withMaxRequestSize(NodeConfig.values.http.maxChunkSize.bytes)
            val server = http.HttpServer()
              .withTcpNoDelay(NodeConfig.values.tcp.noDelay)
              .withTcpKeepAlive(NodeConfig.values.tcp.keepAlive)
              .withReuseAddr(NodeConfig.values.tcp.reuseAddr)
              .withSoLinger(NodeConfig.values.tcp.soLinger)
              .withSpecifics(codec).handler(HttpAuthHandler.apply)
            listeners.foreach(server.bind)

          case _ if handler.isDefined =>
            listeners.foreach { addr =>
              val srv = new Bootstrap
              srv.group(eventLoop)
                .localAddress(addr)
                .channel(classOf[NioDatagramChannel])
                .handler(handler.get)
                .option[java.lang.Integer](ChannelOption.SO_RCVBUF, 1500)
              srv.bind().sync
              info("Listening for %s on %s:%s", what, addr.getAddress.getHostAddress, addr.getPort)
            }

          case _ =>
            warn("No Handler was associated with " + what)
        }
      } match {
        case Success(v) => true
        case Failure(f) =>
          error("Unable to bind for %s to that ip:port combination. Check your configuration.".format(what))
          if (NodeConfig.values.debugStackTraces) f.printStackTrace()
          false
      }
    }

    if (!startListeningFor("HTTP", NodeConfig.values.http.listen, None)) return Runtime.getRuntime.halt(0)
    if (!startListeningFor("NetFlow", NodeConfig.values.netflow.listen, Some(NetFlowHandler))) return Runtime.getRuntime.halt(0)
    if (!startListeningFor("sFlow", NodeConfig.values.sflow.listen, Some(SFlowHandler))) return Runtime.getRuntime.halt(0)

    info("Ready")

    // Add Shutdown Hook to cleanly shutdown Netty
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() { PS.stop() }
    })
  }

  def stop(): Unit = synchronized {
    if (eventLoop == null) return
    info("Shutting down")

    // Shut down all event loops to terminate all threads.
    eventLoop.shutdownGracefully()
    _eventLoop.set(null)
    eventLoop2.shutdownGracefully()
    _eventLoop2.set(null)

    actors.SenderManager.stop()
    actors.FlowManager.stop()
    storage.Connection.start()

    info("Shutdown complete")
  }
}

