package io.netflow

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import io.netflow.actors.{ FlowManager, SenderManager }
import io.netflow.lib._
import io.netflow.netty._
import io.netty.bootstrap._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio._
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
    CassandraConnection.start()
    SenderManager
    FlowManager

    def startListeningFor(what: String, listeners: Seq[InetSocketAddress], handler: Option[ChannelHandler]): Boolean = {
      Try {
        what match {
          case "HTTP" =>
            val chanTcp = new ServerBootstrap().group(eventLoop, eventLoop2)
              .channel(classOf[NioServerSocketChannel])
              .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, NodeConfig.values.tcp.noDelay)
              .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, NodeConfig.values.tcp.keepAlive)
              .childOption[java.lang.Boolean](ChannelOption.SO_REUSEADDR, NodeConfig.values.tcp.reuseAddr)
              .childOption[java.lang.Integer](ChannelOption.SO_LINGER, NodeConfig.values.tcp.soLinger)
              .childHandler(new ChannelInitializer[SocketChannel] {
                override def initChannel(ch: SocketChannel) {
                  ch.pipeline.addLast("nego", new ProtoNegoHandler)
                }
              })
            chanTcp.childOption[java.lang.Integer](ChannelOption.SO_SNDBUF, NodeConfig.values.tcp.sendBufferSize)
            chanTcp.childOption[java.lang.Integer](ChannelOption.SO_RCVBUF, NodeConfig.values.tcp.receiveBufferSize)

            listeners.foreach { addr =>
              Try(chanTcp.bind(addr).syncUninterruptibly()) match {
                case Success(socket) =>
                  info("Listening for HTTP on %s:%s", addr.getAddress.getHostAddress, addr.getPort)
                case Failure(e) =>
                  info("Unable to bind to %s:%s. Check your configuration.", addr.getAddress.getHostAddress, addr.getPort)
                  if (NodeConfig.values.debugStackTraces) e.printStackTrace()
              }
            }

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

    SenderManager.stop()
    FlowManager.stop()
    CassandraConnection.shutdown()

    info("Shutdown complete")
  }
}

