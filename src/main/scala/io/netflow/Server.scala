package io.netflow

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import io.netflow.actors.{ FlowManager, SenderManager }
import io.netflow.lib._
import io.netflow.netty._
import io.netty.bootstrap._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket.nio._
import io.wasted.util._

import scala.util.{ Failure, Success, Try }

private[netflow] object Server extends Logger { PS =>
  private val _eventLoop = new AtomicReference[NioEventLoopGroup](null)
  private def eventLoop = _eventLoop.get()

  def start(): Unit = synchronized {
    if (eventLoop != null) return
    info("Starting up netflow.io version %s", io.netflow.lib.BuildInfo.version)
    _eventLoop.set(new NioEventLoopGroup)
    CassandraConnection.start()
    SenderManager
    FlowManager

    def startListeningFor(what: String, listeners: Seq[InetSocketAddress], handler: ChannelHandler): Boolean = {
      Try {
        listeners.foreach { addr =>
          val srv = new Bootstrap
          srv.group(eventLoop)
            .localAddress(addr)
            .channel(classOf[NioDatagramChannel])
            .handler(handler)
            .option[java.lang.Integer](ChannelOption.SO_RCVBUF, 1500)
          srv.bind().sync
          info("Listening for %s on %s:%s", what, addr.getAddress.getHostAddress, addr.getPort)
        }
      } match {
        case Success(v) => true
        case Failure(f) =>
          error("Unable to bind for %s to that ip:port combination. Check your configuration.".format(what))
          debug(f)
          false
      }
    }

    if (!startListeningFor("NetFlow", NodeConfig.values.netflow.listen, NetFlowHandler)) return Runtime.getRuntime.halt(0)
    if (!startListeningFor("sFlow", NodeConfig.values.sflow.listen, SFlowHandler)) return Runtime.getRuntime.halt(0)

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

    SenderManager.stop()
    FlowManager.stop()
    CassandraConnection.shutdown()

    info("Shutdown complete")
  }
}

