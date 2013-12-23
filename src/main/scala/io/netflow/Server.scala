package io.netflow

import io.netflow.netty._
import io.wasted.util._

import io.netty.bootstrap._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket.nio._

import java.net.InetSocketAddress
import scala.util.{ Try, Success, Failure }

private[netflow] object Server extends Logger { PS =>
  private var eventLoop: Option[NioEventLoopGroup] = None

  def start() {
    if (eventLoop.isDefined) return
    info("Starting up netflow.io version %s", io.netflow.lib.BuildInfo.version)
    Service.start()

    val el = new NioEventLoopGroup
    eventLoop = Some(el)
    def startListeningFor(what: String, listeners: Seq[InetSocketAddress], handler: ChannelHandler): Boolean = {

      // Refresh filters from Backend
      Try {
        listeners.foreach { addr =>
          val srv = new Bootstrap
          srv.group(el)
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

    if (!startListeningFor("NetFlow", NetFlowConfig.values.netflows, NetFlowHandler)) return stop()
    if (!startListeningFor("sFlow", NetFlowConfig.values.sflows, SFlowHandler)) return stop()

    info("Ready")

    // Add Shutdown Hook to cleanly shutdown Netty
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() { PS.stop() }
    })
  }

  def stop() {
    info("Shutting down")

    // Shut down all event loops to terminate all threads.
    eventLoop.map(_.shutdownGracefully())
    eventLoop = None
    Service.stop()
    info("Shutdown complete")
  }
}

