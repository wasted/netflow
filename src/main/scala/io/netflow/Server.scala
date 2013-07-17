package io.netflow

import io.netflow.netty._
import io.wasted.util._

import io.netty.bootstrap._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket.nio._

import scala.util.{ Try, Success, Failure }

object Server extends App with Logger { PS =>
  override def main(args: Array[String]) { start() }

  def start() {
    info("Starting up netflow.io version %s", io.netflow.lib.BuildInfo.version)
    Service.start()

    val eventLoop = new NioEventLoopGroup
    def startListeningFor(what: String, config: String, default: List[String], handler: ChannelHandler): Boolean = {
      val listeners = Config.getInetAddrList(config, default)

      // Refresh filters from Backend
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

    if (!startListeningFor("NetFlow", "netflow.listen", List("0.0.0.0:2055"), NetFlowHandler)) return stop(eventLoop)
    if (!startListeningFor("sFlow", "sflow.listen", List("0.0.0.0:6343"), SFlowHandler)) return stop(eventLoop)

    info("Ready")

    // Add Shutdown Hook to cleanly shutdown Netty
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() { PS.stop(eventLoop) }
    })
  }

  private def stop(eventLoop: NioEventLoopGroup) {
    info("Shutting down")

    // Shut down all event loops to terminate all threads.
    eventLoop.shutdownGracefully()
    Service.stop()
    info("Shutdown complete")
  }
}

