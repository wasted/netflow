package io.netflow

import io.netflow.netty._
import io.wasted.util._

import io.netty.bootstrap._
import io.netty.channel._
import io.netty.channel.socket.nio._
import io.netty.logging._

import scala.util.{ Try, Success, Failure }

object Server extends App with Logger { PS =>
  override def main(args: Array[String]) { start() }
  private var servers: List[Bootstrap] = List()
  private var listeners: List[java.net.InetSocketAddress] = List()
  private var senders: List[(java.net.InetAddress, Int)] = List()

  def receiveFrom() = senders
  def listeningOn() = listeners

  def start() {
    info("Starting up netflow.io version %s", lib.BuildInfo.version)
    Service.start()

    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    val conf = Config.getStringList("udp.listen", List("0.0.0.0:2055")).map(_.split(":"))
    listeners = conf.map(l => new java.net.InetSocketAddress(l.head, l.last.toInt))

    // Refresh filters from Backend
    Try {
      servers = listeners.map { addr =>
        val srv = new Bootstrap
        val chan = srv.group(new NioEventLoopGroup)
          .localAddress(addr)
          .channel(classOf[NioDatagramChannel])
          .handler(TrafficHandler)
          .option[java.lang.Integer](ChannelOption.SO_RCVBUF, 102400)
        srv.bind().sync
        info("Listening on %s:%s", addr.getAddress.getHostAddress, addr.getPort)
        srv
      }
    } match {
      case Success(v) => Some(v)
      case Failure(f) => f.printStackTrace; error("Unable to bind to that ip:port combination. Check your configuration."); debug(f); stop; return
    }

    if (servers.length == 0) {
      error("Not listening on any ip:port. Set tcp.listen accordingly.")
      stop
      return
    }

    info("Ready")
    // Add Shutdown Hook to cleanly shutdown Netty
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() { PS.stop }
    })
  }

  private def stop() {
    info("Shutting down")

    // Shut down all event loops to terminate all threads.
    Tryo(servers.foreach(_.shutdown))
    servers = List()
    listeners = List()
    senders = List()
    Service.stop()
    info("Shutdown complete")
  }
}

