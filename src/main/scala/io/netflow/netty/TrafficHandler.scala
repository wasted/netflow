package io.netflow.netty

import io.netflow._
import io.netflow.flows._
import io.netflow.flows.cisco._
import io.netflow.backends.StorageConnection
import io.wasted.util._

import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.socket.DatagramPacket
import io.netty.util.CharsetUtil

import org.joda.time.DateTime
import java.net.{ InetAddress, InetSocketAddress }

@ChannelHandler.Sharable
private[netflow] object TrafficServerHandler extends ChannelInboundMessageHandlerAdapter[DatagramPacket] with Logger {

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.printStackTrace
  }

  def unsupportedPacket(sender: InetSocketAddress): Unit = {
    warn("Unsupported UDP Packet received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)
  }

  override def messageReceived(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val sender = msg.remoteAddress
    val buf = msg.data
    implicit val sc = Service.backend.start

    if (!Service.backend.acceptFrom(sender)) {
      warn("Unauthorized NetFlow received from " + msg.remoteAddress.getAddress.getHostAddress + "/" + msg.remoteAddress.getPort)
      return
    }

    // The first two bytes contain the NetFlow version
    if (buf.readableBytes < 2) return unsupportedPacket(sender)

    //Tryo(handleCisco(sender, buf)) getOrElse unsupportedPacket(sender)
    handleCisco(sender, buf)
    Service.backend.stop
  }

  private def handleCisco(sender: InetSocketAddress, buf: ByteBuf)(implicit sc: StorageConnection): Unit =
    Try(buf.getUnsignedShort(0)) match {
      case Failure(e) =>
        debug("%s", e)
        unsupportedPacket(sender)
        Service.backend.countDatagram(new DateTime, sender, true)
        None

      case Success(version) if version == 5 =>
        Tryo(new V5FlowPacket(sender, buf)) map { flowPacket =>
          Service.backend.countDatagram(new DateTime, sender, false)
          Service.backend.save(flowPacket)
        }

      case Success(version) if version == 9 || version == 10 =>
        Tryo(new V9FlowPacket(sender, buf)) map { flowPacket =>
          Service.backend.countDatagram(new DateTime, sender, false)
          Service.backend.save(flowPacket)
        }

      case Success(version) =>
        info("Unsupported NetFlow version " + version + " received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)
        Service.backend.countDatagram(new DateTime, sender, true)
        None
    }
}

