package io.netflow.netty

import io.netflow._
import io.wasted.util._

import io.netty.channel._
import io.netty.channel.socket.DatagramPacket

import java.net.InetSocketAddress

@ChannelHandler.Sharable
private[netflow] object TrafficHandler extends ChannelInboundMessageHandlerAdapter[DatagramPacket] with Logger {

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.printStackTrace()
  }

  def unsupportedPacket(sender: InetSocketAddress): Unit = {
    warn("Unsupported UDP Packet received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)
  }

  override def messageReceived(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val sender = msg.remoteAddress

    // The first two bytes contain the NetFlow version
    if (msg.data.readableBytes < 2) return unsupportedPacket(sender)
    Service.findActorFor(sender) match {
      case Some(actor) => actor ! msg.data.copy()
      case None =>
        warn("Unauthorized NetFlow received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)
    }
  }
}

