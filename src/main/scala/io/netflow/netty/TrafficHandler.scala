package io.netflow.netty

import io.netflow._
import io.netflow.flows._
import io.wasted.util._

import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.socket.DatagramPacket

import scala.util.{ Try, Success, Failure }
import java.net.InetSocketAddress

abstract class TrafficHandler extends ChannelInboundMessageHandlerAdapter[DatagramPacket] with Logger {

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.printStackTrace()
  }

  override def messageReceived(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val sender = msg.sender

    // The first two bytes contain the NetFlow version and first four bytes the sFlow version
    if (msg.content().readableBytes() < 4)
      return warn("Unsupported UDP Packet received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)

    Service.findActorFor(sender) match {
      case Some(actor) => send(actor, sender, msg.content())
      case None =>
        warn("Unauthorized NetFlow received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)
    }
  }

  protected val unhandledException = Failure(new UnhandledFlowPacketException)

  def send(actor: Wactor.Address, sender: InetSocketAddress, buf: ByteBuf): Unit
}

@ChannelHandler.Sharable
object NetFlowHandler extends TrafficHandler {
  def send(actor: Wactor.Address, sender: InetSocketAddress, buf: ByteBuf) {
    val fp: Try[FlowPacket] = Tryo(buf.getUnsignedShort(0)) match {
      case Some(1) => cflow.NetFlowV1Packet(sender, buf)
      case Some(5) => cflow.NetFlowV5Packet(sender, buf)
      case Some(6) => cflow.NetFlowV6Packet(sender, buf)
      case Some(7) => cflow.NetFlowV7Packet(sender, buf)
      case Some(9) => cflow.NetFlowV9Packet(sender, buf, actor)
      case Some(10) => cflow.NetFlowV10Packet(sender, buf, actor)
      case _ => unhandledException
    }
    actor ! fp
  }
}

@ChannelHandler.Sharable
object SFlowHandler extends TrafficHandler {
  def send(actor: Wactor.Address, sender: InetSocketAddress, buf: ByteBuf) {
    if (buf.readableBytes < 28) actor ! unhandledException
    val fp: Try[FlowPacket] = Tryo(buf.getLong(0)) match {
      case Some(3) => unhandledException // sFlow 3
      case Some(4) => unhandledException // sFlow 4
      case Some(5) => sflow.SFlowV5Packet(sender, buf)
      case _ => unhandledException
    }
    actor ! fp
  }
}
