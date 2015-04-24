package io.netflow.netty

import java.net.InetSocketAddress

import io.netflow.actors._
import io.netflow.lib._
import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.socket.DatagramPacket
import io.wasted.util._

abstract class TrafficHandler extends SimpleChannelInboundHandler[DatagramPacket] with Logger {

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
    e.printStackTrace()
  }

  protected def handOff(actor: Wactor.Address, sender: InetSocketAddress, buf: ByteBuf): Unit

  override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
    val sender = msg.sender

    // The first two bytes contain the NetFlow version and first four bytes the sFlow version
    if (msg.content().readableBytes() < 4) {
      warn("Unsupported UDP Packet received from " + sender.getAddress.getHostAddress + ":" + sender.getPort)
      return
    }

    // Retain the payload
    msg.content().retain()

    // Try to get an actor
    val actor = SenderManager.findActorFor(sender.getAddress)
    actor onSuccess {
      case actor: Wactor.Address => handOff(actor, sender, msg.content())
    }
    actor onFailure {
      case e: Throwable =>
        warn("Unauthorized Flow received from " + sender.getAddress.getHostAddress + ":" + sender.getPort)
        if (NodeConfig.values.storage.isDefined) FlowManager.bad(sender)

    }
  }
}

@ChannelHandler.Sharable
object NetFlowHandler extends TrafficHandler {
  def handOff(actor: Wactor.Address, sender: InetSocketAddress, buf: ByteBuf): Unit = {
    actor ! NetFlow(sender, buf)
  }
}

@ChannelHandler.Sharable
object SFlowHandler extends TrafficHandler {
  def handOff(actor: Wactor.Address, sender: InetSocketAddress, buf: ByteBuf): Unit = {
    actor ! SFlow(sender, buf)
  }
}
