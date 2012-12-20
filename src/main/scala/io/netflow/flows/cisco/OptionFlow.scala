package io.netflow.flows.cisco

import io.netflow.flows._
import io.netty.buffer._
import java.net.{ InetAddress, InetSocketAddress }

private[netflow] class OptionFlow(val sender: InetSocketAddress, buf: ByteBuf, val template: Template) extends Flow {
  var srcPort, dstPort, srcAS, dstAS, proto, tos = 0
  var pkts, bytes = 0L
  def srcAddress = null.asInstanceOf[InetAddress]
  def dstAddress = null.asInstanceOf[InetAddress]
  def nextHop = null.asInstanceOf[InetAddress]

  override def toString() = "%s:%s (%s) -> %s -> %s:%s (%s) Proto %s - ToS %s - %s pkts - %s bytes".format(
    srcAddress.getHostAddress, srcPort, srcAS,
    nextHop.getHostAddress,
    dstAddress.getHostAddress, dstPort, dstAS,
    proto, tos, pkts, bytes)
}

