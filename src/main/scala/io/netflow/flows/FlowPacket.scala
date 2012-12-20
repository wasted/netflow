package io.netflow.flows

import io.wasted.util._

import java.net.{ InetAddress, InetSocketAddress }

private[netflow] class FlowException(msg: String) extends Exception(msg)

trait FlowPacket extends Logger {
  def count: Int
  def uptime: Long
  def unix_secs: Long
  def sender: InetSocketAddress
  def senderIP: String
  def senderPort: Int
  def flows: List[Flow]
  lazy val date = new org.joda.time.DateTime(unix_secs * 1000)
}

trait Flow extends Logger

trait FlowData extends Flow {
  def srcPort: Int
  def dstPort: Int
  def srcAddress: InetAddress
  def dstAddress: InetAddress
  def srcAS: Int
  def dstAS: Int
  def nextHop: InetAddress
  def proto: Int
  def tos: Int
  def pkts: Long
  def bytes: Long

  override def toString() = "%s:%s (%s) -> %s -> %s:%s (%s) Proto %s - ToS %s - %s pkts - %s bytes".format(
    srcAddress.getHostAddress, srcPort, srcAS,
    nextHop.getHostAddress,
    dstAddress.getHostAddress, dstPort, dstAS,
    proto, tos, pkts, bytes)

  private def srcAddressIP() = srcAddress.getHostAddress
  private def dstAddressIP() = dstAddress.getHostAddress
  private def nextHopIP() = nextHop.getHostAddress

  lazy val json = s"""
    {
      "srcPort": $srcPort,
      "dstPort": $dstPort,
      "srcAddress": "$srcAddressIP",
      "dstAddress": "$dstAddressIP",
      "srcAS": $srcAS,
      "dstAS": $dstAS,
      "nextHop": "$nextHopIP",
      "proto": $proto,
      "tos": $tos,
      "pkts": $pkts,
      "bytes": $bytes
    }"""
}

