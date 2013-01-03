package io.netflow.flows

import io.wasted.util._

import java.net.{ InetAddress, InetSocketAddress }

private[netflow] class FlowException(msg: String) extends Exception(msg)

trait FlowPacket {
  def version: String
  def sender: InetSocketAddress
  def senderIP = sender.getAddress.getHostAddress
  def senderPort = sender.getPort

  def count: Int
  def uptime: Long
  def unix_secs: Long
  def flows: List[Flow]
  lazy val date = new org.joda.time.DateTime(unix_secs * 1000)
}

trait Flow {
  def version: String
}

trait FlowData extends Flow {
  def version: String
  def sender: InetSocketAddress
  def senderIP = sender.getAddress.getHostAddress
  def senderPort = sender.getPort

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

  override def toString() = "%s from %s/%s %s:%s (%s) -> %s -> %s:%s (%s) Proto %s - ToS %s - %s pkts - %s bytes".format(
    version, senderIP, senderPort,
    srcAddress.getHostAddress, srcPort, srcAS,
    nextHop.getHostAddress,
    dstAddress.getHostAddress, dstPort, dstAS,
    proto, tos, pkts, bytes)

  private def srcAddressIP() = srcAddress.getHostAddress
  private def dstAddressIP() = dstAddress.getHostAddress
  private def nextHopIP() = nextHop.getHostAddress

  lazy val json = s"""
    {
      "flowVersion": "$version",
      "flowSender": "$senderIP/$senderPort",
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

