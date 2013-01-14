package io.netflow.flows.sflow

import io.netflow.flows._
import io.wasted.util.Logger

import io.netty.buffer._
import java.net.InetSocketAddress
import scala.util.Try

/**
 * sFlow Version 5 Packet
 *
 * On sFlows which don't have srcAS and dstAS, we simply set them to 0.
 *
 * Flow coming from a v4 Agent
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-3   | version       | The version of sFlow records exported 005            |
 * *-------*---------------*------------------------------------------------------*
 * | 4-7   | agent version | The InetAddress version of the Agent (1=v4, 2=v6)    |
 * *-------*---------------*------------------------------------------------------*
 * | 8-11  | agent IPv4    | IPv4 Address of the Agent                            |
 * *-------*---------------*------------------------------------------------------*
 * | 12-15 | agent sub id  | Agent Sub ID                                         |
 * *-------*---------------*------------------------------------------------------*
 * | 16-19 | sequence id   | Sequence counter of total flows exported             |
 * *-------*---------------*------------------------------------------------------*
 * | 20-23 | sysuptime     | System Uptime                                        |
 * *-------*---------------*------------------------------------------------------*
 * | 24-27 | samples       | Number of samples                                    |
 * *-------*---------------*------------------------------------------------------*
 *
 * Flow coming from a v6 Agent
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-3   | version       | The version of sFlow records exported 005            |
 * *-------*---------------*------------------------------------------------------*
 * | 4-7   | agent version | The InetAddress version of the Agent (1=v4, 2=v6)    |
 * *-------*---------------*------------------------------------------------------*
 * | 8-23  | agent IPv6    | IPv6 Address of the Agent                            |
 * *-------*---------------*------------------------------------------------------*
 * | 24-27 | agent sub id  | Agent Sub ID                                         |
 * *-------*---------------*------------------------------------------------------*
 * | 28-31 | sequence id   | Sequence counter of total flows exported             |
 * *-------*---------------*------------------------------------------------------*
 * | 32-35 | sysuptime     | System Uptime                                        |
 * *-------*---------------*------------------------------------------------------*
 * | 36-39 | samples       | Number of samples                                    |
 * *-------*---------------*------------------------------------------------------*
 */

object SFlowV5Packet extends Logger {

  /**
   * Parse a Version 5 sFlow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[SFlowV5Packet] = Try[SFlowV5Packet] {
    val version = buf.getInteger(0, 4).toInt
    if (version != 5) throw new InvalidFlowVersionException(sender, version)

    val len = buf.readableBytes
    if (len < 28)
      throw new IncompleteFlowPacketHeaderException(sender)

    val packet = SFlowV5Packet(sender)

    val agentIPversion = if (buf.getInteger(4, 4) == 1L) 4 else 6
    val agentLength = if (agentIPversion == 4) 4 else 16
    val agent = buf.getInetAddress(8, agentLength)

    var offset = 8 + agentLength
    val agentSubId = buf.getInteger(offset, 4)
    val sequenceId = buf.getInteger(offset + 4, 4)
    packet.uptime = buf.getInteger(offset + 8, 4)
    packet.count = buf.getInteger(offset + 12, 4).toInt
    offset = offset + 16

    //packet.flows = Vector.range(0, count) map { i =>
    //  val flow = SFlowV5(5, sender, buf.slice(offset, buf.readableBytes - offset))
    //  offset += flow.length + 8
    //  flow
    //}
    packet
  }
}

case class SFlowV5Packet(sender: InetSocketAddress) extends FlowPacket {
  def version = "sFlowV5 Packet"
}

/**
 * sFlow Version 5
 *
 * *-------*-----------*----------------------------------------------------------*
 * | Bytes | Contents  | Description                                              |
 * *-------*-----------*----------------------------------------------------------*
 * *-------*-----------*----------------------------------------------------------*
 */

object SFlowV5 {

  /**
   * Parse a Version 5 sFlow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  //  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf): IFFlowData = {
  //  // Since sFlows have dynamic length, we need to keep count
  //  var recordLength = 0
  //}
}

case class SFlowV5(sender: InetSocketAddress, length: Int) extends Flow[SFlowV5] {
  def version = "sFlowV5 Flow"

  // TODO implement JSON serialization
  lazy val json = """
  {
    "flowVersion": "%s",
    "flowSender": "%s/%s"
  }""".trim().format(version, senderIP, senderPort)
}