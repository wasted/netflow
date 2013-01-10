package io.netflow.flows.sflow

import io.netflow.flows._
import io.wasted.util._

import io.netty.buffer._
import org.joda.time.DateTime
import java.net.{ InetAddress, InetSocketAddress }

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
 * | 16-19 | sequence id   | Sequence ID                                          |
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
 * | 28-31 | sequence id   | Sequence ID                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 32-35 | sysuptime     | System Uptime                                        |
 * *-------*---------------*------------------------------------------------------*
 * | 36-39 | samples       | Number of samples                                    |
 * *-------*---------------*------------------------------------------------------*
 */
private[netflow] object SFlowV5Packet {

  /**
   * Parse a Version 5 sFlow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): SFlowV5Packet = {
    val version = buf.getInteger(0, 4).get.toInt
    if (version != 5) throw new InvalidFlowVersionException(sender, version)

    val len = buf.readableBytes
    if (len < 28)
      throw new IncompleteFlowPacketHeaderException(sender)

    val agentIPversion = if (buf.getInteger(4, 4).get.toInt == 1) 4 else 6
    val agentLength = if (agentIPversion == 4) 4 else 16
    val agent = buf.getInetAddress(8, agentLength)

    var offset = 8 + agentLength
    val agentSubId = buf.getInteger(offset, 4).get
    val sequenceId = buf.getInteger(offset + 4, 4).get
    val uptime = buf.getInteger(offset + 8, 4).get
    val count = buf.getInteger(offset + 12, 4).get.toInt
    offset = offset + 16

    val flows = Vector(0 to count - 1: _*) map { i =>
      val flow = SFlowV5(5, sender, buf.slice(offset, buf.readableBytes - offset))
      offset += flow.length + 8
      flow
    }

    SFlowV5Packet(version, sender, count, uptime, new DateTime, flows)
  }
}

private[netflow] case class SFlowV5Packet(
  versionNumber: Int,
  sender: InetSocketAddress,
  count: Int,
  uptime: Long,
  date: DateTime,
  flows: Vector[Flow]) extends FlowPacket {
  lazy val version = "sflow:" + versionNumber + ":packet"
}

/**
 * sFlow Version 5
 *
 * *-------*-----------*----------------------------------------------------------*
 * | Bytes | Contents  | Description                                              |
 * *-------*-----------*----------------------------------------------------------*
 * | 0-3   | srcaddr   | Source IP address                                        |
 * *-------*-----------*----------------------------------------------------------*
 * | 4-7   | dstaddr   | Destination IP address                                   |
 * *-------*-----------*----------------------------------------------------------*
 * | 8-11  | nexthop   | IP address of next hop senderIP                            |
 * *-------*-----------*----------------------------------------------------------*
 * | 12-13 | input     | Interface index (ifindex) of input interface             |
 * *-------*-----------*----------------------------------------------------------*
 * | 14-15 | output    | Interface index (ifindex) of output interface            |
 * *-------*-----------*----------------------------------------------------------*
 * | 16-19 | dPkts     | Packets in the flow                                      |
 * *-------*-----------*----------------------------------------------------------*
 * | 20-23 | dOctets   | Total number of Layer 3 bytes in the packets of the flow |
 * *-------*-----------*----------------------------------------------------------*
 * | 24-27 | First     | SysUptime at start of flow                               |
 * *-------*-----------*----------------------------------------------------------*
 * | 28-31 | Last      | SysUptime at the time the last packet of the flow was    |
 * |       |           | received                                                 |
 * *-------*-----------*----------------------------------------------------------*
 * | 32-33 | srcport   | TCP/UDP source port number or equivalent                 |
 * *-------*-----------*----------------------------------------------------------*
 * | 34-35 | dstport   | TCP/UDP destination port number or equivalent            |
 * *-------*-----------*----------------------------------------------------------*
 * | 36    | pad1      | Unused (zero) bytes                                      |
 * *-------*-----------*----------------------------------------------------------*
 * | 37    | tcp_flags | Cumulative OR of TCP flags                               |
 * *-------*-----------*----------------------------------------------------------*
 * | 38    | prot      | IP protocol type (for example, TCP = 6; UDP = 17)        |
 * *-------*-----------*----------------------------------------------------------*
 * | 39    | tos       | IP type of service (ToS)                                 |
 * *-------*-----------*----------------------------------------------------------*
 * | 40    | flags     | Cumulative OR of TCP flags                               |
 * *-------*-----------*----------------------------------------------------------*
 * | 41-47 | pad2      | Unused (zero) bytes                                      |
 * *-------*-----------*----------------------------------------------------------*
 */

private[netflow] object SFlowV5 {

  /**
   * Parse a Version 5 sFlow
   *
   * @param version NetFlow Version
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf): IFFlowData = {
    // Since sFlows have dynamic length, we need to keep count
    var recordLength = 0

    IFFlowData(
      "netflow:" + version + ":flow",
      sender,
      recordLength)
  }
}

