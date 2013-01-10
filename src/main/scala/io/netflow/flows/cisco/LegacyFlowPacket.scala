package io.netflow.flows.cisco

import io.netflow.flows._
import io.wasted.util._

import io.netty.buffer._
import org.joda.time.DateTime
import java.net.{ InetAddress, InetSocketAddress }

/**
 * NetFlow Version 1, 5, 6 or 7 Packet
 *
 * The common thing between all those NetFlow-Versions is the beginning of their
 * header and most of their data. Another common thing is that they all only can
 * work with IPv4.
 *
 * On NetFlows which don't have srcAS and dstAS, we simply set them to 0.
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-1   | version       | The version of NetFlow records exported 005          |
 * *-------*---------------*------------------------------------------------------*
 * | 2-3   | count         | Number of flows exported in this packet (1-30)       |
 * *-------*---------------*------------------------------------------------------*
 * | 4-7   | SysUptime     | Current time in milli since the export device booted |
 * *-------*---------------*------------------------------------------------------*
 * | 8-11  | unix_secs     | Current count of seconds since 0000 UTC 1970         |
 * *-------*---------------*------------------------------------------------------*
 * | 12-15 | unix_nsecs    | Residual nanoseconds since 0000 UTC 1970             |
 * *-------*---------------*------------------------------------------------------*
 */
private[netflow] object LegacyFlowPacket {
  val versionMap = Map(1 -> (16, 48), 5 -> (24, 48), 6 -> (24, 52), 7 -> (24, 52))

  /**
   * Parse a Version 1, 5, 6 or 7 Flow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): LegacyFlowPacket = {
    val version = buf.getInteger(0, 2).get.toInt
    if (!versionMap.contains(version)) throw new InvalidFlowVersionException(sender, version)

    val (headerSize, flowSize) = versionMap(version)

    val count = buf.getInteger(2, 2).get.toInt
    if (count <= 0 || buf.readableBytes < headerSize + count * flowSize)
      throw new CorruptFlowPacketException(sender)

    val uptime = buf.getInteger(4, 4).get
    val unix_secs = buf.getInteger(8, 4).get

    val flows = Vector.range(0, count) map { i =>
      LegacyFlow(version, sender, buf.slice(headerSize + (i * flowSize), flowSize))
    }

    val date = new org.joda.time.DateTime(unix_secs * 1000)
    LegacyFlowPacket(version, sender, count, uptime, date, flows)
  }
}

private[netflow] case class LegacyFlowPacket(
  versionNumber: Int,
  sender: InetSocketAddress,
  count: Int,
  uptime: Long,
  date: DateTime,
  flows: Vector[Flow]) extends FlowPacket {
  lazy val version = "netflow:" + versionNumber + ":packet"
}

/**
 * NetFlow Version 1, 5, 6 or 7 Flow
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

private[netflow] object LegacyFlow {

  /**
   * Parse a Version 1, 5, 6 or 7 Flow
   *
   * @param version NetFlow Version
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf Slice containing the UDP Packet
   */
  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf): IPFlowData = {
    val srcPort = buf.getInteger(32, 2).get.toInt
    val dstPort = buf.getInteger(34, 2).get.toInt

    val srcAS = version match { case 5 | 6 | 7 => buf.getInteger(40, 2).get.toInt case _ => 0 }
    val dstAS = version match { case 5 | 6 | 7 => buf.getInteger(42, 2).get.toInt case _ => 0 }

    val srcAddress = buf.getInetAddress(0, 4)
    val dstAddress = buf.getInetAddress(4, 4)

    val nextHop = buf.getInetAddress(8, 4)

    val pkts = buf.getInteger(16, 4).get
    val bytes = buf.getInteger(20, 4).get

    val proto = buf.getUnsignedByte(38).toInt
    val tos = buf.getUnsignedByte(39).toInt

    IPFlowData(
      "netflow:" + version + ":flow",
      sender,
      srcPort, dstPort,
      srcAS, dstAS,
      srcAddress, dstAddress, nextHop,
      pkts, bytes, proto, tos,
      buf.readableBytes)
  }
}

