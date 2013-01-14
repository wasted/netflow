package io.netflow.flows.cflow

import io.netflow.flows._

import io.netty.buffer._
import java.net.InetSocketAddress
import scala.util.Try

/**
 * NetFlow Version 1
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

object NetFlowV1Packet {
  private val headerSize = 16
  private val flowSize = 48

  /**
   * Parse a Version 1 FlowPacket
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[NetFlowV1Packet] = Try[NetFlowV1Packet] {
    val version = buf.getInteger(0, 2).toInt
    if (version != 1) throw new InvalidFlowVersionException(sender, version)

    val packet = NetFlowV1Packet(sender)
    packet.count = buf.getInteger(2, 2).toInt
    if (packet.count <= 0 || buf.readableBytes < headerSize + packet.count * flowSize)
      throw new CorruptFlowPacketException(sender)

    packet.uptime = buf.getInteger(4, 4) / 1000
    packet.date = new org.joda.time.DateTime(buf.getInteger(8, 4) * 1000)

    packet.flows = Vector.range(0, packet.count) flatMap { i =>
      NetFlowV1(sender, buf.slice(headerSize + (i * flowSize), flowSize), packet.uptime).toOption
    }

    packet
  }
}

case class NetFlowV1Packet(sender: InetSocketAddress) extends FlowPacket {
  def version = "NetFlowV1 Packet"
}

/**
 * NetFlow Version 1 Flow
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
 * | 36-37 | pad1      | Unused (zero) bytes                                      |
 * *-------*-----------*----------------------------------------------------------*
 * | 38    | prot      | IP protocol type (for example, TCP = 6; UDP = 17)        |
 * *-------*-----------*----------------------------------------------------------*
 * | 39    | tos       | IP type of service (ToS)                                 |
 * *-------*-----------*----------------------------------------------------------*
 * | 40    | tcpflags  | Cumulative OR of TCP flags                               |
 * *-------*-----------*----------------------------------------------------------*
 * | 41-47 | pad2      | Unused (zero) bytes                                      |
 * *-------*-----------*----------------------------------------------------------*
 */

object NetFlowV1 {

  /**
   * Parse a Version 1 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf Slice containing the UDP Packet
   * @param uptime Seconds since UNIX Epoch when the exporting device/sender booted
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, uptime: Long): Try[NetFlowV1] = Try[NetFlowV1] {
    val flow = new NetFlowV1(sender, buf.readableBytes())
    flow.srcAddress = buf.getInetAddress(0, 4)
    flow.dstAddress = buf.getInetAddress(4, 4)
    flow.nextHop = buf.getInetAddress(8, 4)
    flow.snmpInput = buf.getInteger(12, 2).toInt
    flow.snmpOutput = buf.getInteger(14, 2).toInt
    flow.pkts = buf.getInteger(16, 4).toInt
    flow.bytes = buf.getInteger(20, 4).toInt
    flow.start = uptime + buf.getInteger(24, 4).toInt
    flow.stop = uptime + buf.getInteger(28, 4).toInt
    flow.srcPort = buf.getInteger(32, 2).toInt
    flow.dstPort = buf.getInteger(34, 2).toInt
    flow.proto = buf.getUnsignedByte(38).toInt
    flow.tos = buf.getUnsignedByte(39).toInt
    flow.tcpflags = buf.getUnsignedByte(40).toInt
    flow
  }
}

case class NetFlowV1(sender: InetSocketAddress, length: Int) extends NetFlowData[NetFlowV1] {
  def version = "NetFlowV1 Flow"
  var snmpInput: Int = -1
  var snmpOutput: Int = -1

  override lazy val jsonExtra =
    """,
      "snmpInput": %s,
      "snmpOutput": %s
    """.format(snmpInput, snmpOutput)
}
