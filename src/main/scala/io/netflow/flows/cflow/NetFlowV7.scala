package io.netflow.flows.cflow

import io.netflow.flows._

import io.netty.buffer._
import java.net.InetSocketAddress
import scala.util.{ Try, Failure }

/**
 * NetFlow Version 7
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
 * | 16-19 | flow_sequence | Sequence counter of total flows seen                 |
 * *-------*---------------*------------------------------------------------------*
 * | 20-23 | reserved      | Unused (zero) bytes                                  |
 * *-------*---------------*------------------------------------------------------*
 */

object NetFlowV7Packet {
  private val headerSize = 24
  private val flowSize = 52

  /**
   * Parse a Version 7 FlowPacket
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[NetFlowV7Packet] = Try[NetFlowV7Packet] {
    val version = buf.getInteger(0, 2).toInt
    if (version != 7) return Failure(new InvalidFlowVersionException(version))

    val packet = NetFlowV7Packet(sender, buf.readableBytes)
    packet.count = buf.getInteger(2, 2).toInt
    if (packet.count <= 0 || buf.readableBytes < headerSize + packet.count * flowSize)
      return Failure(new CorruptFlowPacketException)

    packet.uptime = buf.getInteger(4, 4) / 1000
    packet.date = new org.joda.time.DateTime(buf.getInteger(8, 4) * 1000)
    packet.flowSequence = buf.getInteger(16, 4)

    // we use a mutable array here in order not to bash the garbage collector so badly
    // because whenever we append something to our vector, the old vectors need to get GC'd
    val flows = scala.collection.mutable.ArrayBuffer[Flow[_]]()
    for (i <- 0 to packet.count) NetFlowV7(sender, buf.slice(headerSize + (i * flowSize), flowSize), packet.uptime).foreach(flows += _)
    packet.flows = flows.toVector
    packet
  }
}

case class NetFlowV7Packet(sender: InetSocketAddress, length: Int) extends FlowPacket {
  def version = "NetFlowV7 Packet"
  var flowSequence: Long = -1L
}

/**
 * NetFlow Version 7 Flow
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
 * | 37    | tcpflags  | Cumulative OR of TCP flags                               |
 * *-------*-----------*----------------------------------------------------------*
 * | 38    | prot      | IP protocol type (for example, TCP = 6; UDP = 17)        |
 * *-------*-----------*----------------------------------------------------------*
 * | 39    | tos       | IP type of service (ToS)                                 |
 * *-------*-----------*----------------------------------------------------------*
 * | 40-41 | src_as    | AS number of the source, either origin or peer           |
 * *-------*-----------*----------------------------------------------------------*
 * | 42-43 | dst_as    | AS number of the destination, either origin or peer      |
 * *-------*-----------*----------------------------------------------------------*
 * | 44    | src_mask  | Source address prefix mask bits                          |
 * *-------*-----------*----------------------------------------------------------*
 * | 45    | dst_mask  | Destination address prefix mask bits                     |
 * *-------*-----------*----------------------------------------------------------*
 * | 46-47 | flags     | Flags indicating various things (validity)               |
 * *-------*-----------*----------------------------------------------------------*
 * | 48-51 | router_sc | IP address of the router that is bypassed by the         |
 * |       |           | Catalyst 5000 series switch. This is the same address    |
 * |       |           | the router uses when usesit sends NetFlow export packets.|
 * |       |           | This IP address is propagated to all switches bypassing  |
 * |       |           | the router through the FCP protocol.                     |
 * *-------*-----------*----------------------------------------------------------*
 */

object NetFlowV7 {

  /**
   * Parse a Version 7 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf Slice containing the UDP Packet
   * @param uptime Seconds since UNIX Epoch when the exporting device/sender booted
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, uptime: Long): Try[NetFlowV7] = Try[NetFlowV7] {
    val flow = new NetFlowV7(sender, buf.readableBytes())
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
    flow.tcpflags = buf.getUnsignedByte(37).toInt
    flow.proto = buf.getUnsignedByte(38).toInt
    flow.tos = buf.getUnsignedByte(39).toInt
    flow.srcAS = buf.getInteger(40, 2).toInt
    flow.dstAS = buf.getInteger(42, 2).toInt
    flow.srcMask = buf.getUnsignedByte(44).toInt
    flow.dstMask = buf.getUnsignedByte(45).toInt
    flow.flags = buf.getInteger(46, 2).toInt
    flow.routerAddress = buf.getInetAddress(48, 4)
    flow
  }
}

case class NetFlowV7(sender: InetSocketAddress, length: Int) extends NetFlowData[NetFlowV7] {
  def version = "NetFlowV7"
  var snmpInput: Int = -1
  var snmpOutput: Int = -1
  var srcMask: Int = -1
  var dstMask: Int = -1
  var flags: Int = -1
  var routerAddress = defaultAddr

  override lazy val jsonExtra =
    """,
      "snmpInput": %s,
      "snmpOutput": %s,
      "srcMask": %s,
      "dstMask": %s,
      "flags": %s
    """.format(snmpInput, snmpOutput, srcMask, dstMask, flags)
}
