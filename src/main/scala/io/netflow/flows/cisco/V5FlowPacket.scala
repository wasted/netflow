package io.netflow.flows.cisco

import io.netflow.flows._

import io.netty.buffer._

import java.net.InetSocketAddress

/**
 * NetFlow Version 5 Packet
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-1   | version       | The version of NetFlow records exported 009          |
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
 * | 20    | engine_type   | Type of flow-switching engine                        |
 * *-------*---------------*------------------------------------------------------*
 * | 21    | engine_id     | Slot number of the flow-switching engine             |
 * *-------*---------------*------------------------------------------------------*
 * | 22-23 | reserved      | Unused (zero) bytes                                  |
 * *-------*---------------*------------------------------------------------------*
 *
 * @param senderIP senderIP's IP Address
 * @param buf Netty ByteBuf containing the UDP Packet
 */
private[netflow] class V5FlowPacket(val sender: InetSocketAddress, buf: ByteBuf) extends FlowPacket {
  def senderIP() = sender.getAddress.getHostAddress
  def senderPort() = sender.getPort

  private val V5_Header_Size = 24
  private val V5_Flow_Size = 48
  private val len = buf.readableBytes

  val count = buf.getUnsignedShort(2)
  val uptime = buf.getUnsignedInt(4)
  val unix_secs = buf.getUnsignedInt(8)
  val unix_nsecs = buf.getUnsignedInt(12)
  val flowSequence = buf.getUnsignedInt(16)

  if (len < V5_Header_Size)
    throw new IncompleteFlowPacketHeaderException(sender)

  if (count <= 0 || len != V5_Header_Size + count * V5_Flow_Size)
    throw new CorruptFlowPacketException(sender)

  val flows: List[Flow] = {
    val flows = (0 to count - 1).toList map { i =>
      new V5Flow(senderIP, buf.copy(V5_Header_Size + (i * V5_Flow_Size), V5_Flow_Size))
    }
    val flowCount = flows.length
    info("Flows passed " + flowCount + " of " + count)
    flows
  }

  info("NetFlow version 5 received from " + senderIP + "/" + senderPort)
}

/**
 * NetFlow Version 5 Flow
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
 * | 40-41 | src_as    | Autonomous system number of the source, either origin or |
 * |       |           | peer                                                     |
 * *-------*-----------*----------------------------------------------------------*
 * | 42-43 | dst_as    | Autonomous system number of the destination, either      |
 * |       |           | origin or peer                                           |
 * *-------*-----------*----------------------------------------------------------*
 * | 44    | src_mask  | Source address prefix mask bits                          |
 * *-------*-----------*----------------------------------------------------------*
 * | 45    | dst_mask  | Destination address prefix mask bits                     |
 * *-------*-----------*----------------------------------------------------------*
 * | 46-47 | pad2      | Unused (zero) bytes                                      |
 * *-------*-----------*----------------------------------------------------------*
 *
 * @param senderIP senderIP's IP Address
 * @param buf Netty ByteBuf containing the UDP Packet
 */
private[netflow] class V5Flow(val senderIP: String, buf: ByteBuf) extends FlowData {

  val srcPort = buf.getUnsignedShort(32)
  val dstPort = buf.getUnsignedShort(34)
  val srcAS = buf.getUnsignedShort(40)
  val dstAS = buf.getUnsignedShort(42)

  val srcAddress = buf.getInetAddress(0, 4)
  val dstAddress = buf.getInetAddress(4, 4)

  val nextHop = buf.getInetAddress(8, 4)

  val pkts = buf.getUnsignedInt(16)
  val bytes = buf.getUnsignedInt(20)

  val proto = buf.getUnsignedByte(38).toInt
  val tos = buf.getUnsignedByte(39).toInt

  info(toString)
}

