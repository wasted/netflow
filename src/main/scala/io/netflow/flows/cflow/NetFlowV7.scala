package io.netflow.flows.cflow

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.twitter.util.Future
import io.netflow.lib._
import io.netflow.storage
import io.netty.buffer._
import net.liftweb.json.JsonDSL._
import org.joda.time.DateTime

import scala.util.{ Failure, Try }

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
    val version = buf.getUnsignedInteger(0, 2).toInt
    if (version != 7) return Failure(new InvalidFlowVersionException(version))

    val count = buf.getUnsignedInteger(2, 2).toInt
    if (count <= 0 || buf.readableBytes < headerSize + count * flowSize)
      return Failure(new CorruptFlowPacketException)

    val uptime = buf.getUnsignedInteger(4, 4)
    val timestamp = new DateTime(buf.getUnsignedInteger(8, 4) * 1000)
    val id = UUIDs.startOf(timestamp.getMillis)
    val flowSequence = buf.getUnsignedInteger(16, 4)

    val flows: List[NetFlowV7] = (0 to count - 1).toList.flatMap { i =>
      apply(sender, buf.slice(headerSize + (i * flowSize), flowSize), id, uptime, timestamp)
    }
    NetFlowV7Packet(id, sender, buf.readableBytes, uptime, timestamp, flows, flowSequence)
  }

  /**
   * Parse a Version 7 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf Slice containing the UDP Packet
   * @param fpId FlowPacket-UUID this Flow arrived on
   * @param uptime Millis since UNIX Epoch when the exporting device/sender booted
   * @param timestamp DateTime when this flow was exported
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, uptime: Long, timestamp: DateTime): Option[NetFlowV7] =
    Try[NetFlowV7] {
      NetFlowV7(UUIDs.timeBased(), sender, buf.readableBytes(), uptime, timestamp,
        buf.getUnsignedInteger(32, 2).toInt, // srcPort
        buf.getUnsignedInteger(34, 2).toInt, // dstPort
        Option(buf.getUnsignedInteger(40, 2).toInt).filter(_ != -1), // srcAS
        Option(buf.getUnsignedInteger(42, 2).toInt).filter(_ != -1), // dstAS
        buf.getUnsignedInteger(16, 4), // pkts
        buf.getUnsignedInteger(20, 4), // bytes
        buf.getUnsignedByte(38).toInt, // proto
        buf.getUnsignedByte(39).toInt, // tos
        buf.getUnsignedByte(37).toInt, // tcpflags
        Some(buf.getUnsignedInteger(24, 4)).filter(_ != 0).map(x => timestamp.minus(uptime - x)), // start
        Some(buf.getUnsignedInteger(28, 4)).filter(_ != 0).map(x => timestamp.minus(uptime - x)), // stop
        buf.getInetAddress(0, 4), // srcAddress
        buf.getInetAddress(4, 4), // dstAddress
        Option(buf.getInetAddress(8, 4)).filter(_.getHostAddress != "0.0.0.0"), // nextHop
        buf.getUnsignedInteger(12, 2).toInt, // snmpInput
        buf.getUnsignedInteger(14, 2).toInt, // snmpOutput
        buf.getUnsignedByte(44).toInt, // srcMask
        buf.getUnsignedByte(45).toInt, // dstMask
        buf.getUnsignedInteger(46, 2).toInt, // flags
        buf.getInetAddress(48, 4), // routerAddress
        fpId)
    }.toOption

  private def doLayer[T](f: FlowPacketMeta[NetFlowV7Packet] => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Cassandra) => f(storage.cassandra.NetFlowV7Packet)
    case Some(StorageLayer.Redis) => f(storage.redis.NetFlowV7Packet)
    case _ => Future.exception(NoBackendDefined)
  }

  def persist(fp: NetFlowV7Packet): Unit = doLayer(l => Future.value(l.persist(fp)))
}

case class NetFlowV7Packet(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: DateTime,
                           flows: List[NetFlowV7], flowSequence: Long) extends FlowPacket {
  def version = "NetFlowV7 Packet"
  def count = flows.length

  def persist() = NetFlowV7Packet.persist(this)
}

case class NetFlowV7(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: DateTime,
                     srcPort: Int, dstPort: Int, srcAS: Option[Int], dstAS: Option[Int],
                     pkts: Long, bytes: Long, proto: Int, tos: Int, tcpflags: Int,
                     start: Option[DateTime], stop: Option[DateTime],
                     srcAddress: InetAddress, dstAddress: InetAddress, nextHop: Option[InetAddress],
                     snmpInput: Int, snmpOutput: Int, srcMask: Int, dstMask: Int, flags: Int,
                     routerAddress: InetAddress, packet: UUID)
  extends NetFlowData[NetFlowV7] {
  def version = "NetFlowV7"

  override lazy val jsonExtra = ("srcMask" -> srcMask) ~ ("dstMask" -> dstMask) ~
    ("snmp" -> ("input" -> snmpInput) ~ ("output" -> snmpOutput)) ~ ("flags" -> flags)
  ("routerAddress" -> routerAddress.getHostAddress) ~ ("packet" -> packet.toString)
}
