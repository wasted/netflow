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
 * NetFlow Version 6
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
 * | 20    | engine_type   | Type of flow-switching engine                        |
 * *-------*---------------*------------------------------------------------------*
 * | 21    | engine_id     | Slot number of the flow-switching engine             |
 * *-------*---------------*------------------------------------------------------*
 * | 22-23 | sampling_int  | First two bits hold the sampling mode                |
 * |       |               | remaining 14 bits hold value of sampling interval    |
 * *-------*---------------*------------------------------------------------------*
 */

object NetFlowV6Packet {
  private val headerSize = 24
  private val flowSize = 52

  /**
   * Parse a Version 6 FlowPacket
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[NetFlowV6Packet] = Try[NetFlowV6Packet] {
    val version = buf.getUnsignedInteger(0, 2).toInt
    if (version != 6) return Failure(new InvalidFlowVersionException(version))

    val count = buf.getUnsignedInteger(2, 2).toInt
    if (count <= 0 || buf.readableBytes < headerSize + count * flowSize)
      return Failure(new CorruptFlowPacketException)

    val uptime = buf.getUnsignedInteger(4, 4)
    val timestamp = new DateTime(buf.getUnsignedInteger(8, 4) * 1000)
    val id = UUIDs.startOf(timestamp.getMillis)
    val flowSequence = buf.getUnsignedInteger(16, 4)
    val engineType = buf.getUnsignedInteger(20, 1).toInt
    val engineId = buf.getUnsignedInteger(21, 1).toInt
    // the first 2 bits are the sampling mode, the remaining 14 the interval
    val sampling = buf.getUnsignedInteger(22, 2).toInt
    val samplingInterval = sampling & 0x3FFF
    val samplingMode = sampling >> 14

    val flows: List[NetFlowV6] = (0 to count - 1).toList.flatMap { i =>
      apply(sender, buf.slice(headerSize + (i * flowSize), flowSize), id, uptime, timestamp, samplingInterval)
    }
    NetFlowV6Packet(id, sender, buf.readableBytes, uptime, timestamp, flows, flowSequence, engineType, engineId,
      samplingInterval, samplingMode)
  }

  /**
   * Parse a Version 6 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf Slice containing the UDP Packet
   * @param fpId FlowPacket-UUID this Flow arrived on
   * @param uptime Millis since UNIX Epoch when the exporting device/sender booted
   * @param timestamp DateTime when this flow was exported
   * @param samplingInterval Interval samples are sent
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, uptime: Long, timestamp: DateTime, samplingInterval: Int): Option[NetFlowV6] =
    Try[NetFlowV6] {
      val sampling = NodeConfig.values.netflow.calculateSamples
      val pkts = buf.getUnsignedInteger(16, 4)
      val bytes = buf.getUnsignedInteger(20, 4)
      NetFlowV6(UUIDs.timeBased(), sender, buf.readableBytes(), uptime, timestamp,
        buf.getUnsignedInteger(32, 2).toInt, // srcPort
        buf.getUnsignedInteger(34, 2).toInt, // dstPort
        Option(buf.getUnsignedInteger(40, 2).toInt).filter(_ != -1), // srcAS
        Option(buf.getUnsignedInteger(42, 2).toInt).filter(_ != -1), // dstAS
        if (sampling) pkts * samplingInterval else pkts, // pkts
        if (sampling) bytes * samplingInterval else bytes, // bytes
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
        fpId)
    }.toOption

  private def doLayer[T](f: FlowPacketMeta[NetFlowV6Packet] => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Cassandra) => f(storage.cassandra.NetFlowV6Packet)
    case Some(StorageLayer.Redis) => f(storage.redis.NetFlowV6Packet)
    case _ => Future.exception(NoBackendDefined)
  }

  def persist(fp: NetFlowV6Packet): Unit = doLayer(l => Future.value(l.persist(fp)))
}

case class NetFlowV6Packet(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: DateTime,
                           flows: List[NetFlowV6],
                           flowSequence: Long, engineType: Int, engineId: Int,
                           samplingInterval: Int, samplingMode: Int) extends FlowPacket {
  def version = "NetFlowV6 Packet"
  def count = flows.length

  def persist() = NetFlowV6Packet.persist(this)
}

case class NetFlowV6(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: DateTime,
                     srcPort: Int, dstPort: Int, srcAS: Option[Int], dstAS: Option[Int],
                     pkts: Long, bytes: Long, proto: Int, tos: Int, tcpflags: Int,
                     start: Option[DateTime], stop: Option[DateTime],
                     srcAddress: InetAddress, dstAddress: InetAddress, nextHop: Option[InetAddress],
                     snmpInput: Int, snmpOutput: Int, srcMask: Int, dstMask: Int, packet: UUID)
  extends NetFlowData[NetFlowV6] {
  def version = "NetFlowV6"

  override lazy val jsonExtra = ("srcMask" -> srcMask) ~ ("dstMask" -> dstMask) ~
    ("snmp" -> ("input" -> snmpInput) ~ ("output" -> snmpOutput))
}
