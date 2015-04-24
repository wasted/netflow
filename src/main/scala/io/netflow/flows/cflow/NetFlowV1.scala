package io.netflow.flows.cflow

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.twitter.util.Future
import io.netflow.lib._
import io.netflow.storage
import io.netty.buffer._
import net.liftweb.json.JObject
import net.liftweb.json.JsonDSL._
import org.joda.time.DateTime

import scala.util.{ Failure, Try }

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
    val version = buf.getUnsignedInteger(0, 2).toInt
    if (version != 1) return Failure(new InvalidFlowVersionException(version))

    val count = buf.getUnsignedInteger(2, 2).toInt
    if (count <= 0 || buf.readableBytes < headerSize + count * flowSize)
      return Failure(new CorruptFlowPacketException)

    val uptime = buf.getUnsignedInteger(4, 4)
    val timestamp = new DateTime(buf.getUnsignedInteger(8, 4) * 1000)
    val id = UUIDs.startOf(timestamp.getMillis)

    val flows: List[NetFlowV1] = (0 to count - 1).toList.flatMap { i =>
      apply(sender, buf.slice(headerSize + (i * flowSize), flowSize), id, uptime, timestamp)
    }
    NetFlowV1Packet(id, sender, buf.readableBytes, uptime, timestamp, flows)
  }

  /**
   * Parse a Version 1 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf Slice containing the UDP Packet
   * @param fpId FlowPacket-UUID this Flow arrived on
   * @param uptime Millis since UNIX Epoch when the exporting device/sender booted
   * @param timestamp DateTime when this flow was exported
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, uptime: Long, timestamp: DateTime): Option[NetFlowV1] =
    Try[NetFlowV1] {
      NetFlowV1(UUIDs.timeBased(), sender, buf.readableBytes(), uptime, timestamp,
        buf.getUnsignedInteger(32, 2).toInt, // srcPort
        buf.getUnsignedInteger(34, 2).toInt, // dstPort
        None, None, // srcAS and dstAS
        buf.getUnsignedInteger(16, 4), // pkts
        buf.getUnsignedInteger(20, 4), // bytes
        buf.getUnsignedByte(38).toInt, // proto
        buf.getUnsignedByte(39).toInt, // tos
        buf.getUnsignedByte(40).toInt, // tcpflags
        Some(buf.getUnsignedInteger(24, 4)).filter(_ != 0).map(x => timestamp.minus(uptime - x)), // start
        Some(buf.getUnsignedInteger(28, 4)).filter(_ != 0).map(x => timestamp.minus(uptime - x)), // stop
        buf.getInetAddress(0, 4), // srcAddress
        buf.getInetAddress(4, 4), // dstAddress
        Option(buf.getInetAddress(8, 4)).filter(_.getHostAddress != "0.0.0.0"), // nextHop
        buf.getUnsignedInteger(12, 2).toInt, // snmpInput
        buf.getUnsignedInteger(14, 2).toInt, // snmpOutput
        fpId)
    }.toOption

  private def doLayer[T](f: FlowPacketMeta[NetFlowV1Packet] => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Cassandra) => f(storage.cassandra.NetFlowV1Packet)
    case Some(StorageLayer.Redis) => f(storage.redis.NetFlowV1Packet)
    case _ => Future.exception(NoBackendDefined)
  }

  def persist(fp: NetFlowV1Packet): Unit = doLayer(l => Future.value(l.persist(fp)))
}

case class NetFlowV1Packet(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: DateTime,
                           flows: List[NetFlowV1]) extends FlowPacket {
  def version = "NetFlowV1 Packet"
  def count = flows.length
  def persist(): Unit = NetFlowV1Packet.persist(this)
}

case class NetFlowV1(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long, timestamp: DateTime,
                     srcPort: Int, dstPort: Int, srcAS: Option[Int], dstAS: Option[Int],
                     pkts: Long, bytes: Long, proto: Int, tos: Int, tcpflags: Int,
                     start: Option[DateTime], stop: Option[DateTime],
                     srcAddress: InetAddress, dstAddress: InetAddress, nextHop: Option[InetAddress],
                     snmpInput: Int, snmpOutput: Int, packet: UUID) extends NetFlowData[NetFlowV1] {
  def version = "NetFlowV1"

  override lazy val jsonExtra: JObject = "snmp" -> ("input" -> snmpInput) ~ ("output" -> snmpOutput)
}
