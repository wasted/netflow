package io.netflow.flows.cflow

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.Implicits._
import io.netflow.actors.SenderWorker
import io.netflow.lib._
import io.netty.buffer._
import io.wasted.util._
import net.liftweb.json._
import org.joda.time.DateTime

import scala.util.{ Failure, Success, Try }

/**
 * NetFlow Version 9 Packet - FlowSet DataSet
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
 * | 12-15 | PackageSeq    | Sequence counter of total flows exported             |
 * *-------*---------------*------------------------------------------------------*
 * | 16-19 | Source ID     | engine type+engine id                                |
 * *-------*---------------*------------------------------------------------------*
 * | 20-   | others        | Unused (zero) bytes                                  |
 * *-------*---------------*------------------------------------------------------*
 */
object NetFlowV9Packet extends Logger {
  private val headerSize = 20

  /**
   * Parse a v9 Flow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param actor Actor which holds the Templates for async saves
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, actor: SenderWorker): Try[NetFlowV9Packet] = Try[NetFlowV9Packet] {
    val length = buf.readableBytes()
    val version = buf.getUnsignedInteger(0, 2).toInt
    if (version != 9) return Failure(new InvalidFlowVersionException(version))

    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    if (length < headerSize)
      return Failure(new IncompleteFlowPacketHeaderException)

    val count = buf.getUnsignedInteger(2, 2).toInt
    val uptime = buf.getUnsignedInteger(4, 4)
    val timestamp = new DateTime(buf.getUnsignedInteger(8, 4) * 1000)
    val id = UUIDs.startOf(timestamp.getMillis)
    val flowSequence = buf.getUnsignedInteger(12, 4)
    val sourceId = buf.getUnsignedInteger(16, 4)

    var flowsetCounter = 0
    var packetOffset = headerSize

    // we use a mutable array here in order not to bash the garbage collector so badly
    // because whenever we append something to our vector, the old vectors need to get GC'd
    val flows = scala.collection.mutable.ArrayBuffer[Flow[_]]()
    while (flowsetCounter < count && packetOffset < length) {
      val flowsetId = buf.getUnsignedInteger(packetOffset, 2).toInt
      val flowsetLength = buf.getUnsignedInteger(packetOffset + 2, 2).toInt
      if (flowsetLength == 0) return Failure(new IllegalFlowSetLengthException)
      if (packetOffset + flowsetLength > length) return Failure(new ShortFlowPacketException)

      flowsetId match {
        case 0 | 2 => // template flowset - 0 NetFlow v9, 2 IPFIX
          var templateOffset = packetOffset + 4 // add the 4 byte flowset Header
          debug("Template FlowSet (" + flowsetId + ") from " + senderIP + ":" + senderPort)
          do {
            val fieldCount = buf.getUnsignedShort(templateOffset + 2)
            val templateSize = fieldCount * 4 + 4
            if (templateOffset + templateSize < length) {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV9TemplateMeta(sender, buffer, id, flowsetId, timestamp) match {
                case Success(tmpl) =>
                  actor.setTemplate(tmpl)
                  flows += tmpl
                case Failure(e) => warn(e.toString)
              }
              flowsetCounter += 1
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case 1 | 3 => // template flowset - 1 NetFlow v9, 3 IPFIX
          debug("OptionTemplate FlowSet (" + flowsetId + ") from " + senderIP + ":" + senderPort)
          var templateOffset = packetOffset + 4 // add the 4 byte flowset Header
          do {
            val scopeLen = buf.getUnsignedInteger(templateOffset + 2, 2).toInt
            val optionLen = buf.getUnsignedInteger(templateOffset + 4, 2).toInt
            val templateSize = scopeLen + optionLen + 6
            if (templateOffset + templateSize < length) {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV9TemplateMeta(sender, buffer, id, flowsetId, timestamp) match {
                case Success(tmpl) =>
                  actor.setTemplate(tmpl)
                  flows += tmpl
                case Failure(e) => warn(e.toString); e.printStackTrace()
              }
              flowsetCounter += 1
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case a: Int if a > 255 => // flowset - templateId == flowsetId
          actor.templates.get(flowsetId).
            filter(_.isInstanceOf[NetFlowV9TemplateRecord]).
            map(_.asInstanceOf[NetFlowV9TemplateRecord]).
            map { tmpl =>
              val option = tmpl.flowsetId == 1
              var recordOffset = packetOffset + 4 // add the 4 byte flowset Header
              while (recordOffset - packetOffset + tmpl.length <= flowsetLength) {
                val buffer = buf.slice(recordOffset, tmpl.length)
                val flow =
                  if (option) NetFlowV9Option(sender, buffer, id, tmpl, uptime, timestamp)
                  else NetFlowV9Data(sender, buffer, id, tmpl, uptime, timestamp)

                flow match {
                  case Success(flow) => flows += flow
                  case Failure(e) => warn(e.toString)
                }
                flowsetCounter += 1
                recordOffset += tmpl.length
              }
            }
        case a: Int => debug("Unexpected TemplateId (" + a + ")")
      }
      packetOffset += flowsetLength
    }

    NetFlowV9Packet(id, sender, length, uptime, timestamp, flows.toList, flowSequence, sourceId)
  }
}

case class NetFlowV9Packet(id: UUID, sender: InetSocketAddress, length: Int, uptime: Long,
                           timestamp: DateTime, flows: List[Flow[_]],
                           flowSequence: Long, sourceId: Long) extends FlowPacket {
  def version = "NetFlowV9 Packet"
  def count = flows.length

  def persist() = flows.foldLeft((0, new BatchStatement())) {
    case ((count, b), flowRow) =>
      val statement = flowRow match {
        case row: NetFlowV9DataRecord =>
          NetFlowV9Data.insert
            .value(_.id, UUIDs.startOf(timestamp.getMillis + count))
            .value(_.packet, id)
            .value(_.sender, row.sender.getAddress)
            .value(_.timestamp, row.timestamp)
            .value(_.template, row.template)
            .value(_.uptime, row.uptime)
            .value(_.senderPort, row.senderPort)
            .value(_.length, row.length)
            .value(_.srcPort, row.srcPort)
            .value(_.dstPort, row.dstPort)
            .value(_.srcAS, row.srcAS)
            .value(_.dstAS, row.dstAS)
            .value(_.pkts, row.pkts)
            .value(_.bytes, row.bytes)
            .value(_.proto, row.proto)
            .value(_.tos, row.tos)
            .value(_.tcpflags, row.tcpflags)
            .value(_.start, row.start)
            .value(_.stop, row.stop)
            .value(_.srcAddress, row.srcAddress)
            .value(_.dstAddress, row.dstAddress)
            .value(_.nextHop, row.nextHop)
            .value(_.extra, row.extra)
        case row: NetFlowV9OptionRecord =>
          NetFlowV9Option.insert
            .value(_.id, UUIDs.startOf(timestamp.getMillis + count))
            .value(_.packet, id)
            .value(_.sender, row.sender.getAddress)
            .value(_.timestamp, row.timestamp)
            .value(_.template, row.template)
            .value(_.uptime, row.uptime)
            .value(_.senderPort, row.senderPort)
            .value(_.length, row.length)
            .value(_.extra, row.extra)
        case row: NetFlowV9TemplateRecord =>
          NetFlowV9Template.update.where(_.sender eqs row.sender.getAddress).
            and(_.id eqs row.number).
            modify(_.senderPort setTo row.senderPort).
            and(_.packet setTo row.packet).
            and(_.last setTo DateTime.now).
            and(_.map setTo row.map)
        //case nf10: cflow.NetFlowV10TemplateRecord => FIXME Netflow 10
      }
      (count + 1, b.add(statement))
  }._2.future()
}

sealed class NetFlowV9Data extends CassandraTable[NetFlowV9Data, NetFlowV9DataRecord] {
  import io.netflow.flows.cflow.TemplateFields._
  def parseExtraFields = NodeConfig.values.netflow.extraFields

  /**
   * Parse a Version 9 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param fpId FlowPacket-UUID this Flow arrived in
   * @param template NetFlow Template for this Flow
   * @param timestamp DateTime when this flow was exported
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, template: NetFlowV9TemplateRecord,
            uptime: Long, timestamp: DateTime) = Try[NetFlowV9DataRecord] {
    val srcPort = buf.getUnsignedInteger(template, L4_SRC_PORT).get.toInt
    val dstPort = buf.getUnsignedInteger(template, L4_DST_PORT).get.toInt

    val srcAS = buf.getUnsignedInteger(template, SRC_AS).map(_.toInt).filter(_ != -1)
    val dstAS = buf.getUnsignedInteger(template, DST_AS).map(_.toInt).filter(_ != -1)
    val proto = (buf.getUnsignedInteger(template, PROT) getOrElse -1L).toInt
    val tos = (buf.getUnsignedInteger(template, SRC_TOS) getOrElse -1L).toInt

    // calculate the offset from uptime and subtract that from the timestamp
    val start = buf.getUnsignedInteger(template, FIRST_SWITCHED).filter(_ == 0).
      map(x => timestamp.minus(uptime - x))
    val stop = buf.getUnsignedInteger(template, LAST_SWITCHED).filter(_ == 0).
      map(x => timestamp.minus(uptime - x))
    val tcpflags = (buf.getUnsignedInteger(template, TCP_FLAGS) getOrElse -1L).toInt

    val srcAddress = buf.getInetAddress(template, IPV4_SRC_ADDR, IPV6_SRC_ADDR)
    val dstAddress = buf.getInetAddress(template, IPV4_DST_ADDR, IPV6_DST_ADDR)
    val nextHop = Option(buf.getInetAddress(template, IPV4_NEXT_HOP, IPV6_NEXT_HOP)).
      filter(_.getHostAddress != "0.0.0.0") // FIXME filter v6

    val pkts = buf.getUnsignedInteger(template, InPKTS, OutPKTS).get
    val bytes = buf.getUnsignedInteger(template, InBYTES, OutBYTES).get
    val extraFields: Map[String, Long] = if (!parseExtraFields) Map() else template.getExtraFields(buf)
    NetFlowV9DataRecord(UUIDs.timeBased(), sender, buf.readableBytes(), template.number, uptime, timestamp,
      srcPort, dstPort, srcAS, dstAS, pkts, bytes, proto,
      tos, tcpflags, start, stop, srcAddress, dstAddress, nextHop, extraFields, fpId)
  }

  object id extends UUIDColumn(this) with PartitionKey[UUID]
  object packet extends TimeUUIDColumn(this) with Index[UUID]
  object sender extends InetAddressColumn(this) with PrimaryKey[InetAddress]
  object timestamp extends DateTimeColumn(this) with PrimaryKey[DateTime]
  object uptime extends LongColumn(this)
  object template extends IntColumn(this) with PrimaryKey[Int]
  object senderPort extends IntColumn(this) with Index[Int]
  object length extends IntColumn(this)
  object srcPort extends IntColumn(this) with Index[Int]
  object dstPort extends IntColumn(this) with Index[Int]
  object srcAS extends OptionalIntColumn(this) with Index[Option[Int]]
  object dstAS extends OptionalIntColumn(this) with Index[Option[Int]]
  object pkts extends LongColumn(this)
  object bytes extends LongColumn(this)
  object proto extends IntColumn(this) with Index[Int]
  object tos extends IntColumn(this) with Index[Int]
  object tcpflags extends IntColumn(this)
  object start extends OptionalDateTimeColumn(this) with Index[Option[DateTime]]
  object stop extends OptionalDateTimeColumn(this) with Index[Option[DateTime]]
  object srcAddress extends InetAddressColumn(this) with Index[InetAddress]
  object dstAddress extends InetAddressColumn(this) with Index[InetAddress]
  object nextHop extends OptionalInetAddressColumn(this) with Index[Option[InetAddress]]
  object extra extends MapColumn[NetFlowV9Data, NetFlowV9DataRecord, String, Long](this)

  def fromRow(row: Row): NetFlowV9DataRecord = NetFlowV9DataRecord(id(row), new InetSocketAddress(sender(row),
    senderPort(row)), length(row), template(row), uptime(row), timestamp(row), srcPort(row), dstPort(row),
    srcAS(row), dstAS(row), pkts(row), bytes(row), proto(row), tos(row), tcpflags(row), start(row), stop(row),
    srcAddress(row), dstAddress(row), nextHop(row), extra(row), packet(row))

}

object NetFlowV9Data extends NetFlowV9Data

case class NetFlowV9DataRecord(id: UUID, sender: InetSocketAddress, length: Int, template: Int, uptime: Long,
                               timestamp: DateTime, srcPort: Int, dstPort: Int, srcAS: Option[Int], dstAS: Option[Int],
                               pkts: Long, bytes: Long, proto: Int, tos: Int, tcpflags: Int,
                               start: Option[DateTime], stop: Option[DateTime],
                               srcAddress: InetAddress, dstAddress: InetAddress, nextHop: Option[InetAddress],
                               extra: Map[String, Long], packet: UUID) extends NetFlowData[NetFlowV9DataRecord] {
  def version = "NetFlowV9Data " + template

  override lazy val jsonExtra = Extraction.decompose(extra).asInstanceOf[JObject]
  override lazy val stringExtra = "- Template %s".format(template)
}

sealed class NetFlowV9Option extends CassandraTable[NetFlowV9Option, NetFlowV9OptionRecord] {

  /**
   * Parse a Version 9 Option Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param fpId FlowPacket-UUID which this Flow arrived in
   * @param template NetFlow Template for this Flow
   * @param timestamp DateTime when this flow was exported
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, template: NetFlowV9TemplateRecord,
            uptime: Long, timestamp: DateTime) = Try[NetFlowV9OptionRecord] {
    NetFlowV9OptionRecord(UUIDs.timeBased(), sender, buf.readableBytes(), template.number, uptime, timestamp,
      template.getExtraFields(buf), fpId)
  }

  object id extends TimeUUIDColumn(this) with PartitionKey[UUID]
  object packet extends TimeUUIDColumn(this) with Index[UUID]
  object sender extends InetAddressColumn(this) with PrimaryKey[InetAddress]
  object timestamp extends DateTimeColumn(this) with PrimaryKey[DateTime]
  object uptime extends LongColumn(this)
  object template extends IntColumn(this) with PrimaryKey[Int]
  object senderPort extends IntColumn(this)
  object length extends IntColumn(this)
  object extra extends MapColumn[NetFlowV9Option, NetFlowV9OptionRecord, String, Long](this)

  def fromRow(row: Row): NetFlowV9OptionRecord = NetFlowV9OptionRecord(id(row),
    new InetSocketAddress(sender(row), senderPort(row)),
    length(row), template(row), uptime(row), timestamp(row), extra(row), packet(row))

}

object NetFlowV9Option extends NetFlowV9Option

case class NetFlowV9OptionRecord(id: UUID, sender: InetSocketAddress, length: Int, template: Int, uptime: Long,
                                 timestamp: DateTime, extra: Map[String, Long], packet: UUID)
  extends Flow[NetFlowV9OptionRecord] {
  def version = "NetFlowV9Option " + template
  override lazy val json = Serialization.write(extra)

}

