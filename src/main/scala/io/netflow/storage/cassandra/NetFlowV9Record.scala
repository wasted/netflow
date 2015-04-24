package io.netflow.storage
package cassandra

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._
import com.websudos.phantom.column.{ DateTimeColumn, MapColumn, TimeUUIDColumn }
import io.netflow.flows.cflow._
import io.netflow.lib.FlowPacketMeta
import org.joda.time.DateTime

private[netflow] object NetFlowV9Packet extends FlowPacketMeta[NetFlowV9Packet] {
  def persist(fp: NetFlowV9Packet): Unit = fp.flows.foldLeft((0, new BatchStatement())) {
    case ((count, b), flowRow) =>
      val statement = flowRow match {
        case row: NetFlowV9Data =>
          NetFlowV9DataRecord.insert
            .value(_.id, UUIDs.startOf(fp.timestamp.getMillis + count))
            .value(_.packet, fp.id)
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
        case row: NetFlowV9Option =>
          NetFlowV9OptionRecord.insert
            .value(_.id, UUIDs.startOf(fp.timestamp.getMillis + count))
            .value(_.packet, fp.id)
            .value(_.sender, row.sender.getAddress)
            .value(_.timestamp, row.timestamp)
            .value(_.template, row.template)
            .value(_.uptime, row.uptime)
            .value(_.senderPort, row.senderPort)
            .value(_.length, row.length)
            .value(_.extra, row.extra)
        case row: NetFlowV9Template =>
          NetFlowV9TemplateRecord.update.where(_.sender eqs row.sender.getAddress).
            and(_.id eqs row.number).
            modify(_.senderPort setTo row.senderPort).
            and(_.packet setTo row.packet).
            and(_.last setTo DateTime.now).
            and(_.map setTo row.map)
        //case nf10: cflow.NetFlowV10TemplateRecord => FIXME Netflow 10
      }
      (count + 1, b.add(statement))
  }._2.future()(Connection.session)
}

private[netflow] sealed class NetFlowV9DataRecord extends CassandraTable[NetFlowV9DataRecord, NetFlowV9Data] {
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
  object extra extends MapColumn[NetFlowV9DataRecord, NetFlowV9Data, String, Long](this)

  def fromRow(row: Row): NetFlowV9Data = NetFlowV9Data(id(row), new InetSocketAddress(sender(row),
    senderPort(row)), length(row), template(row), uptime(row), timestamp(row), srcPort(row), dstPort(row),
    srcAS(row), dstAS(row), pkts(row), bytes(row), proto(row), tos(row), tcpflags(row), start(row), stop(row),
    srcAddress(row), dstAddress(row), nextHop(row), extra(row), packet(row))

}

private[netflow] object NetFlowV9DataRecord extends NetFlowV9DataRecord

private[netflow] sealed class NetFlowV9OptionRecord extends CassandraTable[NetFlowV9OptionRecord, NetFlowV9Option] {
  object id extends TimeUUIDColumn(this) with PartitionKey[UUID]
  object packet extends TimeUUIDColumn(this) with Index[UUID]
  object sender extends InetAddressColumn(this) with PrimaryKey[InetAddress]
  object timestamp extends DateTimeColumn(this) with PrimaryKey[DateTime]
  object uptime extends LongColumn(this)
  object template extends IntColumn(this) with PrimaryKey[Int]
  object senderPort extends IntColumn(this)
  object length extends IntColumn(this)
  object extra extends MapColumn[NetFlowV9OptionRecord, NetFlowV9Option, String, Long](this)

  def fromRow(row: Row): NetFlowV9Option = NetFlowV9Option(id(row),
    new InetSocketAddress(sender(row), senderPort(row)),
    length(row), template(row), uptime(row), timestamp(row), extra(row), packet(row))

}

private[netflow] object NetFlowV9OptionRecord extends NetFlowV9OptionRecord