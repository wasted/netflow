package io.netflow.storage
package cassandra

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._
import com.websudos.phantom.column.{ DateTimeColumn, TimeUUIDColumn }
import io.netflow.flows.cflow._
import io.netflow.lib._
import org.joda.time.DateTime

private[netflow] object NetFlowV1Packet extends FlowPacketMeta[NetFlowV1Packet] {
  def persist(fp: NetFlowV1Packet): Unit = fp.flows.foldLeft((0, new BatchStatement())) {
    case ((count, b), row) =>
      val statement = NetFlowV1Record.insert
        .value(_.id, UUIDs.startOf(fp.timestamp.getMillis + count))
        .value(_.packet, fp.id)
        .value(_.sender, row.sender.getAddress)
        .value(_.timestamp, row.timestamp)
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
        .value(_.snmpInput, row.snmpInput)
        .value(_.snmpOutput, row.snmpOutput)
      (count + 1, b.add(statement))
  }._2.future()(Connection.session)
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

private[netflow] sealed class NetFlowV1Record extends CassandraTable[NetFlowV1Record, NetFlowV1] {

  object id extends TimeUUIDColumn(this) with PartitionKey[UUID]
  object packet extends TimeUUIDColumn(this) with Index[UUID]
  object sender extends InetAddressColumn(this) with PrimaryKey[InetAddress]
  object timestamp extends DateTimeColumn(this) with PrimaryKey[DateTime]
  object uptime extends LongColumn(this)
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
  object snmpInput extends IntColumn(this)
  object snmpOutput extends IntColumn(this)

  def fromRow(row: Row): NetFlowV1 = NetFlowV1(id(row), new InetSocketAddress(sender(row), senderPort(row)),
    length(row), uptime(row), timestamp(row), srcPort(row), dstPort(row), srcAS(row), dstAS(row), pkts(row),
    bytes(row), proto(row), tos(row), tcpflags(row), start(row), stop(row), srcAddress(row), dstAddress(row),
    nextHop(row), snmpInput(row), snmpOutput(row), packet(row))
}

private[netflow] object NetFlowV1Record extends NetFlowV1Record