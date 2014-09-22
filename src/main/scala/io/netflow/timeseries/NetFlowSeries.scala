package io.netflow.timeseries

import java.net.InetAddress

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import io.netflow.lib._
import io.wasted.util.InetPrefix

case class NetFlowSeriesRecord(sender: InetAddress, prefix: InetPrefix, date: String, name: String, bytes: Long, pkts: Long,
                               direction: TrafficType.Value, proto: Option[Int],
                               src: Option[String], dst: Option[String],
                               srcPort: Option[Int], dstPort: Option[Int], srcAS: Option[Int], dstAS: Option[Int])

sealed class NetFlowSeries extends CassandraTable[NetFlowSeries, NetFlowSeriesRecord] {

  object sender extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object prefix extends StringColumn(this) with PartitionKey[String]
  object date extends StringColumn(this) with PrimaryKey[String]
  object name extends StringColumn(this) with PrimaryKey[String]
  object direction extends StringColumn(this) with PrimaryKey[String]

  object bytes extends CounterColumn(this)
  object pkts extends CounterColumn(this)

  object proto extends OptionalIntColumn(this) with Index[Option[Int]]
  object srcPort extends OptionalIntColumn(this) with Index[Option[Int]]
  object dstPort extends OptionalIntColumn(this) with Index[Option[Int]]
  object src extends OptionalStringColumn(this) with Index[Option[String]]
  object dst extends OptionalStringColumn(this) with Index[Option[String]]
  object srcAS extends OptionalIntColumn(this) with Index[Option[Int]]
  object dstAS extends OptionalIntColumn(this) with Index[Option[Int]]

  override def fromRow(row: Row): NetFlowSeriesRecord = {
    val prefixT = prefix(row).split("/")
    assert(prefixT.length == 2, "Invalid Prefix of %s".format(prefix(row)))
    val pfx = InetAddress.getByName(prefixT(0))
    val pfxLen = prefixT(1).toInt
    NetFlowSeriesRecord(sender(row), InetPrefix(pfx, pfxLen), date(row), name(row), bytes(row), pkts(row),
      TrafficType.withName(direction(row)), proto(row),
      src(row), dst(row), srcPort(row), dstPort(row), srcAS(row), dstAS(row))
  }
}

object NetFlowSeries extends NetFlowSeries
