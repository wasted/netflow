package io.netflow.timeseries

import java.net.InetAddress

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import io.netflow.lib._
import io.wasted.util._

case class NetFlowSeriesRecord(date: String, direction: TrafficType.Value, proto: Int, bytes: Long, pkts: Long,
                               srcIP: Option[InetAddress], dstIP: Option[InetAddress],
                               srcPort: Int, dstPort: Int,
                               srcAS: Int, dstAS: Int)

sealed class NetFlowSeries extends CassandraTable[NetFlowSeries, NetFlowSeriesRecord] {

  object date extends StringColumn(this) with PartitionKey[String]
  object direction extends StringColumn(this) with PrimaryKey[String]
  object proto extends IntColumn(this) with Index[Int]
  object srcPort extends IntColumn(this) with Index[Int]
  object dstPort extends IntColumn(this) with Index[Int]
  object bytes extends CounterColumn(this)
  object pkts extends CounterColumn(this)
  object srcIP extends InetAddressColumn(this) with Index[InetAddress]
  object dstIP extends InetAddressColumn(this) with Index[InetAddress]
  object srcAS extends IntColumn(this) with Index[Int]
  object dstAS extends IntColumn(this) with Index[Int]

  override def fromRow(row: Row): NetFlowSeriesRecord = {
    NetFlowSeriesRecord(date(row), TrafficType.withName(direction(row)), proto(row), bytes(row), pkts(row),
      Tryo(srcIP(row)), Tryo(dstIP(row)), srcPort(row), dstPort(row), srcAS(row), dstAS(row))
  }
}

object NetFlowSeries extends NetFlowSeries