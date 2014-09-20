package io.netflow.timeseries

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import io.netflow.lib._

case class NetFlowSeriesRecord(date: String, direction: TrafficType.Value, proto: Int, bytes: Long, pkts: Long,
                               src: String, dst: String,
                               srcPort: Int, dstPort: Int,
                               srcAS: Int, dstAS: Int)

sealed class NetFlowSeries extends CassandraTable[NetFlowSeries, NetFlowSeriesRecord] {

  object date extends StringColumn(this) with PartitionKey[String]
  object direction extends StringColumn(this) with PrimaryKey[String]
  object proto extends IntColumn(this) with PrimaryKey[Int]
  object srcPort extends IntColumn(this) with PrimaryKey[Int]
  object dstPort extends IntColumn(this) with PrimaryKey[Int]
  object bytes extends CounterColumn(this)
  object pkts extends CounterColumn(this)
  object src extends StringColumn(this) with PrimaryKey[String]
  object dst extends StringColumn(this) with PrimaryKey[String]
  object srcAS extends IntColumn(this) with PrimaryKey[Int]
  object dstAS extends IntColumn(this) with PrimaryKey[Int]

  override def fromRow(row: Row): NetFlowSeriesRecord = {
    NetFlowSeriesRecord(date(row), TrafficType.withName(direction(row)), proto(row), bytes(row), pkts(row),
      src(row), dst(row), srcPort(row), dstPort(row), srcAS(row), dstAS(row))
  }
}

object NetFlowSeries extends NetFlowSeries