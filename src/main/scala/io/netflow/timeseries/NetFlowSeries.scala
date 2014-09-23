package io.netflow.timeseries

import java.net.InetAddress

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import io.netflow.lib._
import io.wasted.util.InetPrefix

case class NetFlowSeriesRecord(sender: InetAddress, prefix: InetPrefix, date: String, name: String, bytes: Long, pkts: Long,
                               direction: TrafficType.Value)

sealed class NetFlowSeries extends CassandraTable[NetFlowSeries, NetFlowSeriesRecord] {

  object sender extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object prefix extends StringColumn(this) with PrimaryKey[String]
  object date extends StringColumn(this) with PrimaryKey[String]
  object name extends StringColumn(this) with PrimaryKey[String]
  object direction extends StringColumn(this) with PrimaryKey[String]

  object bytes extends CounterColumn(this)
  object pkts extends CounterColumn(this)

  override def fromRow(row: Row): NetFlowSeriesRecord = {
    val prefixT = prefix(row).split("/")
    assert(prefixT.length == 2, "Invalid Prefix of %s".format(prefix(row)))
    val pfx = InetAddress.getByName(prefixT(0))
    val pfxLen = prefixT(1).toInt
    NetFlowSeriesRecord(sender(row), InetPrefix(pfx, pfxLen), date(row), name(row), bytes(row), pkts(row),
      TrafficType.withName(direction(row)))
  }
}

object NetFlowSeries extends NetFlowSeries
