package io.netflow.timeseries

import java.net.InetAddress
import java.util.UUID

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import org.joda.time.DateTime

/**
 * Tracks FlowPackets
 */
case class NetFlowStatsRecord(id: UUID, date: DateTime, sender: InetAddress, port: Int, version: String, flows: Int, bytes: Int)

sealed class NetFlowStats extends CassandraTable[NetFlowStats, NetFlowStatsRecord] {

  object sender extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object id extends TimeUUIDColumn(this) with Index[UUID]
  object port extends IntColumn(this) with Index[Int]
  object version extends StringColumn(this) with Index[String]
  object date extends DateTimeColumn(this) with Index[DateTime]
  object flows extends IntColumn(this)
  object bytes extends IntColumn(this)

  override def fromRow(row: Row): NetFlowStatsRecord = {
    NetFlowStatsRecord(id(row), date(row), sender(row), port(row), version(row), flows(row), bytes(row))
  }
}

object NetFlowStats extends NetFlowStats