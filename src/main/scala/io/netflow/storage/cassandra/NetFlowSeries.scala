package io.netflow.storage.cassandra

import java.net.InetAddress

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits.{ InetAddressColumn, StringColumn }
import com.websudos.phantom._
import com.websudos.phantom.column.CounterColumn
import com.websudos.phantom.keys.{ PartitionKey, PrimaryKey }
import io.netflow.lib._
import io.netflow.storage.{ NetFlowSeries => NetFlowSeriesMeta }
import io.wasted.util.InetPrefix
import net.liftweb.json.JsonAST.JField
import net.liftweb.json._

import scala.concurrent.Future

private[storage] case class NetFlowSeriesRecord(sender: InetAddress, prefix: InetPrefix, date: String, name: String, bytes: Long, pkts: Long,
                                                direction: TrafficType.Value)

private[storage] sealed class NetFlowSeries extends CassandraTable[NetFlowSeries, NetFlowSeriesRecord] {

  object sender extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object prefix extends StringColumn(this) with PrimaryKey[String]
  object date extends StringColumn(this) with PrimaryKey[String]
  object name extends StringColumn(this) with PrimaryKey[String]
  object direction extends StringColumn(this) with PrimaryKey[String]

  object bytes extends CounterColumn(this)
  object pkts extends CounterColumn(this)

  override def fromRow(row: Row): NetFlowSeriesRecord = {
    val prefixT = prefix(row).split("/")
    assert(prefixT.length == 2, "Invalid Prefix of " + prefix(row))
    val pfx = InetAddress.getByName(prefixT(0))
    val pfxLen = prefixT(1).toInt
    NetFlowSeriesRecord(sender(row), InetPrefix(pfx, pfxLen), date(row), name(row), bytes(row), pkts(row),
      TrafficType.withName(direction(row)))
  }
}

private[storage] object NetFlowSeries extends NetFlowSeries with NetFlowSeriesMeta {
  import Connection._
  import com.websudos.phantom.Implicits._
  def apply(date: String, profiles: List[String], sender: InetAddress, prefix: String): com.twitter.util.Future[(String, JValue)] = {
    val p = com.twitter.util.Promise[(String, JValue)]()
    val r = Future.sequence(profiles.map { profile =>
      NetFlowSeries.select
        .where(_.sender eqs sender)
        .and(_.prefix eqs prefix)
        .and(_.date eqs date)
        .and(_.name eqs profile).fetch()
    }).map { rows =>
      date -> Extraction.decompose(rows.flatten[NetFlowSeriesRecord]).transform {
        case JField("prefix", _) => JNothing
        case JField("sender", _) => JNothing
        case JField("date", _) => JNothing
      }
    }
    r.onSuccess {
      case json => p.setValue(json)
    }
    r.onFailure {
      case t: Throwable => p.setException(t)
    }
    p
  }
}
