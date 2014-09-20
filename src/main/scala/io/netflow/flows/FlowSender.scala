package io.netflow.flows

import java.net.InetAddress

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import io.wasted.util.{ Tryo, InetPrefix }
import org.joda.time.DateTime

case class FlowSenderRecord(ip: InetAddress, last: Option[DateTime], flows: Long,
                            prefixes: Set[InetPrefix], thruputPrefixes: Set[InetPrefix]) {
}

sealed class FlowSender extends CassandraTable[FlowSender, FlowSenderRecord] {

  object ip extends InetAddressColumn(this) with PartitionKey[InetAddress] with ClusteringOrder[InetAddress] with Ascending
  object last extends OptionalDateTimeColumn(this)
  object flows extends CounterColumn(this)
  object prefixes extends SetColumn[FlowSender, FlowSenderRecord, String](this)
  object thruputPrefixes extends SetColumn[FlowSender, FlowSenderRecord, String](this)

  private def string2prefix(str: String): Option[InetPrefix] = {
    val split = str.split("/")
    if (split.length != 2) None else for {
      len <- Tryo(split(1).toInt)
      base <- Tryo(InetAddress.getByName(split(0)))
    } yield InetPrefix(base, len)
  }

  private implicit val strings2prefixes = (x: Set[String]) => x.flatMap(string2prefix)

  override def fromRow(row: Row): FlowSenderRecord = {
    FlowSenderRecord(ip(row), last(row), flows(row), prefixes(row), thruputPrefixes(row))
  }
}

object FlowSender extends FlowSender