package io.netflow.storage.cassandra

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.Row
import com.twitter.util.{ Future, Promise }
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._
import com.websudos.phantom.keys.{ Index, PartitionKey, PrimaryKey }
import io.netflow.flows.cflow._
import io.netflow.lib._

private[netflow] sealed class NetFlowV9TemplateRecord extends CassandraTable[NetFlowV9TemplateRecord, NetFlowV9Template] {

  object sender extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object id extends IntColumn(this) with PrimaryKey[Int]
  object packet extends TimeUUIDColumn(this) with Index[UUID]
  object senderPort extends IntColumn(this)
  object last extends DateTimeColumn(this)
  object map extends MapColumn[NetFlowV9TemplateRecord, NetFlowV9Template, String, Int](this)

  def fromRow(row: Row) = NetFlowV9Template(id(row), new InetSocketAddress(sender(row), senderPort(row)),
    packet(row), last(row), map(row))

}

private[netflow] object NetFlowV9TemplateRecord extends NetFlowV9TemplateRecord with NetFlowTemplateMeta[NetFlowV9Template] {
  import Connection._
  import com.websudos.phantom.Implicits._
  def findAll(inet: InetAddress): Future[Seq[NetFlowV9Template]] = {
    val p = Promise[Seq[NetFlowV9Template]]()
    val scalaF = this.select.where(_.sender eqs inet).fetch()
    scalaF.onSuccess {
      case s => p.setValue(s)
    }
    scalaF.onFailure {
      case t: Throwable => p.setException(t)
    }
    p
  }
}