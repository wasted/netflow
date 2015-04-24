package io.netflow.storage
package cassandra

import java.net.InetAddress

import com.datastax.driver.core.Row
import com.twitter.util.{ Future, Promise }
import com.websudos.phantom.Implicits._
import io.netflow.lib._

private[storage] class FlowSenderRecord extends CassandraTable[FlowSenderRecord, FlowSender] {

  object ip extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object last extends OptionalDateTimeColumn(this)
  object prefixes extends SetColumn[FlowSenderRecord, FlowSender, String](this)

  private implicit val strings2prefixes = (x: Set[String]) => x.flatMap(string2prefix)

  override def fromRow(row: Row): FlowSender = {
    FlowSender(ip(row), last(row), prefixes(row))
  }
}

private[storage] object FlowSenderRecord extends FlowSenderRecord with FlowSenderMeta {
  lazy final val notFound = new Throwable("No entry was found") with scala.util.control.NoStackTrace
  import Connection._

  def findAll(): Future[List[FlowSender]] = {
    val p = new Promise[List[FlowSender]]()
    val scalaF = this.select.fetch()
    scalaF.onFailure {
      case t: Throwable => p.setException(t)
    }
    scalaF.onSuccess {
      case s => p.setValue(s.toList)
    }
    p
  }

  def find(inet: InetAddress): Future[FlowSender] = {
    val p = new Promise[FlowSender]()
    val scalaF = this.select.where(_.ip eqs inet).get()
    scalaF.onFailure {
      case t: Throwable => p.setException(t)
    }
    scalaF.onSuccess {
      case Some(s) => p.setValue(s)
      case None => p.setException(notFound)
    }
    p
  }

  def delete(inet: InetAddress): Future[Unit] = {
    val p = new Promise[Unit]()
    val scalaF = this.delete(inet)
    scalaF.onFailure {
      case t: Throwable => p.setException(t)
    }
    scalaF.onSuccess {
      case x => p.setDone()
    }
    p
  }

  def save(sender: FlowSender): Future[FlowSender] = {
    val p = new Promise[FlowSender]()
    val scalaF = this.update.where(_.ip eqs sender.ip).
      modify(_.prefixes setTo sender.prefixes.map(_.toString)).future()(Connection.session)
    scalaF.onFailure {
      case t: Throwable => p.setException(t)
    }
    scalaF.onSuccess {
      case rs => p.setValue(sender)
    }
    p
  }
}

private[storage] class FlowSenderCountRecord extends CassandraTable[FlowSenderCountRecord, FlowSenderCount] {

  object ip extends InetAddressColumn(this) with PartitionKey[InetAddress]
  object dgrams extends CounterColumn(this)
  object flows extends CounterColumn(this)

  override def fromRow(row: Row): FlowSenderCount = {
    FlowSenderCount(ip(row), flows(row), dgrams(row))
  }
}

private[storage] object FlowSenderCountRecord extends FlowSenderCountRecord