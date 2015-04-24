package io.netflow.storage

import java.net.InetAddress

import com.twitter.util.Future
import io.netflow.lib._
import io.wasted.util.InetPrefix
import org.joda.time.DateTime

private[netflow] case class FlowSender(ip: InetAddress, last: Option[DateTime], prefixes: Set[InetPrefix]) {
  def save() = FlowSender.save(this)
  def delete() = FlowSender.delete(ip)
}

private[storage] case class FlowSenderCount(ip: InetAddress, flows: Long, dgrams: Long)

private[storage] trait FlowSenderMeta {
  def findAll(): Future[List[FlowSender]]
  def find(inet: InetAddress): Future[FlowSender]
  def save(sender: FlowSender): Future[FlowSender]
  def delete(inet: InetAddress): Future[Unit]
}

private[netflow] object FlowSender {
  private def doLayer[T](f: FlowSenderMeta => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Redis) => f(redis.FlowSenderRecord)
    case Some(StorageLayer.Cassandra) => f(cassandra.FlowSenderRecord)
    case _ => Future.exception(NoBackendDefined)
  }

  def findAll(): Future[List[FlowSender]] = doLayer { _.findAll() }
  def find(inet: InetAddress): Future[FlowSender] = doLayer { _.find(inet) }
  def save(sender: FlowSender): Future[FlowSender] = doLayer { _.save(sender) }
  def delete(inet: InetAddress): Future[Unit] = doLayer { _.delete(inet) }
}