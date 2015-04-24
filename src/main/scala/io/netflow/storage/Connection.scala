package io.netflow.storage

import com.twitter.util.Future
import io.netflow.lib._

private[storage] trait Connection {
  def start(): Unit
  def shutdown(): Unit
}

private[netflow] object Connection {
  private def doLayer[T](f: Connection => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Cassandra) => f(cassandra.Connection)
    case Some(StorageLayer.Redis) => f(redis.Connection)
    case _ => Future.exception(NoBackendDefined)
  }

  def start(): Future[Unit] = doLayer { layer => Future(layer.start()) }
  def stop(): Future[Unit] = doLayer { layer => Future(layer.shutdown()) }
}
