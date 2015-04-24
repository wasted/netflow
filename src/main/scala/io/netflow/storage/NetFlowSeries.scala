package io.netflow.storage

import java.net.InetAddress

import com.twitter.util.Future
import io.netflow.lib._
import net.liftweb.json.JValue

trait NetFlowSeries {
  def apply(date: String, profiles: List[String], sender: InetAddress, prefix: String): Future[(String, JValue)]
}

object NetFlowSeries {
  private def doLayer[T](f: NetFlowSeries => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Cassandra) => f(cassandra.NetFlowSeries)
    case Some(StorageLayer.Redis) => f(redis.NetFlowSeries)
    case _ => Future.exception(NoBackendDefined)
  }

  def apply(date: String, profiles: List[String], sender: InetAddress, prefix: String): Future[(String, JValue)] = doLayer {
    _(date, profiles, sender, prefix)
  }
}