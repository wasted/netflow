package io.netflow.storage
package redis

import java.net.InetAddress

import com.twitter.util.Future
import net.liftweb.json._

private[netflow] object NetFlowSeries extends NetFlowSeries {
  def apply(date: String, profiles: List[String], sender: InetAddress, prefix: String): Future[(String, JValue)] = {
    Future.value(date -> JString("Not implemented yet for Redis backend"))
  }
}
