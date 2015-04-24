package io.netflow.storage.redis

import java.net.InetAddress

import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import io.netflow.flows.cflow._
import io.netflow.lib._
import io.netty.util.CharsetUtil
import net.liftweb.json.JsonParser

private[netflow] object NetFlowV9TemplateRecord extends NetFlowTemplateMeta[NetFlowV9Template] {
  def findAll(inet: InetAddress): Future[Seq[NetFlowV9Template]] = {
    val key = StringToChannelBuffer("templates:" + inet.getHostAddress)
    Connection.client.hGetAll(key).map {
      _.map(_._2.toString(CharsetUtil.UTF_8)).map(JsonParser.parse).flatMap(_.extractOpt[NetFlowV9Template])
    }
  }
}