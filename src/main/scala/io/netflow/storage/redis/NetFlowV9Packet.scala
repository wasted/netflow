package io.netflow.storage.redis

import com.twitter.finagle.redis.util.StringToChannelBuffer
import io.netflow.flows.cflow._
import io.netflow.lib._
import net.liftweb.json.Serialization

private[netflow] object NetFlowV9Packet extends FlowPacketMeta[NetFlowV9Packet] {
  def persist(fp: NetFlowV9Packet): Unit = fp.flows.foreach {
    case tmpl: NetFlowV9Template =>
      val ip = tmpl.sender.getAddress.getHostAddress
      val key = StringToChannelBuffer("templates:" + ip)
      val index = StringToChannelBuffer(tmpl.id.toString)
      val value = Serialization.write(tmpl)
      Connection.client.hSet(key, index, StringToChannelBuffer(value))
    case _ =>
  }
}
