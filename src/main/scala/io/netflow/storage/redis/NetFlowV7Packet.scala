package io.netflow.storage.redis

import io.netflow.flows.cflow._
import io.netflow.lib._

private[netflow] object NetFlowV7Packet extends FlowPacketMeta[NetFlowV7Packet] {
  def persist(fp: NetFlowV7Packet): Unit = ()
}