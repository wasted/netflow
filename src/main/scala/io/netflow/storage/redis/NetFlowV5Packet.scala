package io.netflow.storage.redis

import io.netflow.flows.cflow._
import io.netflow.lib._

private[netflow] object NetFlowV5Packet extends FlowPacketMeta[NetFlowV5Packet] {
  def persist(fp: NetFlowV5Packet): Unit = ()
}