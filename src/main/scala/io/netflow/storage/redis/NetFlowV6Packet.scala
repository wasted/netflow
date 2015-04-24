package io.netflow.storage.redis

import io.netflow.flows.cflow._
import io.netflow.lib._

private[netflow] object NetFlowV6Packet extends FlowPacketMeta[NetFlowV6Packet] {
  def persist(fp: NetFlowV6Packet): Unit = ()
}