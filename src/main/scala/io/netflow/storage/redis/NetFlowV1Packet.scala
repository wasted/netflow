package io.netflow.storage.redis

import io.netflow.flows.cflow._
import io.netflow.lib._

private[netflow] object NetFlowV1Packet extends FlowPacketMeta[NetFlowV1Packet] {
  def persist(fp: NetFlowV1Packet): Unit = ()
}