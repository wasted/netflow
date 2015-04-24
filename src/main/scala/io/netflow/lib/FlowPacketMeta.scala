package io.netflow.lib

private[netflow] trait FlowPacketMeta[T <: FlowPacket] {
  def persist(fp: T): Unit
}
