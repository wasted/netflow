package io.netflow.lib

import java.net.InetSocketAddress

import io.wasted.util.InetPrefix

case class SaveJob(
  sender: InetSocketAddress,
  flowPacket: FlowPacket,
  prefixes: List[InetPrefix])
