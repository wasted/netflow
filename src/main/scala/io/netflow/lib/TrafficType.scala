package io.netflow.lib

object TrafficType extends Enumeration {
  val Inbound = Value("in")
  val Outbound = Value("out")
  val InboundLocal = Value("in:local")
  val OutboundLocal = Value("out:local")
}
