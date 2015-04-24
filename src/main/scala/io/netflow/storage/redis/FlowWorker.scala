package io.netflow.storage.redis

import java.net.InetAddress

import com.twitter.finagle.redis.util.StringToChannelBuffer
import io.netflow.flows._
import io.netflow.lib._
import io.wasted.util._

private[storage] class FlowWorker(num: Int) extends Wactor {
  override val loggerName = "FlowWorker %02d:".format(num)

  private def hincrBy(str1: String, str2: String, inc: Long) = {
    Connection.client.hIncrBy(StringToChannelBuffer(str1), StringToChannelBuffer(str2), inc)
  }

  private def datagram(s: InetAddress, kind: String, flows: Option[Int] = None) = {
    val key = StringToChannelBuffer("stats:" + s.getHostAddress)
    Connection.client.hIncrBy(key, StringToChannelBuffer(kind), 1)
    flows.map { flowCount =>
      Connection.client.hIncrBy(key, StringToChannelBuffer(kind + ":flows"), flowCount)
    }
  }

  def receive = {
    case BadDatagram(date, sender) =>
      datagram(sender, "bad")

    case SaveJob(sender, flowPacket, prefixes) =>
      val it1 = flowPacket.flows.iterator
      while (it1.hasNext) it1.next() match {
        case tmpl: cflow.Template =>
        /* Handle NetFlowData */
        case flow: NetFlowData[_] =>
          var ourFlow = false

          val srcNetworks = prefixes.filter(_.contains(flow.srcAddress))
          val dstNetworks = prefixes.filter(_.contains(flow.dstAddress))

          // src - out
          var it3 = srcNetworks.iterator
          while (it3.hasNext) {
            val prefix = it3.next()
            ourFlow = true
            // If it is *NOT* *to* another network we monitor
            val trafficType = if (dstNetworks.length == 0) TrafficType.Outbound else TrafficType.OutboundLocal
            save(flowPacket, flow, flow.srcAddress, trafficType, prefix)
          }

          // dst - in
          it3 = dstNetworks.iterator
          while (it3.hasNext) {
            val prefix = it3.next()
            ourFlow = true
            // If it is *NOT* *to* another network we monitor
            val trafficType = if (srcNetworks.length == 0) TrafficType.Inbound else TrafficType.InboundLocal
            save(flowPacket, flow, flow.dstAddress, trafficType, prefix)
          }

          if (!ourFlow) { // invalid flow
            debug("Ignoring Flow: %s", flow)
            datagram(sender.getAddress, "bad")
          }
        case _ =>
      }

      val flowSeq = flowPacket match {
        case a: cflow.NetFlowV5Packet => ", flowSeq: " + a.flowSequence
        case a: cflow.NetFlowV6Packet => ", flowSeq: " + a.flowSequence
        case a: cflow.NetFlowV7Packet => ", flowSeq: " + a.flowSequence
        case a: cflow.NetFlowV9Packet => ", flowSeq: " + a.flowSequence
        //case a: cflow.NetFlowV10Packet => ", flowSeq: " + a.flowSequence
        case _ => ""
      }

      val packetInfoStr = flowPacket.version.replaceAll("Packet", "-") + " length: " + flowPacket.length + flowSeq
      val passedFlowsStr = flowPacket.flows.length + "/" + flowPacket.count + " passed"

      val recvdFlows = flowPacket.flows.groupBy(_.version)
      val recvdFlowsStr = recvdFlows.toList.sortBy(_._1).map(fc => if (fc._2.length == 1) fc._1 else fc._1 + ": " + fc._2.length).mkString(", ")

      // log an elaborate string to loglevel info describing this packet.
      // Warning: can produce huge amounts of logs if written to disk.
      val debugStr = "\t" + packetInfoStr + "\t" + passedFlowsStr + "\t" + recvdFlowsStr

      // Sophisticated log-level hacking :<
      if (flowPacket.count != flowPacket.flows.length) error(debugStr)
      else if (debugStr.contains("Template")) info(debugStr) else debug(debugStr)

      // count them to database
      datagram(sender.getAddress, flowPacket.version, Some(flowPacket.flows.length))
      val it2 = recvdFlows.iterator
      while (it2.hasNext) {
        val rcvd = it2.next()
        datagram(sender.getAddress, rcvd._1, Some(rcvd._2.length))
      }
  }

  private def save(flowPacket: FlowPacket, flow: NetFlowData[_], localAddress: InetAddress, direction: TrafficType.Value, prefix: InetPrefix) {
    val dir = direction.toString
    val ip = localAddress.getHostAddress
    val prot = flow.proto
    val port = flow.dstPort
    //val port = if (direction == 'in) flow.dstPort else flow.srcPort

    val date = flowPacket.timestamp
    val year = date.getYear.toString
    val month = "%02d".format(date.getMonthOfYear)
    val day = "%02d".format(date.getDayOfMonth)
    val hour = "%02d".format(date.getHourOfDay)
    val minute = "%02d".format(date.getMinuteOfHour)

    def account(prefix: String, value: Long) {
      hincrBy(prefix + ":years", year, value)
      hincrBy(prefix + ":" + year, month, value)
      hincrBy(prefix + ":" + year + month, day, value)
      hincrBy(prefix + ":" + year + month + day, hour, value)
      hincrBy(prefix + ":" + year + month + day + "-" + hour, minute, value)
    }

    // Account per Sender
    account("bytes:" + dir, flow.bytes)
    account("pkts:" + dir, flow.pkts)

    // Account per Sender with Protocols
    account("bytes:" + dir + ":proto:" + prot, flow.bytes)
    account("pkts:" + dir + ":proto:" + prot, flow.pkts)

    account("bytes:" + dir + ":port:" + port, flow.bytes)
    account("pkts:" + dir + ":port:" + port, flow.pkts)

    val pfx = prefix.prefix.toString

    // Account per Sender and Network
    account("bytes:" + dir + ":" + pfx, flow.bytes)
    account("pkts:" + dir + ":" + pfx, flow.pkts)

    // Account per Sender and Network with Protocols
    account("bytes:" + dir + ":" + pfx + ":proto:" + prot, flow.bytes)
    account("pkts:" + dir + ":" + pfx + ":proto:" + prot, flow.pkts)

    account("bytes:" + dir + ":" + pfx + ":port:" + port, flow.bytes)
    account("pkts:" + dir + ":" + pfx + ":port:" + port, flow.pkts)

    // Account per Sender and IP
    account("bytes:" + dir + ":" + ip, flow.bytes)
    account("pkts:" + dir + ":" + ip, flow.pkts)

    // Account per Sender and IP with Protocols
    account("bytes:" + dir + ":" + ip + ":proto:" + prot, flow.bytes)
    account("pkts:" + dir + ":" + ip + ":proto:" + prot, flow.pkts)

    account("bytes:" + dir + ":" + ip + ":port:" + port, flow.bytes)
    account("pkts:" + dir + ":" + ip + ":port:" + port, flow.pkts)
  }
}
