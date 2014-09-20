package io.netflow.actors

import java.net.InetAddress

import io.netflow.flows._
import io.netflow.lib._
import io.wasted.util._
import org.joda.time.DateTime

private case class BadDatagram(date: DateTime, sender: InetAddress)

private case class SaveJob(
  sender: InetAddress,
  flowPacket: FlowPacket,
  prefixes: List[InetPrefix],
  thruputPrefixes: List[InetPrefix])

private[netflow] class FlowWorker(num: Int) extends Wactor {
  override val loggerName = "FlowWorker %02d:".format(num)

  def receive = {
    case BadDatagram(date, sender) =>
    // FIXME count bad datagrams

    case SaveJob(sender, flowPacket, prefixes, thruputPrefixes) =>

      /* Finders which are happy with the first result */
      def isInNetworks(flowAddr: InetAddress) = prefixes.exists(_.contains(flowAddr))
      def isInThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.exists(_.contains(flowAddr))

      /* Filters to get a list of prefixes that match */
      def findNetworks(flowAddr: InetAddress) = prefixes.filter(_.contains(flowAddr))
      def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

      val it1 = flowPacket.flows.iterator
      while (it1.hasNext) it1.next() match {
        case tmpl: cflow.Template =>
        /* Handle NetFlowData */
        case flow: NetFlowData[_] =>
          var ourFlow = false

          val srcNetworks = findNetworks(flow.srcAddress)
          val dstNetworks = findNetworks(flow.dstAddress)

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

          /*
          // thruput - in
          it3 = findThruputNetworks(flow.srcAddress).iterator
          while (it3.hasNext) {
            val prefix = it3.next()
            thruput(sender, flow, prefix, flow.dstAddress)
          }

          // thruput - out
          it3 = findThruputNetworks(flow.dstAddress).iterator
          while (it3.hasNext) {
            val prefix = it3.next()
            thruput(sender, flow, prefix, flow.srcAddress)
          }
          */

          if (!ourFlow) debug("Ignoring Flow: %s", flow)
        case _ =>
      }

      val flowSeq = flowPacket match {
        case a: cflow.NetFlowV5Packet => ", flowSeq: " + a.flowSequence
        case a: cflow.NetFlowV6Packet => ", flowSeq: " + a.flowSequence
        case a: cflow.NetFlowV7Packet => ", flowSeq: " + a.flowSequence
        case a: cflow.NetFlowV9Packet => ", flowSeq: " + a.flowSequence
        //case a: cflow.NetFlowV10Packet => ", flowSeq: " + a.flowSequence // FIXME netflow 10
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
      backend.countDatagram(new DateTime, sender, flowPacket.version, flowPacket.flows.length)
      val it2 = recvdFlows.iterator
      while (it2.hasNext) {
        val rcvd = it2.next()
        backend.countDatagram(new DateTime, sender, rcvd._1, rcvd._2.length)
      }
  }

  // Handle NetFlowData
  def save(flowPacket: FlowPacket, flow: NetFlowData[_], localAddress: InetAddress, direction: TrafficType.Value, prefix: InetPrefix) {
    val dir = direction.toString
    val ip = localAddress.getHostAddress
    val port = flow.dstPort
    //val port = if (direction == 'in) flow.dstPort else flow.srcPort

    val date = flowPacket.date
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
    val ipRow = Row(flow.bytes, flow.pkts) & Direction(direction)

    // Account per Sender with Protocols
    if (prefix.accountPerIPDetails) {
      ipRow & Protocol(flow.proto) & Port(port)
    }

    val pfx = prefix.prefix.toString

    // Account per Sender and Network
    account("bytes:" + dir + ":" + pfx, flow.bytes)
    account("pkts:" + dir + ":" + pfx, flow.pkts)

    // Account per Sender and Network with Protocols
    if (prefix.accountPerIPDetails) {
      account("bytes:" + dir + ":" + pfx + ":proto:" + prot, flow.bytes)
      account("pkts:" + dir + ":" + pfx + ":proto:" + prot, flow.pkts)

      account("bytes:" + dir + ":" + pfx + ":port:" + port, flow.bytes)
      account("pkts:" + dir + ":" + pfx + ":port:" + port, flow.pkts)
    }

    if (prefix.accountPerIP) {
      // Account per Sender and IP
      account("bytes:" + dir + ":" + ip, flow.bytes)
      account("pkts:" + dir + ":" + ip, flow.pkts)

      // Account per Sender and IP with Protocols
      if (prefix.accountPerIPDetails) {
        account("bytes:" + dir + ":" + ip + ":proto:" + prot, flow.bytes)
        account("pkts:" + dir + ":" + ip + ":proto:" + prot, flow.pkts)

        account("bytes:" + dir + ":" + ip + ":port:" + port, flow.bytes)
        account("pkts:" + dir + ":" + ip + ":port:" + port, flow.pkts)
      }
    }
  }
}