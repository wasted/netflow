package io.netflow.actors

import java.net.{ InetAddress, InetSocketAddress }

import com.websudos.phantom.Implicits._
import io.netflow.flows._
import io.netflow.lib._
import io.netflow.timeseries._
import io.wasted.util._
import org.joda.time.DateTime

private case class BadDatagram(date: DateTime, sender: InetAddress)

private case class SaveJob(
  sender: InetSocketAddress,
  flowPacket: FlowPacket,
  prefixes: List[InetPrefix],
  thruputPrefixes: List[InetPrefix])

private[netflow] class FlowWorker(num: Int) extends Wactor {
  override val loggerName = "FlowWorker %02d:".format(num)

  def receive = {
    case BadDatagram(date, sender) =>
    // FIXME count bad datagrams

    case SaveJob(sender, flowPacket, prefixes, thruputPrefixes) =>
      var batch = new CounterBatchStatement()

      /* Filters to get a list of prefixes that match */
      def findNetworks(flowAddr: InetAddress) = prefixes.filter(_.contains(flowAddr))
      def findThruputNetworks(flow: NetFlowData[_]) =
        thruputPrefixes.filter(x => x.contains(flow.srcAddress) || x.contains(flow.dstAddress))

      flowPacket.flows foreach {
        case tmpl: cflow.Template =>
        // FIXME maybe add thruput notification
        case flow: NetFlowData[_] =>
          var ourFlow = false

          val srcNetworks = findNetworks(flow.srcAddress)
          val dstNetworks = findNetworks(flow.dstAddress)

          // src - out
          srcNetworks foreach { prefix =>
            ourFlow = true
            // If it is *NOT* *to* another network we monitor
            val trafficType = if (dstNetworks.isEmpty) TrafficType.Outbound else TrafficType.OutboundLocal
            batch = add(batch, flowPacket, flow, flow.srcAddress, trafficType, prefix)
          }

          // dst - in
          dstNetworks foreach { prefix =>
            ourFlow = true
            // If it is *NOT* *to* another network we monitor
            val trafficType = if (srcNetworks.isEmpty) TrafficType.Inbound else TrafficType.InboundLocal
            batch = add(batch, flowPacket, flow, flow.dstAddress, trafficType, prefix)
          }

          // thruput
          findThruputNetworks(flow) foreach { prefix =>
            //thruput(sender, flow, prefix, flow.dstAddress)
          }

          if (!ourFlow) debug("Ignoring Flow: %s", flow)
        case _ =>
      }

      // execute the batch
      batch.future()

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

      // save this record
      NetFlowStats.insert
        .value(_.id, flowPacket.id)
        .value(_.date, DateTime.now)
        .value(_.sender, sender.getAddress)
        .value(_.port, sender.getPort)
        .value(_.version, flowPacket.version)
        .value(_.flows, flowPacket.flows.length)
        .value(_.bytes, flowPacket.length).future()
  }

  // Handle NetFlowData
  def add(batch: CounterBatchStatement, flowPacket: FlowPacket, flow: NetFlowData[_], localAddress: InetAddress, direction: TrafficType.Value, prefix: InetPrefix): CounterBatchStatement = {
    val date = flowPacket.timestamp
    val year = date.getYear.toString
    val month = "%02d".format(date.getMonthOfYear)
    val day = "%02d".format(date.getDayOfMonth)
    val hour = "%02d".format(date.getHourOfDay)
    val minute = "%02d".format(date.getMinuteOfHour)
    val pfx = prefix.prefix.getHostAddress
    val keys = List[String](
      pfx + ":" + year,
      pfx + ":" + year + "/" + month,
      pfx + ":" + year + "/" + month + "/" + day,
      pfx + ":" + year + "/" + month + "/" + day + "-" + hour,
      pfx + ":" + year + "/" + month + "/" + day + "-" + hour + ":" + minute)

    var editBatch = batch
    keys.foreach { key =>
      // first with all fields
      editBatch = editBatch.add(NetFlowSeries.update
        .where(_.date eqs key)
        .and(_.direction eqs direction.toString)
        .and(_.proto eqs flow.proto)
        .and(_.srcPort eqs flow.srcPort)
        .and(_.dstPort eqs flow.dstPort)
        .and(_.src eqs flow.srcAddress.getHostAddress)
        .and(_.dst eqs flow.dstAddress.getHostAddress)
        .and(_.srcAS eqs flow.srcAS.getOrElse(-1)) // minus one for cassandra
        .and(_.dstAS eqs flow.dstAS.getOrElse(-1)) // minus one for cassandra
        .modify(_.bytes increment flow.bytes)
        .and(_.pkts increment flow.pkts))

      // then without proto
      editBatch = editBatch.add(NetFlowSeries.update
        .where(_.date eqs key)
        .and(_.direction eqs direction.toString)
        .and(_.proto eqs -1)
        .and(_.srcPort eqs flow.srcPort)
        .and(_.dstPort eqs flow.dstPort)
        .and(_.src eqs flow.srcAddress.getHostAddress)
        .and(_.dst eqs flow.dstAddress.getHostAddress)
        .and(_.srcAS eqs flow.srcAS.getOrElse(-1)) // minus one for cassandra
        .and(_.dstAS eqs flow.dstAS.getOrElse(-1)) // minus one for cassandra
        .modify(_.bytes increment flow.bytes)
        .and(_.pkts increment flow.pkts))

      // then without ports
      editBatch = editBatch.add(NetFlowSeries.update
        .where(_.date eqs key)
        .and(_.direction eqs direction.toString)
        .and(_.proto eqs -1)
        .and(_.srcPort eqs -1)
        .and(_.dstPort eqs -1)
        .and(_.src eqs flow.srcAddress.getHostAddress)
        .and(_.dst eqs flow.dstAddress.getHostAddress)
        .and(_.srcAS eqs flow.srcAS.getOrElse(-1)) // minus one for cassandra
        .and(_.dstAS eqs flow.dstAS.getOrElse(-1)) // minus one for cassandra
        .modify(_.bytes increment flow.bytes)
        .and(_.pkts increment flow.pkts))

      // then without AS
      editBatch = editBatch.add(NetFlowSeries.update
        .where(_.date eqs key)
        .and(_.direction eqs direction.toString)
        .and(_.proto eqs -1)
        .and(_.srcPort eqs -1)
        .and(_.dstPort eqs -1)
        .and(_.src eqs flow.srcAddress.getHostAddress)
        .and(_.dst eqs flow.dstAddress.getHostAddress)
        .and(_.srcAS eqs -1)
        .and(_.dstAS eqs -1)
        .modify(_.bytes increment flow.bytes)
        .and(_.pkts increment flow.pkts))

      // then without ips
      editBatch = editBatch.add(NetFlowSeries.update
        .where(_.date eqs key)
        .and(_.direction eqs direction.toString)
        .and(_.proto eqs -1)
        .and(_.srcPort eqs -1)
        .and(_.dstPort eqs -1)
        .and(_.src eqs pfx)
        .and(_.dst eqs pfx)
        .and(_.srcAS eqs -1)
        .and(_.dstAS eqs -1)
        .modify(_.bytes increment flow.bytes)
        .and(_.pkts increment flow.pkts))
    }
    editBatch
  }
}
