package io.netflow.actors

import io.netflow.flows._
import io.netflow.backends._
import io.wasted.util._

import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicLong

import org.joda.time.DateTime
import scala.util.{ Success, Failure }
import scala.concurrent.duration._

object TrafficType extends Enumeration {
  val Inbound = Value("in")
  val Outbound = Value("out")
  val InboundLocal = Value("in:local")
  val OutboundLocal = Value("out:local")
}

class SenderActor(sender: InetSocketAddress, protected val backend: Storage) extends Wactor with ThruputSender {
  override protected def loggerName = sender.getAddress.getHostAddress + "/" + sender.getPort
  info("Starting for " + loggerName)
  implicit val wheelTimer = WheelTimer()
  private var senderPrefixes: List[NetFlowInetPrefix] = List()
  protected var thruputPrefixes: List[NetFlowInetPrefix] = List()

  private var cancellable = Shutdown.schedule()

  private var counters = scala.collection.concurrent.TrieMap[(String, String), AtomicLong]()
  private def hincrBy(str1: String, str2: String, inc: Long) = synchronized {
    val kv = (str1, str2)
    counters.get(kv) match {
      case Some(al) => al.addAndGet(inc)
      case None => counters ++= Map(kv -> new AtomicLong(inc))
    }
  }

  def receive = {
    case Shutdown =>
      io.netflow.Service.removeActorFor(sender)
      backend.stop()
      cflow.NetFlowV9Template.clear(sender)
      cflow.NetFlowV10Template.clear(sender)
      this ! Wactor.Die
    case tmpl: cflow.Template => backend.save(tmpl)
    case Success(fp: FlowPacket) =>
      Shutdown.avoid
      save(fp)
    case Failure(e) =>
      Shutdown.avoid
      info("Unsupported FlowPacket received: %s", e)
      backend.countDatagram(new DateTime, sender, "bad:flow")
    case Flush =>
      Flush.action()
  }

  private case object Shutdown {
    def schedule() = scheduleOnce(Shutdown, 5.minutes)
    def avoid() {
      cancellable.cancel
      cancellable = schedule()
    }
  }

  private case object Flush {
    def schedule() = scheduleOnce(Flush, Storage.flushInterval.seconds)
    var lastPoll = new DateTime().minusSeconds(Storage.pollInterval + 1)

    def action() {
      if (counters.size > 0) {
        backend.save(counters.toMap, sender)
        counters = scala.collection.concurrent.TrieMap()
      }
      val repoll = new DateTime().minusSeconds(Storage.pollInterval).isAfter(lastPoll)
      if (repoll) {
        thruputPrefixes = backend.getThruputPrefixes(sender)
        senderPrefixes = backend.getPrefixes(sender)
        lastPoll = new DateTime
      }
      schedule()
    }
  }
  this ! Flush

  private def findNetworks(flowAddr: InetAddress) = senderPrefixes.filter(_.contains(flowAddr))
  private def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

  private def save(flowPacket: FlowPacket): Unit = {
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

        if (!ourFlow) { // invalid flow
          debug("Ignoring Flow: %s", flow)
          save(flowPacket, flow)
        }
      case _ =>
    }

    val flowSeq = flowPacket match {
      case a: cflow.NetFlowV5Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV6Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV7Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV9Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV10Packet => ", flowSeq: " + a.flowSequence
      case _ => ""
    }

    val packetInfoStr = flowPacket.version.replaceAll("Packet", "-") + " length: " + flowPacket.length + flowSeq
    val passedFlowsStr = flowPacket.flows.length + "/" + flowPacket.count + " passed"

    var recvdFlows = flowPacket.flows.groupBy(_.version)
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

  // Handle invalid Flows
  def save(flowPacket: FlowPacket, flow: NetFlowData[_]) {
  }

  // Handle NetFlowData
  def save(flowPacket: FlowPacket, flow: NetFlowData[_], localAddress: InetAddress, direction: TrafficType.Value, prefix: NetFlowInetPrefix) {
    val dir = direction.toString
    val ip = localAddress.getHostAddress
    val prot = flow.proto
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
    account("bytes:" + dir, flow.bytes)
    account("pkts:" + dir, flow.pkts)

    // Account per Sender with Protocols
    if (prefix.accountPerIPDetails) {
      account("bytes:" + dir + ":proto:" + prot, flow.bytes)
      account("pkts:" + dir + ":proto:" + prot, flow.pkts)

      account("bytes:" + dir + ":port:" + port, flow.bytes)
      account("pkts:" + dir + ":port:" + port, flow.pkts)
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
