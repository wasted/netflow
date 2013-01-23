package io.netflow.actors

import io.netflow.flows._
import io.netflow.backends._
import io.netflow.netty._
import io.wasted.util._

import java.net.{ InetAddress, InetSocketAddress }

import org.joda.time.DateTime
import akka.actor._
import scala.util.{ Try, Success, Failure }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class SenderActor(sender: InetSocketAddress, protected val backend: Storage) extends Actor with Thruput with Logger {
  override protected def loggerName = sender.getAddress.getHostAddress + "/" + sender.getPort
  protected var thruputPrefixes: List[InetPrefix] = List()
  private var senderPrefixes: List[InetPrefix] = List()

  private val accountPerIP = false
  private val accountPerIPProto = false
  private var cancellable = Shutdown.schedule()

  private var counters = Map[(String, String), Long]()
  private def hincrBy(str1: String, str2: String, inc: Long) =
    counters ++= Map((str1, str2) -> (counters.get((str1, str2)).getOrElse(0L) + inc))

  def receive = {
    case Shutdown =>
      io.netflow.Service.removeActorFor(sender)
      backend.stop()
      context.stop(self)
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
    def schedule() = context.system.scheduler.scheduleOnce(5.minutes, self, Shutdown)
    def avoid() {
      cancellable.cancel
      cancellable = schedule()
    }
  }

  private case object Flush {
    def schedule() = context.system.scheduler.scheduleOnce(Storage.flushInterval.seconds, self, Flush)
    var lastPoll = new DateTime().minusSeconds(Storage.pollInterval + 1)

    def action() {
      if (counters.size > 0) {
        backend.save(counters, sender)
        counters = Map()
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
  Flush.action()

  private def findNetworks(flowAddr: InetAddress) = senderPrefixes.filter(_.contains(flowAddr))
  private def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

  private def save(flowPacket: FlowPacket): Unit = {
    val it1 = flowPacket.flows.iterator
    while (it1.hasNext) it1.next() match {
      case tmpl: cflow.Template =>
      // TODO Report it through thruput?

      /* Handle NetFlowData */
      case flow: NetFlowData[_] =>
        var ourFlow = false

        // src - in
        var it3 = findNetworks(flow.srcAddress).iterator
        while (it3.hasNext) {
          val prefix = it3.next
          ourFlow = true
          save(flowPacket, flow, flow.srcAddress, 'in, prefix.toString)
        }

        // dst - out
        it3 = findNetworks(flow.dstAddress).iterator
        while (it3.hasNext) {
          val prefix = it3.next
          ourFlow = true
          save(flowPacket, flow, flow.dstAddress, 'out, prefix.toString)
        }

        // thruput - in
        it3 = findThruputNetworks(flow.srcAddress).iterator
        while (it3.hasNext) {
          val prefix = it3.next
          thruput(sender, flow, prefix, flow.dstAddress)
        }

        // thruput - out
        it3 = findThruputNetworks(flow.dstAddress).iterator
        while (it3.hasNext) {
          val prefix = it3.next
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
  def save(flowPacket: FlowPacket, flow: NetFlowData[_], localAddress: InetAddress, direction: Symbol, prefix: String) {
    val dir = direction.name
    val ip = localAddress.getHostAddress
    val prot = flow.proto

    val date = flowPacket.date
    val year = date.getYear.toString
    val month = "%02d".format(date.getMonthOfYear)
    val day = "%02d".format(date.getDayOfMonth)
    val hour = "%02d".format(date.getHourOfDay)
    val minute = "%02d".format(date.getMinuteOfHour)

    def account(prefix: String, value: Long) {
      hincrBy(prefix + ":years", year, value)
      hincrBy(prefix + ":" + "year", month, value)
      hincrBy(prefix + ":" + year + month, day, value)
      hincrBy(prefix + ":" + year + month + day, hour, value)
      hincrBy(prefix + ":" + year + month + day + "-" + hour, minute, value)
    }

    // Account per Sender
    account("bytes:" + dir, flow.bytes)
    account("pkts:" + dir, flow.pkts)

    // Account per Sender with Protocols
    if (accountPerIPProto) {
      account("bytes:" + dir + ":" + prot, flow.bytes)
      account("pkts:" + dir + ":" + prot, flow.pkts)
    }

    // Account per Sender and Network
    account("bytes:" + dir + ":" + prefix, flow.bytes)
    account("pkts:" + dir + ":" + prefix, flow.pkts)

    // Account per Sender and Network with Protocols
    if (accountPerIPProto) {
      account("bytes:" + dir + ":" + prefix + ":" + prot, flow.bytes)
      account("pkts:" + dir + ":" + prefix + ":" + prot, flow.pkts)
    }

    if (accountPerIP) {
      // Account per Sender and IP
      account("bytes:" + dir + ":" + ip, flow.bytes)
      account("pkts:" + dir + ":" + ip, flow.pkts)

      // Account per Sender and IP with Protocols
      if (accountPerIPProto) {
        account("bytes:" + dir + ":" + ip + ":" + prot, flow.bytes)
        account("pkts:" + dir + ":" + ip + ":" + prot, flow.pkts)
      }
    }
  }
}
