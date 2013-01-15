package io.netflow.actors

import io.netflow.flows._
import io.netflow.backends._
import io.netflow.netty._
import io.wasted.util._

import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }

import io.netty.buffer._
import io.netty.channel.socket.DatagramPacket

import org.joda.time.DateTime
import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class SenderActor(sender: InetSocketAddress, protected val backend: Storage) extends Actor with Thruput with Logger {
  override protected def loggerName = sender.getAddress.getHostAddress + "/" + sender.getPort
  protected var thruputPrefixes: List[InetPrefix] = List()
  private var senderPrefixes: List[InetPrefix] = List()

  // this is a temporary cache which will be flushed
  private var templateCache: Map[Int, cflow.Template] = Map()

  private val accountPerIP = false
  private val accountPerIPProto = false
  private var cancellable = Shutdown.schedule()

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
        templateCache = Map()
        lastPoll = new DateTime
      }
      schedule()
    }
  }

  private var counters = Map[(String, String), Long]()
  private def hincrBy(str1: String, str2: String, inc: Long) =
    counters ++= Map((str1, str2) -> (counters.get((str1, str2)).getOrElse(0L) + inc))

  def receive = {
    case Shutdown =>
      io.netflow.Service.removeActorFor(sender)
      backend.stop()
      context.stop(self)
    case NetFlow(data: ByteBuf) =>
      Shutdown.avoid()
      handleCisco(data) match {
        case Success(flowPacket) => save(flowPacket)
        case Failure(e) =>
          info("Unsupported NetFlow Packet received: %s", e)
          backend.countDatagram(new DateTime, sender, "bad:netflow")
      }
      data.free()
    case SFlow(data: ByteBuf) =>
      Shutdown.avoid()
      handleSFlow(data) match {
        case Success(flowPacket) => save(flowPacket)
        case Failure(e) =>
          info("Unsupported sFlow Packet received: %s", e)
          backend.countDatagram(new DateTime, sender, "bad:sflow")
      }
      data.free()
    case Flush =>
      Flush.action()
  }
  Flush.action()

  private def findNetworks(flowAddr: InetAddress) = senderPrefixes.filter(_.contains(flowAddr))
  private def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

  private val unhandledException = Failure(new UnhandledFlowPacketException(sender))

  private def handleSFlow(buf: ByteBuf): Try[sflow.SFlowV5Packet] = {
    if (buf.readableBytes < 28) return unhandledException
    Tryo(buf.getLong(0)) match {
      case Some(3) => unhandledException // sFlow 3
      case Some(4) => unhandledException // sFlow 4
      case Some(5) => sflow.SFlowV5Packet(sender, buf)
      case _ => unhandledException
    }
  }

  private def handleCisco(buf: ByteBuf): Try[FlowPacket] =
    Tryo(buf.getUnsignedShort(0)) match {
      case Some(1) => cflow.NetFlowV1Packet(sender, buf)
      case Some(5) => cflow.NetFlowV5Packet(sender, buf)
      case Some(6) => cflow.NetFlowV6Packet(sender, buf)
      case Some(7) => cflow.NetFlowV7Packet(sender, buf)
      case Some(9) => cflow.NetFlowV9Packet(sender, buf)
      case Some(10) => cflow.NetFlowV10Packet(sender, buf)
      case _ => unhandledException
    }

  private def save(flowPacket: FlowPacket): Unit = {
    flowPacket.flows foreach {
      case tmpl: cflow.Template =>
        templateCache ++= Map(tmpl.id -> tmpl)
        backend.save(tmpl)

      /* Handle NetFlowData */
      case flow: NetFlowData[_] =>
        var ourFlow = false

        // src - in
        findNetworks(flow.srcAddress) foreach { prefix =>
          ourFlow = true
          save(flowPacket, flow, flow.srcAddress, 'in, prefix.toString)
        }

        // dst - out
        findNetworks(flow.dstAddress) foreach { prefix =>
          ourFlow = true
          save(flowPacket, flow, flow.dstAddress, 'out, prefix.toString)
        }

        // thruput - in
        findThruputNetworks(flow.srcAddress) foreach { prefix =>
          thruput(sender, flow, prefix, flow.dstAddress)
        }

        // thruput - out
        findThruputNetworks(flow.dstAddress) foreach { prefix =>
          thruput(sender, flow, prefix, flow.srcAddress)
        }

        if (!ourFlow) { // invalid flow
          debug("Ignoring Flow: %s", flow)
          save(flowPacket, flow)
        }
      case _ =>
    }

    val recvdFlows = flowPacket.flows.groupBy(_.version)

    val recvdFlowsStr = List(flowPacket.flows.length + "/" + flowPacket.count + " flows passed") ++
      recvdFlows.map(fc => if (fc._2.length == 1) fc._1 else fc._1 + ": " + fc._2.length) mkString (", ")

    val flowSeq = flowPacket match {
      case a: cflow.NetFlowV5Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV6Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV7Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV9Packet => ", flowSeq: " + a.flowSequence
      case a: cflow.NetFlowV10Packet => ", flowSeq: " + a.flowSequence
      case _ => ""
    }

    // log an elaborate string to loglevel info describing this packet.
    // Warning: can produce huge amounts of logs if written to disk.
    val debugStr = flowPacket.version + " from " + flowPacket.senderIP + "/" + flowPacket.senderPort +
      " (" + recvdFlowsStr + ", length: " + flowPacket.length + flowSeq + ")"

    // Sophisticated log-level hacking :<
    if (flowPacket.count != flowPacket.flows.length) error(debugStr)
    else if (debugStr.matches("Template")) info(debugStr) else debug(debugStr)

    // count them to database
    backend.countDatagram(new DateTime, sender, flowPacket.version, flowPacket.flows.length)
    recvdFlows foreach { rcvd =>
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
