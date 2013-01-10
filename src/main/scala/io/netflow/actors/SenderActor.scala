package io.netflow.actors

import io.netflow.flows._
import io.netflow.backends._
import io.wasted.util._

import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }

import io.netty.buffer._
import io.netty.channel.socket.DatagramPacket

import org.joda.time.DateTime
import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

private[netflow] class SenderActor(sender: InetSocketAddress, protected val backend: Storage) extends Actor with Thruput with Logger {
  override protected def loggerName = sender.getAddress.getHostAddress + "/" + sender.getPort
  protected var thruputPrefixes: List[InetPrefix] = List()
  private var senderPrefixes: List[InetPrefix] = List()
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
      backend.stop
      context.stop(self)
    case msg: DatagramPacket =>
      Shutdown.avoid()
      handleSFlow(sender, msg.data) orElse handleCisco(sender, msg.data) match {
        case Some(flowPacket) => save(flowPacket)
        case None =>
          debug("Unsupported FlowPacket received")
          io.netflow.netty.TrafficHandler.unsupportedPacket(sender)
          backend.countDatagram(new DateTime, sender, "bad")
      }
    case Flush =>
      Flush.action()
  }
  Flush.action()

  private def findNetworks(flowAddr: InetAddress) = senderPrefixes.filter(_.contains(flowAddr))
  private def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

  private def handleSFlow(sender: InetSocketAddress, buf: ByteBuf): Option[FlowPacket] = {
    if (buf.readableBytes < 28) return None
    Tryo(buf.getLong(0)) match {
      case Some(3) => None // sFlow 3
      case Some(4) => None // sFlow 4
      case Some(5) => Tryo(sflow.SFlowV5Packet(sender, buf))
      case _ => None
    }
  }

  private def handleCisco(sender: InetSocketAddress, buf: ByteBuf): Option[FlowPacket] =
    Tryo(buf.getUnsignedShort(0)) match {
      case Some(v) if v == 1 || v == 5 || v == 6 || v == 7 => Tryo(cisco.LegacyFlowPacket(sender, buf))
      case Some(v) if v == 9 || v == 10 => Tryo(cisco.TemplateFlowPacket(sender, buf))
      case _ => None
    }

  private def save(flowPacket: FlowPacket): Unit = {
    flowPacket.flows foreach {
      case tmpl: cisco.Template =>
        backend.save(tmpl)

      /* Handle IPFlowData */
      case flow: IPFlowData =>
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
          thruput(sender, prefix, flow.dstAddress, flow)
        }

        // thruput - out
        findThruputNetworks(flow.dstAddress) foreach { prefix =>
          thruput(sender, prefix, flow.srcAddress, flow)
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

    // log an elaborate string to loglevel info describing this packet.
    // Warning: can produce huge amounts of logs if written to disk.
    info(flowPacket.version + " from " + flowPacket.senderIP + "/" + flowPacket.senderPort + " (" + recvdFlowsStr + ")")

    // count them to database
    backend.countDatagram(new DateTime, sender, flowPacket.version, flowPacket.flows.length)
    recvdFlows foreach { rcvd =>
      backend.countDatagram(new DateTime, sender, rcvd._1, rcvd._2.length)
    }
  }

  // Handle invalid Flows
  def save(flowPacket: FlowPacket, flow: IPFlowData) {
  }

  // Handle valid Flows

  def save(flowPacket: FlowPacket, flow: IPFlowData, localAddress: InetAddress, direction: Symbol, prefix: String) {
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
