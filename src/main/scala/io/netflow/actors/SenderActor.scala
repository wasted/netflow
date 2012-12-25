package io.netflow.actors

import io.netflow.flows._
import io.netflow.backends._
import io.wasted.util._

import scala.collection.immutable.HashMap
import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }

import io.netty.buffer._
import io.netty.channel.socket.DatagramPacket

import org.joda.time.DateTime
import akka.actor._

private[netflow] class SenderActor(sender: InetSocketAddress, backend: Storage) extends Actor with Thruput with Logger {
  override protected def loggerName = sender.getAddress.getHostAddress + "/" + sender.getPort
  private var thruputPrefixes: List[InetPrefix] = backend.getThruputPrefixes(sender)
  private var senderPrefixes: List[InetPrefix] = backend.getPrefixes(sender)
  private var ciscoTemplates = HashMap[Int, cisco.Template]()
  private val accountPerIP = false
  private val accountPerIPProto = false

  private case object Flush
  def receive = {
    case msg: DatagramPacket => handleCisco(msg.remoteAddress, msg.data) //getOrElse
    case Flush =>
      backend.save(counters, sender)
      counters = HashMap()
  }

  private def findNetworks(flowAddr: InetAddress) = senderPrefixes.filter(_.contains(flowAddr))
  private def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

  private def handleCisco(sender: InetSocketAddress, buf: ByteBuf): Option[FlowPacket] =
    Try(buf.getUnsignedShort(0)) match {
      case Failure(e) =>
        debug("%s", e)
        io.netflow.netty.TrafficHandler.unsupportedPacket(sender)
        backend.countDatagram(new DateTime, sender, true)
        None

      case Success(version) if version == 5 =>
        Tryo(new cisco.V5FlowPacket(sender, buf)) map { flowPacket =>
          backend.countDatagram(new DateTime, sender, false, flowPacket.flows.length)
          save(flowPacket)
          flowPacket
        }

      case Success(version) if version == 9 || version == 10 =>
        val flowPacket = new cisco.V9FlowPacket(sender, buf)
        backend.countDatagram(new DateTime, sender, false, flowPacket.flows.length)
        save(flowPacket)
        Some(flowPacket)

      case Success(version) =>
        info("Unsupported NetFlow version " + version + " received from " + sender.getAddress.getHostAddress + "/" + sender.getPort)
        backend.countDatagram(new DateTime, sender, true)
        None
    }

  private def save(flowPacket: FlowPacket): Unit = {
    //thruput(flowPacket)
    flowPacket.flows foreach {
      case tmpl: cisco.Template =>
        ciscoTemplates ++= Map(tmpl.id -> tmpl)
        backend.save(tmpl)

      /* Handle FlowData */
      case flow: FlowData =>
        var ourFlow = false

        // src - in
        findNetworks(flow.srcAddress) foreach { prefix =>
          ourFlow = true
          save(flowPacket, flow, flow.srcAddress, 'in, prefix.toString())
        }

        // dst - out
        findNetworks(flow.dstAddress) foreach { prefix =>
          ourFlow = true
          save(flowPacket, flow, flow.dstAddress, 'out, prefix.toString())
        }

        if (!ourFlow) { // invalid flow
          debug("Ignoring Flow: %s", flow)
          save(flowPacket, flow)
        }
      case _ =>
    }
  }

  private var counters = HashMap[(String, String), Long]()
  private def hincrBy(str1: String, str2: String, inc: Long) =
    counters ++= Map((str1, str2) -> (counters.get((str1, str2)).getOrElse(0L) + inc))

  // Handle invalid Flows
  def save(flowPacket: FlowPacket, flow: FlowData) {
  }

  // Handle valid Flows

  def save(flowPacket: FlowPacket, flow: FlowData, localAddress: InetAddress, direction: Symbol, prefix: String) {
    val dir = direction.name
    val ip = localAddress.getHostAddress
    val prot = flow.proto

    val date = flowPacket.date
    val year = date.getYear.toString
    val month = date.getMonthOfYear.toString
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
