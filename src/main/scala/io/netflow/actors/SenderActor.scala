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
  private var ciscoTemplates: HashMap[Int, cisco.Template] = HashMap()

  def receive = {
    case msg: DatagramPacket => handleCisco(msg.remoteAddress, msg.data) //getOrElse
  }

  private def findNetworks(flowAddr: InetAddress) = senderPrefixes.filter(_.contains(flowAddr))
  private def findThruputNetworks(flowAddr: InetAddress) = thruputPrefixes.filter(_.contains(flowAddr))

  private def getCiscoTemplate(id: Int) = ciscoTemplates.get(id) match {
    case Some(tmpl: cisco.Template) =>
      Some(tmpl)
    case None =>
      backend.ciscoTemplateFields(sender, id) match {
        case Some(fields: HashMap[String, Int]) =>
          cisco.Template(sender, id, fields) match {
            case Some(tmpl: cisco.Template) =>
              ciscoTemplates ++= Map(tmpl.id -> tmpl)
              Some(tmpl)
            case None => None
          }
        case None => None
      }
  }

  private def handleCisco(sender: InetSocketAddress, buf: ByteBuf): Option[FlowPacket] =
    Try(buf.getUnsignedShort(0)) match {
      case Failure(e) =>
        debug("%s", e)
        io.netflow.netty.TrafficHandler.unsupportedPacket(sender)
        backend.countDatagram(new DateTime, sender, true)
        None

      case Success(version) if version == 5 =>
        Tryo(new cisco.V5FlowPacket(sender, buf)) map { flowPacket =>
          backend.countDatagram(new DateTime, sender, false)
          save(flowPacket)
          flowPacket
        }

      case Success(version) if version == 9 || version == 10 =>
        val flowPacket = new cisco.V9FlowPacket(sender, buf, getCiscoTemplate)
        backend.countDatagram(new DateTime, sender, false)
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
          backend.save(flowPacket, flow, flow.srcAddress, 'in, prefix.toString())
        }

        // dst - out
        findNetworks(flow.dstAddress) foreach { prefix =>
          ourFlow = true
          backend.save(flowPacket, flow, flow.dstAddress, 'out, prefix.toString())
        }

        if (!ourFlow) { // invalid flow
          debug("Ignoring Flow: %s", flow)
          backend.save(flowPacket, flow)
        }
      case _ =>
    }
  }

}
