package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._

import scala.concurrent._
import scala.collection.immutable.HashMap
import scala.util.{ Try, Success, Failure }

import org.joda.time.DateTime

import java.net.{ InetAddress, InetSocketAddress }

private[netflow] trait Backend[A <: Storage with Thruput[A]] {
  this: A with Storage with Thruput[A] =>

  def start(): StorageConnection
  def stop(implicit sc: StorageConnection): Unit
  def ciscoTemplateFields(sender: InetSocketAddress, id: Int)(implicit sc: StorageConnection): Option[HashMap[String, Int]]
  def countDatagram(date: DateTime, sender: InetSocketAddress, bad: Boolean = false)(implicit sc: StorageConnection): Unit
  def acceptFrom(sender: InetSocketAddress)(implicit sc: StorageConnection): Boolean

  protected def findNetworks(prefixes: List[InetPrefix], flowAddr: InetAddress): List[InetPrefix] = {
    prefixes.filter(_.contains(flowAddr))
  }

  def save(flowPacket: FlowPacket)(implicit sc: StorageConnection): Unit = {
    val prefixes = getPrefixes(flowPacket.sender)
    thruput(flowPacket)
    flowRecursion(flowPacket, prefixes, flowPacket.flows)
  }

  protected def flowRecursion(flowPacket: FlowPacket, prefixes: List[InetPrefix], flows: List[Flow])(implicit sc: StorageConnection): Unit = if (flows.length > 0) {
    flows.head match {
      case tmpl: cisco.Template =>
        save(tmpl)

      /* Handle FlowData */
      case flow: FlowData =>
        var ourFlow = false
        val senderAddr = flowPacket.sender

        def findInPrefix(prefixes: List[InetPrefix], src: InetAddress, dir: Symbol): Unit = if (prefixes.length > 0) {
          val prefix = prefixes.head
          ourFlow = true
          save(flowPacket, flow, src, dir, prefix.toString)
          findInPrefix(prefixes.tail, src, dir)
        }

        // src - in
        findInPrefix(findNetworks(prefixes, flow.srcAddress), flow.srcAddress, 'in)

        // dst - out
        findInPrefix(findNetworks(prefixes, flow.dstAddress), flow.dstAddress, 'out)

        if (!ourFlow) { // invalid flow
          debug("Ignoring Flow: %s", flow)
          save(flowPacket, flow)
        }

      case _ =>
    }
    flowRecursion(flowPacket, prefixes, flows.tail)
  }
}

