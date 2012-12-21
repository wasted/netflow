package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._
import io.wasted.util.http._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.HashMap
import scala.util.{ Try, Success, Failure }

import org.joda.time.DateTime

import java.net.{ InetAddress, InetSocketAddress }

private[netflow] case class StorageConnection(conn: Any) {
  def as[T]() = conn.asInstanceOf[T]
  def tryAs[T]() = Tryo(as[T])
  def run[ST, RT](f: (ST) => RT): RT = f(as[ST])
}

private[netflow] trait Storage extends Logger {
  def start(): StorageConnection
  def stop(implicit sc: StorageConnection): Unit

  // Save invalid Flows
  protected def save(flowPacket: FlowPacket, flow: FlowData)(implicit sc: StorageConnection): Unit

  // Save valid Flows
  protected def save(flowPacket: FlowPacket, flow: FlowData, localAddress: InetAddress, direction: Symbol, prefix: String)(implicit sc: StorageConnection): Unit

  protected def save(tmpl: cisco.Template)(implicit sc: StorageConnection): Unit

  def ciscoTemplateFields(sender: InetSocketAddress, id: Int)(implicit sc: StorageConnection): Option[HashMap[String, Int]]

  // Validate the sender
  protected var senders: Vector[(InetAddress, Int)] = Vector()
  def senderExists(sender: InetSocketAddress)(implicit sc: StorageConnection): Boolean
  def acceptFrom(sender: InetSocketAddress)(implicit sc: StorageConnection): Boolean = {
    val obj = (sender.getAddress, sender.getPort)
    if (senders.exists(_ == obj)) return true
    if (senderExists(sender)) {
      senders +:= obj
      return true
    }
    false
  }

  // Count datagrams from this sender
  def countDatagram(date: DateTime, sender: InetSocketAddress, bad: Boolean = false)(implicit sc: StorageConnection): Unit

  protected var prefixes: Vector[InetPrefix] = Vector()

  protected def getPrefix(network: String) = prefixes.find(_.toString == network) match {
    case Some(prefix) => Some(prefix)
    case _ =>
      val split = network.split("/")
      if (split.length == 2) {
        val prefix = split.head
        val prefixLen = split.last
        Try(InetPrefix(java.net.InetAddress.getByName(prefix), prefixLen.toInt)) match {
          case Success(a: InetPrefix) =>
            prefixes +:= a
            Some(a)
          case Failure(f) =>
            warn("Unable to parse prefix: " + network); None
        }
      } else { warn("Unable to parse prefix: " + network); None }
  }

  protected def getPrefixes(sender: InetSocketAddress)(implicit sc: StorageConnection): List[InetPrefix]
  protected def getThruputRecipients(sender: InetSocketAddress, prefix: InetPrefix)(implicit sc: StorageConnection): List[ThruputRecipient]
  protected def getThruputPrefixes(sender: InetSocketAddress)(implicit sc: StorageConnection): List[InetPrefix]

}

