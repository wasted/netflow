package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._

import scala.collection.immutable.HashMap
import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }

import org.joda.time.DateTime

private[netflow] object Storage extends Logger {

  private val host = Config.getString("redis.host", "127.0.0.1")
  private val port = Config.getInt("redis.port", 6379)
  private val maxConns = Config.getInt("backend.maxConns", 100)
  private val newConnect = () => new Redis(host, port, false, false)

  private val pool = new PooledResource[Storage](newConnect, maxConns)
  def start(): Option[Storage] = pool.get()
  def stop(c: Storage) { pool.release(c) }
}

private[netflow] trait Storage extends Logger {
  // Save invalid Flows
  def save(flowPacket: FlowPacket, flow: FlowData): Unit

  // Save valid Flows
  def save(flowPacket: FlowPacket, flow: FlowData, localAddress: InetAddress, direction: Symbol, prefix: String): Unit

  def save(template: cisco.Template): Unit

  def ciscoTemplateFields(sender: InetSocketAddress, id: Int): Option[HashMap[String, Int]]

  // Validate the sender
  def acceptFrom(sender: InetSocketAddress): Boolean

  // Count DataGram from this sender
  def countDatagram(date: DateTime, sender: InetSocketAddress, bad: Boolean, passedFlows: Int = 0): Unit

  def getThruputRecipients(sender: InetSocketAddress, prefix: InetPrefix): List[ThruputRecipient]

  def getPrefixes(sender: InetSocketAddress): List[InetPrefix]
  def getThruputPrefixes(sender: InetSocketAddress): List[InetPrefix]

  protected def getPrefix(network: String) = {
    val split = network.split("/")
    if (split.length == 2) {
      val prefix = split.head
      val prefixLen = split.last
      Try(InetPrefix(java.net.InetAddress.getByName(prefix), prefixLen.toInt)) match {
        case Success(a: InetPrefix) => Some(a)
        case Failure(f) => warn("Unable to parse prefix: " + network); None
      }
    } else { warn("Unable to parse prefix: " + network); None }
  }

  def stop()
}

