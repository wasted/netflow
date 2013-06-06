package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._

import scala.collection.immutable.HashMap
import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicLong

import org.joda.time.DateTime

case class NetFlowInetPrefix(prefix: InetPrefix, accountPerIP: Boolean, accountPerIPDetails: Boolean) {
  def contains(addr: InetAddress) = prefix.contains(addr)
}

object Storage extends Logger {

  private val host = Config.getString("redis.host", "127.0.0.1")
  private val port = Config.getInt("redis.port", 6379)
  private val maxConns = Config.getInt("backend.maxConns", 100)
  val pollInterval = Config.getInt("backend.pollInterval", 10)
  val flushInterval = Config.getInt("backend.flushInterval", 5)

  private val newConnect = () => new Redis(host, port)

  private val pool = new PooledResource[Storage](newConnect, maxConns)
  def start(): Option[Storage] = pool.get()
  def stop(c: Storage) { pool.release(c) }
}

trait Storage extends Logger {
  def save(flowData: Map[(String, String), AtomicLong], sender: InetSocketAddress): Unit
  def save(template: cflow.Template): Unit
  def ciscoTemplateFields(sender: InetSocketAddress, id: Int): Option[HashMap[String, Int]]

  /**
   * Validate the sender and return another sender-Address if needed.
   */
  def acceptFrom(sender: InetSocketAddress): Option[InetSocketAddress]

  /**
   * Count DataGram from this sender
   *
   * @param kind Should be "good", "bad" or the flow type (e.g. "NetFlow v9")
   */
  def countDatagram(date: DateTime, sender: InetSocketAddress, kind: String, passedFlows: Int = 0): Unit

  def getThruputRecipients(sender: InetSocketAddress, prefix: NetFlowInetPrefix): List[ThruputRecipient]

  def getPrefixes(sender: InetSocketAddress): List[NetFlowInetPrefix]
  def getThruputPrefixes(sender: InetSocketAddress): List[NetFlowInetPrefix]

  protected def getPrefix(network: String) = {
    val split = network.split("/")
    if (split.length >= 2) {
      val prefix = split(0)
      val prefixLen = split(1)
      val accountPerIP = Tryo(split(2).toBoolean) getOrElse false
      val accountPerIPDetails = Tryo(split(3).toBoolean) getOrElse false
      Try(InetPrefix(java.net.InetAddress.getByName(prefix), prefixLen.toInt)) match {
        case Success(a: InetPrefix) => Some(NetFlowInetPrefix(a, accountPerIP, accountPerIPDetails))
        case Failure(f) => warn("Unable to parse prefix: " + network); None
      }
    } else { warn("Unable to parse prefix: " + network); None }
  }

  def stop()
}

