package io.netflow.backends

import io.netflow.NetFlowConfig
import io.netflow.flows._
import io.wasted.util._

import scala.collection.immutable.HashMap
import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicLong

import org.joda.time.DateTime

private[netflow] case class NetFlowInetPrefix(prefix: InetPrefix, accountPerIP: Boolean, accountPerIPDetails: Boolean) {
  def contains(addr: InetAddress) = prefix.contains(addr)
}

private[netflow] object Storage extends Logger {

  def pollInterval = NetFlowConfig.values.pollInterval
  def flushInterval = NetFlowConfig.values.flushInterval

  private val redisPool = new PooledResource[Redis](() => new Redis, NetFlowConfig.values.redis.maxConns)

  def start(): Option[Storage] = NetFlowConfig.values.storage match {
    case "redis" => redisPool.get()
    case "cassandra" => Some(Cassandra)
  }
  def stop(c: Storage): Unit = c match {
    case a: Redis => redisPool.release(a)
    case Cassandra => // we do not need to release anything
  }
}

private[netflow] trait Storage extends Logger {
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

