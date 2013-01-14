package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._

import org.joda.time.DateTime
import redis.client._
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

import io.netty.util.CharsetUtil
import java.util.UUID
import java.net.InetSocketAddress

class Redis(host: String, port: Int) extends Storage {
  private val redisClient = new RedisClient(host, port)

  def save(flowData: Map[(String, String), Long], sender: InetSocketAddress) {
    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    val prefix = "cflow:" + senderIP + "/" + senderPort

    flowData foreach {
      case ((hash, name), value) => redisClient.hincrby(prefix + ":" + hash, name, value)
    }
  }

  def ciscoTemplateFields(sender: InetSocketAddress, id: Int): Option[HashMap[String, Int]] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    var fields = HashMap[String, Int]()
    redisClient.hgetall("template:" + ip + "/" + port + ":" + id).asStringMap(CharsetUtil.UTF_8) foreach { field =>
      Tryo(fields ++= Map(field._1 -> field._2.toInt))
    }
    if (fields.size == 0) None else Some(fields)
  }

  def save(tmpl: cflow.Template) {
    val (ip, port) = (tmpl.sender.getAddress.getHostAddress, tmpl.sender.getPort)
    val key = "template:" + ip + "/" + port + ":" + tmpl.id
    redisClient.del(Array(key))
    redisClient.hmset(key, tmpl.objectMap)
  }

  def countDatagram(date: DateTime, sender: InetSocketAddress, kind: String, flowsPassed: Int = 0) {
    val senderAddr = sender.getAddress.getHostAddress + "/" + sender.getPort
    redisClient.hincrby("stats:" + senderAddr, kind, 1)
    redisClient.hset("stats:" + senderAddr, "last", date.getMillis.toString)
  }

  def acceptFrom(sender: InetSocketAddress): Option[InetSocketAddress] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    if (redisClient.sismember("senders", ip + "/" + port).data == 1) return Some(sender)
    if (redisClient.sismember("senders", ip + "/0").data == 1) return Some(new InetSocketAddress(sender.getAddress, 0))
    None
  }

  def getThruputPrefixes(sender: InetSocketAddress): List[InetPrefix] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisClient.smembers("thruput:" + ip + "/" + port).asStringList(CharsetUtil.UTF_8).toList flatMap (getPrefix)
  }

  def getThruputPlatform(id: String): Option[ThruputPlatform] = {
    Tryo(UUID.fromString(id)) match {
      case Some(uuid) =>
        val map = mapAsScalaMap(redisClient.hgetall("thruput:" + id).asStringMap(CharsetUtil.UTF_8))
        if (map.size == 0) return None
        for {
          url <- map.get("url")
          auth <- map.get("auth")
          sign <- map.get("sign")
          platform <- Tryo(ThruputPlatform(url, auth, sign))
        } yield platform
      case _ => None
    }
  }

  def getThruputRecipients(sender: InetSocketAddress, prefix: InetPrefix): List[ThruputRecipient] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisClient.smembers("thruput:" + ip + "/" + port + ":" + prefix.toString).asStringList(CharsetUtil.UTF_8).toList flatMap { rcpt =>
      rcpt.indexOf(":") match {
        case -1 =>
          getThruputPlatform(rcpt).map(pf => ThruputRecipient(pf))
        case splitAt =>
          val split = rcpt.splitAt(splitAt)
          getThruputPlatform(split._1) match {
            case Some(platform) if split._2.trim.length == 0 => // broadcast
              Some(ThruputRecipient(platform))
            case Some(platform) => // to user
              Some(ThruputRecipient(platform, Some(split._2)))
            case None => info("Thruput Platform " + split._1 + " could not be found"); None
          }
      }
    }
  }

  def getPrefixes(sender: InetSocketAddress): List[InetPrefix] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisClient.smembers("sender:" + ip + "/" + port).asStringList(CharsetUtil.UTF_8).toList flatMap (getPrefix)
  }

  def stop() {
    redisClient.close()
  }
}
