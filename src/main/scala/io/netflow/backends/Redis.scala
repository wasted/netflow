package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._

import org.joda.time.DateTime
import redis.client._
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

import io.netty.util.CharsetUtil
import java.util.UUID
import java.net.{ InetAddress, InetSocketAddress }

private[netflow] class Redis(host: String, port: Int, accountPerIP: Boolean, accountPerIPProto: Boolean) extends Storage with Thruput {
  private val redisClient = new RedisClient(host, port)

  // Handle invalid Flows
  def save(flowPacket: FlowPacket, flow: FlowData) {
  }

  // Handle valid Flows

  def save(flowPacket: FlowPacket, flow: FlowData, localAddress: InetAddress, direction: Symbol, prefix: String) {
    val senderIP = flowPacket.senderIP
    val senderPort = flowPacket.senderPort

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
      redisClient.hincrby(prefix + ":years", year, value)
      redisClient.hincrby(prefix + ":" + "year", month, value)
      redisClient.hincrby(prefix + ":" + year + month, day, value)
      redisClient.hincrby(prefix + ":" + year + month + day, hour, value)
      redisClient.hincrby(prefix + ":" + year + month + day + "-" + hour, minute, value)
    }

    // Account per Sender
    account("netflow:" + senderIP + "/" + senderPort + ":bytes:" + dir, flow.bytes)
    account("netflow:" + senderIP + "/" + senderPort + ":pkts:" + dir, flow.pkts)

    // Account per Sender with Protocols
    if (accountPerIPProto) {
      account("netflow:" + senderIP + "/" + senderPort + ":bytes:" + dir + ":" + prot, flow.bytes)
      account("netflow:" + senderIP + "/" + senderPort + ":pkts:" + dir + ":" + prot, flow.pkts)
    }

    // Account per Sender and Network
    account("netflow:" + senderIP + "/" + senderPort + ":bytes:" + dir + ":" + prefix, flow.bytes)
    account("netflow:" + senderIP + "/" + senderPort + ":pkts:" + dir + ":" + prefix, flow.pkts)

    // Account per Sender and Network with Protocols
    if (accountPerIPProto) {
      account("netflow:" + senderIP + "/" + senderPort + ":bytes:" + dir + ":" + prefix + ":" + prot, flow.bytes)
      account("netflow:" + senderIP + "/" + senderPort + ":pkts:" + dir + ":" + prefix + ":" + prot, flow.pkts)
    }

    if (accountPerIP) {
      // Account per Sender and IP
      account("netflow:" + senderIP + "/" + senderPort + ":bytes:" + dir + ":" + ip, flow.bytes)
      account("netflow:" + senderIP + "/" + senderPort + ":pkts:" + dir + ":" + ip, flow.pkts)

      // Account per Sender and IP with Protocols
      if (accountPerIPProto) {
        account("netflow:" + senderIP + "/" + senderPort + ":bytes:" + dir + ":" + ip + ":" + prot, flow.bytes)
        account("netflow:" + senderIP + "/" + senderPort + ":pkts:" + dir + ":" + ip + ":" + prot, flow.pkts)
      }
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

  def save(tmpl: cisco.Template) {
    val (ip, port) = (tmpl.sender.getAddress.getHostAddress, tmpl.sender.getPort)
    redisClient.hmset("template:" + ip + "/" + port + ":" + tmpl.id, tmpl.objectMap)
  }

  def countDatagram(date: DateTime, sender: InetSocketAddress, bad: Boolean, flowsPassed: Int = 0) {
    val state = bad match { case true => "bad" case false => "good" }
    val senderAddr = sender.getAddress.getHostAddress + "/" + sender.getPort
    redisClient.hincrby("router:udp:" + senderAddr, state, 1)
    redisClient.hset("router:udp:" + senderAddr, "last", date.getMillis.toString)
  }

  def acceptFrom(sender: InetSocketAddress): Boolean = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisClient.sismember("senders", ip + "/" + port).data == 1
  }

  def getThruputPrefixes(sender: InetSocketAddress): List[InetPrefix] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisClient.smembers("thruput:" + ip + "/" + port).asStringList(CharsetUtil.UTF_8).toList flatMap (getPrefix)
  }

  def getThruputPlatform(id: String): Option[ThruputPlatform] = {
    Tryo(UUID.fromString(id)) match {
      case Some(uuid) =>
        //val map = redisClient.hgetall("thruput:" + id).asStringMap(CharsetUtil.UTF_8)
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
