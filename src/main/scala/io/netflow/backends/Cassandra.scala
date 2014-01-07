package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._

import org.joda.time.DateTime
import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

import java.util.UUID
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicLong
import com.datastax.driver.core._
import io.netflow.lib.{ NetFlowInetPrefix, Storage, NodeConfig }

private[netflow] class Cassandra extends Storage {
  def keyspace = NodeConfig.values.cassandra
  private val client = {
    val hosts = NodeConfig.values.cassandra.hosts.mkString(",")
    debug(s"Opening new connection to $hosts")
    Cluster.builder().addContactPoint(hosts).build()
  }

  private val metadata = {
    val md = client.getMetadata
    md.getAllHosts.asScala.foreach { host =>
      debug("Datacenter: %s, Host: %s, Rack: %s", host.getDatacenter, host.getAddress, host.getRack)
    }
    md
  }

  private val structure = {
    val columns = for {
      accType <- List("bytes", "pkts")
      direction <- List("", "_in", "_out")
      protocol <- "" :: (0 to 255).toList
    } yield s"$accType$direction$protocol counter"
    columns.mkString(", ")
  }

  private val session = {
    val session = client.connect()
    session.execute("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", keyspace)
    session.execute("USE %s;", keyspace)
    session.execute("CREATE TABLE prefixes (id uuid, name text, addrs set<inet>, PRIMARY KEY (id));")
    //session.execute("CREATE INDEX ON %s.prefixes (addrs);") not supported yet
    session.execute("CREATE TABLE cflow_inet (addr inet, time ascii, collector inet, port int, %s, PRIMARY KEY (addr, time));", structure)
    session.execute("CREATE TABLE cflow_prefix (id uuid, time ascii, %s, PRIMARY KEY (id, time));", structure)
    session
  }

  def save(flowData: Map[(String, String), AtomicLong], sender: InetSocketAddress) {
    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    val prefix = "netflow:" + senderIP + "/" + senderPort

    flowData foreach {
      case ((hash, name), value) => redisConnection.hincrby(prefix + ":" + hash, name, value.get)
    }
  }

  def ciscoTemplateFields(sender: InetSocketAddress, id: Int): Option[HashMap[String, Int]] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    var fields = HashMap[String, Int]()
    redisConnection.hgetall("template:" + ip + "/" + port + ":" + id).asScala foreach { field =>
      Tryo(fields ++= Map(field._1 -> field._2.toInt))
    }
    if (fields.size == 0) None else Some(fields)
  }

  def save(tmpl: cflow.Template) {
    val (ip, port) = (tmpl.sender.getAddress.getHostAddress, tmpl.sender.getPort)
    val key = "template:" + ip + "/" + port + ":" + tmpl.id
    redisConnection.del(key)
    redisConnection.hmset(key, tmpl.objectMap.asJava)
  }

  def countDatagram(date: DateTime, sender: InetSocketAddress, kind: String, flowsPassed: Int = 0) {
    val senderAddr = sender.getAddress.getHostAddress + "/" + sender.getPort
    redisConnection.hincrby("stats:" + senderAddr, kind, 1)
    redisConnection.hset("stats:" + senderAddr, "last", date.getMillis.toString)
  }

  def acceptFrom(sender: InetSocketAddress): Option[InetSocketAddress] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    if (redisConnection.sismember("senders", ip + "/" + port)) return Some(sender)
    if (redisConnection.sismember("senders", ip + "/0")) return Some(new InetSocketAddress(sender.getAddress, 0))
    None
  }

  def getThruputPrefixes(sender: InetSocketAddress): List[NetFlowInetPrefix] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisConnection.smembers("thruput:" + ip + "/" + port).asScala.toList.flatMap(getPrefix)
  }

  def getThruputPlatform(id: String): Option[ThruputPlatform] = {
    Tryo(UUID.fromString(id)) match {
      case Some(uuid) =>
        val map = redisConnection.hgetall("thruput:" + id).asScala
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

  def getThruputRecipients(sender: InetSocketAddress, prefix: NetFlowInetPrefix): List[ThruputRecipient] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisConnection.smembers("thruput:" + ip + "/" + port + ":" + prefix.toString).asScala.toList flatMap { rcpt =>
      val split = rcpt.split(":", 2)
      split.length match {
        case 1 => getThruputPlatform(split(0)).map(pf => ThruputRecipient(pf))
        case 2 =>
          getThruputPlatform(split(0)) match {
            case Some(platform) if split(1).trim.length == 0 => // broadcast
              Some(ThruputRecipient(platform))
            case Some(platform) => // to user
              Some(ThruputRecipient(platform, Some(split(1))))
            case None => info("Thruput Platform " + split(0) + " could not be found"); None
          }
      }
    }
  }

  def getPrefixes(sender: InetSocketAddress): List[NetFlowInetPrefix] = {
    val (ip, port) = (sender.getAddress.getHostAddress, sender.getPort)
    redisConnection.smembers("sender:" + ip + "/" + port).asScala.toList.flatMap(getPrefix)
  }

  def stop() {
    client.shutdown()
  }
}

