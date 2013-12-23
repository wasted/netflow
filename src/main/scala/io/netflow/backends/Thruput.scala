package io.netflow.backends

import io.netflow.flows._
import io.wasted.util.http._

import java.net.{ InetSocketAddress, InetAddress }
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private[netflow] case class ThruputRecipient(platform: ThruputPlatform, toUser: Option[String] = None) {
  def auth = platform.auth
  def sign = platform.sign
  def url = platform.url
}

private[netflow] case class ThruputPlatform(urlStr: String, authStr: String, signStr: String) {
  val auth = UUID.fromString(authStr)
  val sign = UUID.fromString(signStr)
  val url = new java.net.URL(urlStr)
  def msg(fd: Flow[_], addr: InetAddress, toUser: List[String]): String = {
    val user = toUser.length match {
      case 0 => ""
      case _ => """, "to": { "user": ["""" + toUser.mkString("""", """") + """"] }"""
    }
    val ip = """, "ip": """" + addr.getHostAddress + """""""

    """{ "mime": "wasted/netflow", "body": """ + fd.json + ip + user + " }"
  }
}

private[netflow] object ThruputClientManager {
  private val thruputClients = new ConcurrentHashMap[ThruputPlatform, Thruput]()

  def get(tp: ThruputPlatform) = Option(thruputClients.get(tp)) match {
    case Some(tp) => Some(tp)
    case None =>
      val thruputClient = new Thruput(tp.url.toURI, tp.auth, tp.sign)
      thruputClient.connect
      thruputClients.put(tp, thruputClient)
      Some(thruputClient)
  }

  def shutdown(tp: ThruputPlatform): Unit = Option(thruputClients.get(tp)).map(_.shutdown)
}

private[netflow] trait ThruputSender {
  protected def backend: Storage
  protected def thruputPrefixes: List[NetFlowInetPrefix]

  protected val thruput = (sender: InetSocketAddress, flow: Flow[_], prefix: NetFlowInetPrefix, addr: InetAddress) => {
    val iter = backend.getThruputRecipients(sender, prefix).groupBy(_.platform).iterator
    while (iter.hasNext) {
      val platformRcpts = iter.next()
      val rcptPlatform = platformRcpts._1
      ThruputClientManager.get(rcptPlatform) match {
        case Some(tc) =>
          val msg = rcptPlatform.msg(flow, addr, platformRcpts._2.flatMap(_.toUser))
          tc.write(msg)
        case _ =>
      }
    }
  }
}

