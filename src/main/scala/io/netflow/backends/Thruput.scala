package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._
import io.wasted.util.http._

import java.net.{ InetSocketAddress, InetAddress }
import java.util.UUID

case class ThruputRecipient(platform: ThruputPlatform, toUser: Option[String] = None) {
  def auth = platform.auth
  def sign = platform.sign
  def url = platform.url
}

case class ThruputPlatform(urlStr: String, authStr: String, signStr: String) {
  val auth = UUID.fromString(authStr)
  val sign = UUID.fromString(signStr)
  val url = new java.net.URL(urlStr)
  def msg(fd: Flow[_], addr: InetAddress, toUser: List[String]): String = {
    val user = toUser.length match {
      case 0 => ""
      case _ => """, "to": ["""" + toUser.mkString("""", """") + """"]"""
    }
    val ip = """, "ip": """" + addr.getHostAddress + """""""

    """{ "mime": "wasted/netflow", "body": """ + fd.json + ip + user + " }"
  }
}
trait Thruput {
  protected val backend: Storage
  protected var thruputPrefixes: List[InetPrefix]

  protected val thruputHttpClient = HttpClient(2)

  protected val thruput = (sender: InetSocketAddress, flow: Flow[_], prefix: InetPrefix, addr: InetAddress) =>
    backend.getThruputRecipients(sender, prefix).groupBy(_.platform) foreach { platformRcpts =>
      val rcpt = platformRcpts._1
      Tryo(thruputHttpClient.thruput(rcpt.url, rcpt.auth, rcpt.sign, rcpt.msg(flow, addr, platformRcpts._2.flatMap(_.toUser))))
    }

}
