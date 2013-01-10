package io.netflow.backends

import io.netflow.flows._
import io.wasted.util._
import io.wasted.util.http._

import scala.util.{ Try, Success, Failure }
import scala.concurrent._
import java.util.concurrent.TimeUnit
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
  def msg(fd: JsonFlowData, ip: InetAddress, toUser: List[String]): String = {
    val user = toUser.length match {
      case 0 => ""
      case _ => """, "to": ["""" + toUser.mkString("""", """") + """"]"""
    }
    """{ "mime": "wasted/netflow", "body": """ + fd.json + """, "ip": """" + ip.getHostAddress + """"""" + user + " }"
  }
}
private[netflow] trait Thruput {
  protected val backend: Storage
  protected var thruputPrefixes: List[InetPrefix]

  protected val thruputHttpClient = HttpClient(2)

  protected val thruput = (sender: InetSocketAddress, prefix: InetPrefix, addr: InetAddress, fd: JsonFlowData) =>
    backend.getThruputRecipients(sender, prefix).groupBy(_.platform) foreach { platformRcpts =>
      val rcpt = platformRcpts._1
      Tryo(thruputHttpClient.thruput(rcpt.url, rcpt.auth, rcpt.sign, rcpt.msg(fd, addr, platformRcpts._2.flatMap(_.toUser))))
    }

}
