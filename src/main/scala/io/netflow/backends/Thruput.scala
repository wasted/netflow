package io.netflow.backends

import io.netflow.flows._

import java.net.InetAddress
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
  def msg(fd: FlowData, ip: InetAddress, toUser: List[String]): String = {
    val user = toUser.length match {
      case 0 => ""
      case _ => """, "to": ["""" + toUser.mkString("""", """") + """"]"""
    }
    """{ "mime": "wasted/netflow", "body": """ + fd.json + """, "ip": """" + ip.getHostAddress + """"""" + user + " }"
  }
}
private[netflow] trait Thruput {
} /*
  protected var thruputPlatforms = HashMap[String, ThruputPlatform]()
  protected val thruputHttpClient = HttpClient()

  protected val thruputHandleFlow = (sender: InetSocketAddress, prefix: InetPrefix, addr: InetAddress, fd: FlowData, sc: StorageConnection) => {
    val ip = addr.getHostAddress
    getThruputRecipients(sender, prefix)(sc).groupBy(_.platform) foreach { platformRcpts =>
      val rcpt = platformRcpts._1
      thruputHttpClient.thruput(rcpt.url, rcpt.auth, rcpt.sign, rcpt.msg(fd, addr, platformRcpts._2.flatMap(_.toUser)))
    }
  }

  protected def workFlows(flowPacket: FlowPacket, list: List[Flow], prefixes: List[InetPrefix])(implicit sc: StorageConnection): Unit = if (list.length > 0) list.head 
  protected def workPrefix(flowPacket: FlowPacket, fd: FlowData, plist: List[InetPrefix])(implicit sc: StorageConnection): Unit = if (plist.length > 0) {
    val prefix = plist.head
    if (prefix.contains(fd.srcAddress)) thruputHandleFlow(flowPacket.sender, prefix, fd.dstAddress, fd, sc)
    if (prefix.contains(fd.dstAddress)) thruputHandleFlow(flowPacket.sender, prefix, fd.srcAddress, fd, sc)
    workPrefix(flowPacket, fd, plist.tail)
  }

  protected def thruput(flowPacket: FlowPacket)(implicit sc: StorageConnection) {
    // Get all thruput prefixes for this sender
    val prefixes = getCachedThruputPrefixes(flowPacket.sender)
    // Filter out everything but FlowData
    workFlows(flowPacket, flowPacket.flows, prefixes)
match {
    case fd: FlowData =>
      workPrefix(flowPacket, fd, prefixes)
      workFlows(flowPacket, list.tail, prefixes)
    case _ =>
  }

  }
}
*/
