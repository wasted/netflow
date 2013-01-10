package io.netflow.flows.cisco

import io.netflow.flows._
import io.wasted.util.{ Logger, Tryo }

import io.netty.buffer._
import org.joda.time.DateTime
import java.net.{ InetAddress, InetSocketAddress }
import scala.util.{ Try, Success, Failure }

/**
 * NetFlow Version 9 or 10 (IPFIX) Packet - FlowSet DataSet
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-1   | version       | The version of NetFlow records exported 009          |
 * *-------*---------------*------------------------------------------------------*
 * | 2-3   | count         | Number of flows exported in this packet (1-30)       |
 * *-------*---------------*------------------------------------------------------*
 * | 4-7   | SysUptime     | Current time in milli since the export device booted |
 * *-------*---------------*------------------------------------------------------*
 * | 8-11  | unix_secs     | Current count of seconds since 0000 UTC 1970         |
 * *-------*---------------*------------------------------------------------------*
 * | 12-15 | PackageSeq    | pk id of all flows                                   |
 * *-------*---------------*------------------------------------------------------*
 * | 16-19 | Source ID     | engine type+engine id                                |
 * *-------*---------------*------------------------------------------------------*
 * | 20-   | others        | Unused (zero) bytes                                  |
 * *-------*---------------*------------------------------------------------------*
 */
private[netflow] object TemplateFlowPacket extends Logger {
  private val V9_Header_Size = 20

  /**
   * Parse a v9 or v10 (IPFIX) Flow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): TemplateFlowPacket = {
    val version = buf.getInteger(0, 2).get.toInt
    if (version != 9 && version != 10) throw new InvalidFlowVersionException(sender, version)

    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    if (buf.readableBytes < V9_Header_Size)
      throw new IncompleteFlowPacketHeaderException(sender)

    val count = buf.getInteger(2, 2).get.toInt
    val uptime = buf.getInteger(4, 4).get
    val unix_secs = buf.getInteger(8, 4).get
    val packageSeq = buf.getInteger(12, 4).get
    val sourceId = buf.getInteger(16, 4).get

    var flows = Vector[Flow]()
    var flowsetCounter = 0
    var packetOffset = V9_Header_Size
    while (flowsetCounter < count && packetOffset < buf.readableBytes) {
      val flowsetId = buf.getInteger(packetOffset, 2).get.toInt
      val flowsetLength = buf.getInteger(packetOffset + 2, 2).get.toInt
      if (flowsetLength == 0) throw new IllegalFlowSetLengthException(sender)
      flowsetId match {
        case 0 | 2 => // template flowset - 0 NetFlow v9, 2 IPFIX
          val flowtype = if (flowsetId == 0) "NetFlow v9" else "IPFIX"
          var templateOffset = packetOffset + 4
          debug(flowtype + " Template FlowSet (" + flowsetId + ") from " + senderIP + "/" + senderPort)
          do {
            val fieldCount = buf.getUnsignedShort(templateOffset + 2)
            val templateSize = fieldCount * 4 + 4
            try {
              val buffer = buf.slice(templateOffset, templateSize)
              Template(sender, buffer, flowsetId).map(flows :+= _)
              flowsetCounter += 1
            } catch {
              case e: IndexOutOfBoundsException => throw new IllegalFlowSetLengthException(sender)
              case e: Throwable => warn(e.toString, e); debug(e.toString, e)
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case 1 | 3 => // template flowset - 1 NetFlow v9, 3 IPFIX
          val flowtype = if (flowsetId == 0) "NetFlow v9" else "IPFIX"
          debug(flowtype + " OptionTemplate FlowSet (" + flowsetId + ") from " + senderIP + "/" + senderPort)
          var templateOffset = packetOffset + 4
          do {
            val scopeLen = buf.getInteger(templateOffset + 2, 2).get.toInt
            val optionLen = buf.getInteger(templateOffset + 4, 2).get.toInt
            val templateSize = scopeLen + optionLen + 6
            try {
              val buffer = buf.slice(templateOffset, templateSize)
              Template(sender, buffer, flowsetId).map(flows :+= _)
              flowsetCounter += 1
            } catch {
              case e: IndexOutOfBoundsException => throw new IllegalFlowSetLengthException(sender)
              case e: Throwable => warn(e.toString, e); debug(e.toString, e)
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case a: Int if a > 255 => // flowset - templateId == flowsetId
          Template(sender, flowsetId) match {
            case Some(tmpl) =>
              val flowversion = if (tmpl.isIPFIX) 10 else 9
              var recordOffset = packetOffset + 4
              while (recordOffset + tmpl.length <= packetOffset + flowsetLength) {
                try {
                  val buffer = buf.slice(recordOffset, tmpl.length)
                  flows :+= TemplateFlow(flowversion, sender, buffer, tmpl)
                  flowsetCounter += 1
                } catch {
                  case e: IllegalFlowDirectionException => warn(e.toString)
                  case e: Throwable => warn(e.toString, e); e.printStackTrace
                }
                recordOffset += tmpl.length
              }
            case _ =>
          }
        case a: Int => debug("Unexpected TemplateId (" + a + ")")
      }
      packetOffset += flowsetLength.toInt
    }
    val date = new org.joda.time.DateTime(unix_secs * 1000)
    TemplateFlowPacket(version, sender, count, uptime, date, flows)
  }

}

private[netflow] case class TemplateFlowPacket(
  versionNumber: Int,
  sender: InetSocketAddress,
  count: Int,
  uptime: Long,
  date: DateTime,
  flows: Vector[Flow]) extends FlowPacket {
  lazy val version = "netflow:" + versionNumber + ":packet"
}

private[netflow] object TemplateFlow {
  import FieldDefinition._

  /**
   * Parse a v9 or IPFIX Flow
   *
   * @param version NetFlow Version
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf, template: Template): IPFlowData = {
    if (buf.array.length < template.length)
      throw new CorruptFlowTemplateException(sender, template.id)
    import scala.language.postfixOps

    val srcPort = buf.getInteger(template, L4_SRC_PORT).get.toInt
    val dstPort = buf.getInteger(template, L4_DST_PORT).get.toInt

    val srcAS = buf.getInteger(template, SRC_AS) getOrElse 0L toInt
    val dstAS = buf.getInteger(template, DST_AS) getOrElse 0L toInt
    val proto = buf.getInteger(template, PROT) getOrElse 0L toInt
    val tos = buf.getInteger(template, SRC_TOS) getOrElse 0L toInt

    val getSrcs = buf.getInetAddress(template, IPV4_SRC_ADDR, IPV6_SRC_ADDR)
    val getDsts = buf.getInetAddress(template, IPV4_DST_ADDR, IPV6_DST_ADDR)
    val nextHop = buf.getInetAddress(template, IPV4_NEXT_HOP, IPV6_NEXT_HOP)

    val direction: Option[Int] = if (!template.hasDirection) None else
      Some(buf.getUnsignedByte(template.typeOffset(DIRECTION)).toInt)

    val srcAddress = direction match {
      case Some(0) => getSrcs
      case Some(1) => getDsts
      case _ => getSrcs
    }

    val dstAddress = direction match {
      case Some(0) => getDsts
      case Some(1) => getSrcs
      case _ => getDsts
    }

    val pkts = direction match {
      case Some(0) => buf.getInteger(template, InPKTS, OutPKTS)
      case Some(1) => buf.getInteger(template, OutPKTS, InPKTS)
      case _ => buf.getInteger(template, InPKTS, OutPKTS)
    }

    val bytes = direction match {
      case Some(0) => buf.getInteger(template, InBYTES, OutBYTES)
      case Some(1) => buf.getInteger(template, OutBYTES, InBYTES)
      case _ => buf.getInteger(template, InBYTES, OutBYTES)
    }

    direction match {
      case Some(x) if x > 1 => throw new IllegalFlowDirectionException(sender, x)
      case _ =>
    }
    IPFlowData(
      "netflow:" + version + ":flow",
      sender,
      srcPort, dstPort,
      srcAS, dstAS,
      srcAddress, dstAddress, nextHop,
      pkts, bytes, proto, tos,
      buf.readableBytes)
  }

}

