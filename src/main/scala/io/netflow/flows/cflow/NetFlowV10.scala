package io.netflow.flows.cflow

import io.netflow.flows._
import io.wasted.util.Logger

import io.netty.buffer._
import scala.language.postfixOps
import java.net.InetSocketAddress
import scala.util.Try

/**
 * NetFlow Version 9 Packet - FlowSet DataSet
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
 * | 12-15 | PackageSeq    | Sequence counter of total flows exported             |
 * *-------*---------------*------------------------------------------------------*
 * | 16-19 | Source ID     | engine type+engine id                                |
 * *-------*---------------*------------------------------------------------------*
 * | 20-   | others        | Unused (zero) bytes                                  |
 * *-------*---------------*------------------------------------------------------*
 */
object NetFlowV10Packet extends Logger {
  private val V10_Header_Size = 20

  /**
   * Parse a V10 Flow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[NetFlowV10Packet] = Try[NetFlowV10Packet] {
    val version = buf.getInteger(0, 2).toInt
    if (version != 9) throw new InvalidFlowVersionException(sender, version)
    val packet = NetFlowV10Packet(sender)

    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    if (buf.readableBytes < V10_Header_Size)
      throw new IncompleteFlowPacketHeaderException(sender)

    packet.count = buf.getInteger(2, 2).toInt
    packet.uptime = buf.getInteger(4, 4) / 1000
    packet.date = new org.joda.time.DateTime(buf.getInteger(8, 4) * 1000)
    packet.flowSequence = buf.getInteger(12, 4)
    packet.sourceId = buf.getInteger(16, 4)

    var flowsetCounter = 0
    var packetOffset = V10_Header_Size
    while (flowsetCounter < packet.count && packetOffset < buf.readableBytes) {
      val flowsetId = buf.getInteger(packetOffset, 2).toInt
      val flowsetLength = buf.getInteger(packetOffset + 2, 2).toInt
      if (flowsetLength == 0) throw new IllegalFlowSetLengthException(sender)
      flowsetId match {
        case 2 => // template flowset - 0 NetFlow V10, 2 IPFIX
          var templateOffset = packetOffset + 4
          debug("Template FlowSet (" + flowsetId + ") from " + senderIP + "/" + senderPort)
          do {
            val fieldCount = buf.getUnsignedShort(templateOffset + 2)
            val templateSize = fieldCount * 4 + 4
            try {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV10Template(sender, buffer, flowsetId).map(packet.flows :+= _)
              flowsetCounter += 1
            } catch {
              case e: IndexOutOfBoundsException => throw new IllegalFlowSetLengthException(sender)
              case e: Throwable => warn(e.toString, e); debug(e.toString, e)
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case 3 => // template flowset - 1 NetFlow V10, 3 IPFIX
          debug("OptionTemplate FlowSet (" + flowsetId + ") from " + senderIP + "/" + senderPort)
          var templateOffset = packetOffset + 4
          do {
            val scopeLen = buf.getInteger(templateOffset + 2, 2).toInt
            val optionLen = buf.getInteger(templateOffset + 4, 2).toInt
            val templateSize = scopeLen + optionLen + 6
            try {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV10Template(sender, buffer, flowsetId).map(packet.flows :+= _)
              flowsetCounter += 1
            } catch {
              case e: IndexOutOfBoundsException => throw new IllegalFlowSetLengthException(sender)
              case e: Throwable => warn(e.toString, e); debug(e.toString, e)
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case a: Int if a > 255 => // flowset - templateId == flowsetId
          NetFlowV10Template(sender, flowsetId) match {
            case Some(tmpl) =>
              var recordOffset = packetOffset + 4
              while (recordOffset + tmpl.length <= packetOffset + flowsetLength) {
                try {
                  val buffer = buf.slice(recordOffset, tmpl.length)
                  NetFlowV10Data(sender, buffer, tmpl).map(packet.flows :+= _)
                  flowsetCounter += 1
                } catch {
                  case e: IllegalFlowDirectionException => warn(e.toString)
                  case e: Throwable => warn(e.toString, e); e.printStackTrace()
                }
                recordOffset += tmpl.length
              }
            case _ =>
          }
        case a: Int => debug("Unexpected TemplateId (" + a + ")")
      }
      packetOffset += flowsetLength
    }
    packet
  }
}

case class NetFlowV10Packet(sender: InetSocketAddress) extends FlowPacket {
  def version = "NetFlowV10Data Packet"
  var flowSequence: Long = -1L
  var sourceId: Long = -1L
}

object NetFlowV10Data extends Logger {
  import TemplateFields._

  /**
   * Parse a Version 10 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param template NetFlow Template for this Flow
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, template: NetFlowV10Template): Try[NetFlowV10Data] = Try[NetFlowV10Data] {
    val flow = NetFlowV10Data(sender, buf.readableBytes())
    flow.srcPort = buf.getInteger(template, L4_SRC_PORT).get.toInt
    flow.dstPort = buf.getInteger(template, L4_DST_PORT).get.toInt

    flow.srcAS = (buf.getInteger(template, SRC_AS) getOrElse -1L).toInt
    flow.dstAS = (buf.getInteger(template, DST_AS) getOrElse -1L).toInt
    flow.proto = (buf.getInteger(template, PROT) getOrElse 0L).toInt
    flow.tos = (buf.getInteger(template, SRC_TOS) getOrElse 0L).toInt

    val getSrcs = buf.getInetAddress(template, IPV4_SRC_ADDR, IPV6_SRC_ADDR)
    val getDsts = buf.getInetAddress(template, IPV4_DST_ADDR, IPV6_DST_ADDR)
    flow.nextHop = buf.getInetAddress(template, IPV4_NEXT_HOP, IPV6_NEXT_HOP)

    val direction: Option[Int] = if (!template.hasDirection) None else
      Some(buf.getUnsignedByte(template.typeOffset(DIRECTION)).toInt)

    flow.srcAddress = direction match {
      case Some(0) => getSrcs
      case Some(1) => getDsts
      case _ => getSrcs
    }

    flow.dstAddress = direction match {
      case Some(0) => getDsts
      case Some(1) => getSrcs
      case _ => getDsts
    }

    flow.pkts = direction match {
      case Some(0) => buf.getInteger(template, InPKTS, OutPKTS)
      case Some(1) => buf.getInteger(template, OutPKTS, InPKTS)
      case _ => buf.getInteger(template, InPKTS, OutPKTS)
    }

    flow.bytes = direction match {
      case Some(0) => buf.getInteger(template, InBYTES, OutBYTES)
      case Some(1) => buf.getInteger(template, OutBYTES, InBYTES)
      case _ => buf.getInteger(template, InBYTES, OutBYTES)
    }

    direction match {
      case Some(x) if x > 1 => throw new IllegalFlowDirectionException(sender, x)
      case _ =>
    }

    // TODO use template.fields to get all fields out of this flow
    // flow.extraFields =
    flow
  }

}

case class NetFlowV10Data(sender: InetSocketAddress, length: Int) extends NetFlowData[NetFlowV10Data] {
  def version = "NetFlowV10Data Flow"

  var extraFields = Map[String, Long]()
  override lazy val jsonExtra = extraFields.foldRight(", ") { (b, json) =>
    json + "\"" + b._1 + "\": %s".format(b._2)
  }

}

