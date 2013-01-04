package io.netflow.flows.cisco

import io.netflow.flows._
import io.wasted.util.{ Logger, Tryo }

import io.netty.buffer._

import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }

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
   * @param version NetFlow Version
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf): TemplateFlowPacket = {
    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    if (buf.readableBytes < V9_Header_Size)
      throw new IncompleteFlowPacketHeaderException(sender)

    val count = buf.getUnsignedShort(2).toInt
    val uptime = buf.getUnsignedInt(4)
    val unix_secs = buf.getUnsignedInt(8)
    val packageSeq = buf.getUnsignedInt(12)
    val sourceId = buf.getUnsignedInt(16)

    var flows = List[Flow]()
    var flowsetCounter = 0
    var packetOffset = V9_Header_Size
    while (flowsetCounter < count && packetOffset < buf.readableBytes) {
      val flowsetId = buf.getUnsignedShort(packetOffset).toInt
      val flowsetLength = buf.getUnsignedShort(packetOffset + 2).toInt
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
              val buffer = buf.copy(templateOffset, templateSize)
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
            val scopeLen = buf.getUnsignedShort(templateOffset + 2)
            val optionLen = buf.getUnsignedShort(templateOffset + 4)
            val templateSize = scopeLen + optionLen + 6
            try {
              val buffer = buf.copy(templateOffset, templateSize)
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
                  val buffer = buf.copy(recordOffset, tmpl.length)
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
    TemplateFlowPacket(version, sender, count, uptime, unix_secs, flows)
  }

}

private[netflow] case class TemplateFlowPacket(
  versionNumber: Int,
  sender: InetSocketAddress,
  count: Int,
  uptime: Long,
  unix_secs: Long,
  flows: List[Flow]) extends FlowPacket {
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
  def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf, template: Template): TemplateFlow = {
    if (buf.array.length < template.typeOffset(-1))
      throw new CorruptFlowTemplateException(sender, template.id)
    import scala.language.postfixOps

    def getInt(field1: Int): Option[Long] = if (!template.hasField(field1)) None
    else template.typeOffset(field1) match {
      case -1 => None
      case offset: Int =>
        template.typeLen(field1) match {
          case 2 => Some(buf.getUnsignedShort(offset).toLong)
          case 4 => Some(buf.getUnsignedInt(offset).toLong)
          case 8 => Some(scala.math.BigInt((0 to 7).toArray.map(b => buf.getByte(offset + b))).toLong)
          case _ => None
        }
    }

    def getIntOr(field1: Int, field2: Int): Long = getInt(field1) orElse getInt(field2) getOrElse 0L

    def getAddress(field1: Int): Option[InetAddress] =
      if (template.hasField(field1)) Tryo(buf.getInetAddress(template, field1)) else None

    def getAddressOr(field1: Int, field2: Int) =
      getAddress(field1) orElse getAddress(field2) getOrElse InetAddress.getByName("0.0.0.0")

    val srcPort = buf.getUnsignedShort(template.typeOffset(L4_SRC_PORT))
    val dstPort = buf.getUnsignedShort(template.typeOffset(L4_DST_PORT))
    val srcAS = getInt(SRC_AS) getOrElse 0L toInt
    val dstAS = getInt(DST_AS) getOrElse 0L toInt

    val direction: Option[Int] = if (!template.hasDirection) None else
      Some(buf.getUnsignedByte(template.typeOffset(DIRECTION)).toInt)

    val getSrcs = getAddressOr(IPV4_SRC_ADDR, IPV6_SRC_ADDR)
    val getDsts = getAddressOr(IPV4_DST_ADDR, IPV6_DST_ADDR)

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
    val nextHop = getAddressOr(IPV4_NEXT_HOP, IPV6_NEXT_HOP)

    val pkts = direction match {
      case Some(0) => getIntOr(InPKTS, OutPKTS)
      case Some(1) => getIntOr(OutPKTS, InPKTS)
      case _ => getIntOr(InPKTS, OutPKTS)
    }
    val bytes = direction match {
      case Some(0) => getIntOr(InBYTES, OutBYTES)
      case Some(1) => getIntOr(OutBYTES, InBYTES)
      case _ => getIntOr(InBYTES, OutBYTES)
    }
    val proto = getInt(PROT) getOrElse 0L toInt
    val tos = getInt(SRC_TOS) getOrElse 0L toInt

    val flow = TemplateFlow(version, sender, srcPort, dstPort, srcAS, dstAS, srcAddress, dstAddress, nextHop, pkts, bytes, proto, tos)
    direction match {
      case Some(x) if x > 1 => throw new IllegalFlowDirectionException(sender, x, flow)
      case _ =>
    }
    flow
  }

}

private[netflow] case class TemplateFlow(
  versionNumber: Int,
  sender: InetSocketAddress,
  srcPort: Int,
  dstPort: Int,
  srcAS: Int,
  dstAS: Int,
  srcAddress: InetAddress,
  dstAddress: InetAddress,
  nextHop: InetAddress,
  pkts: Long,
  bytes: Long,
  proto: Int,
  tos: Int) extends FlowData {
  lazy val version = "netflow:" + versionNumber + ":flow"
}
