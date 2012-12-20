package io.netflow.flows.cisco

import io.wasted.util._
import io.netflow.flows._
import io.netflow.backends.StorageConnection

import io.netty.buffer._
import io.netty.util.CharsetUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Try, Success, Failure }
import java.net.{ InetAddress, InetSocketAddress }

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
 * | 12-15 | PackageSeq    | pk id of all flows                                   |
 * *-------*---------------*------------------------------------------------------*
 * | 16-19 | Source ID     | engine type+engine id                                |
 * *-------*---------------*------------------------------------------------------*
 * | 20-   | others        | Unused (zero) bytes                                  |
 * *-------*---------------*------------------------------------------------------*
 *
 * @param senderIP senderIP's IP Address
 * @param buf Netty ByteBuf containing the UDP Packet
 */
private[netflow] class V9FlowPacket(val sender: InetSocketAddress, buf: ByteBuf)(implicit sc: StorageConnection) extends FlowPacket {
  private val V9_Header_Size = 20
  def senderIP() = sender.getAddress.getHostAddress
  def senderPort() = sender.getPort

  if (buf.readableBytes < V9_Header_Size)
    throw new IncompleteFlowPacketHeaderException(sender)

  val count = buf.getUnsignedShort(2).toInt
  val uptime = buf.getUnsignedInt(4)
  val unix_secs = buf.getUnsignedInt(8)
  val packageSeq = buf.getUnsignedInt(12)
  val sourceId = buf.getUnsignedInt(16)

  val flows: List[Flow] = {
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
          info(s"Received $flowtype Template FlowSet ($flowsetId) from $senderIP/$senderPort")
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
          } while (templateOffset - packetOffset <= V9_Header_Size + flowsetLength)

        case 1 | 3 => // template flowset - 1 NetFlow v9, 3 IPFIX
          val flowtype = if (flowsetId == 0) "NetFlow v9" else "IPFIX"
          info(s"Received $flowtype OptionTemplate FlowSet ($flowsetId) from $senderIP/$senderPort")
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
          } while (templateOffset - packetOffset <= V9_Header_Size + flowsetLength)

        case a: Int if a > 255 => // flowset - templateId == flowsetId
          Template(sender, flowsetId) match {
            case Some(tmpl) =>
              val flowtype = if (tmpl.isIPFIX) "IPFIX" else "NetFlow v9"
              val flowdata = if (tmpl.isOptionTemplate) "Option" else "Master"
              info(s"Received $flowtype $flowdata FlowSet ($flowsetId) from $senderIP/$senderPort")
              var recordOffset = packetOffset + 4
              while (recordOffset + tmpl.length <= V9_Header_Size + flowsetLength) {
                try {
                  val buffer = buf.copy(recordOffset, tmpl.length)
                  flows :+= new V9Flow(sender, buffer, tmpl)
                  flowsetCounter += 1
                } catch {
                  case e: Throwable => warn(e.toString, e); e.printStackTrace
                }
                recordOffset += tmpl.length
              }
            case _ =>
          }
        case a: Int => debug(s"Unexpected TemplateId ($a)")
      }
      packetOffset += flowsetLength.toInt
    }
    info(s"Flows passed $flowsetCounter of $count")
    flows
  }

  info(s"NetFlow version 9 received from $senderIP/$senderPort")
}

private[netflow] class V9Flow(val sender: InetSocketAddress, buf: ByteBuf, val template: Template) extends FlowData {
  if (buf.array.length < template.typeOffset(-1))
    throw new CorruptFlowTemplateException(sender, template.id)

  import FieldDefinition._

  val srcPort = buf.getUnsignedShort(template.typeOffset(L4_SRC_PORT))
  val dstPort = buf.getUnsignedShort(template.typeOffset(L4_DST_PORT))
  val srcAS = if (!template.hasSrcAS) 0 else
    buf.getUnsignedShort(template.typeOffset(SRC_AS))
  val dstAS = if (!template.hasDstAS) 0 else
    buf.getUnsignedShort(template.typeOffset(DST_AS))

  private def getAddress(field1: Int, field2: Int) = {
    if (template.hasField(field1))
      buf.getInetAddress(template, field1)
    else buf.getInetAddress(template, field2)
  }

  val direction: Option[Int] = if (!template.hasDirection) None else
    Some(buf.getUnsignedByte(template.typeOffset(DIRECTION)).toInt)

  private val getSrcs = getAddress(IPV4_SRC_ADDR, IPV6_SRC_ADDR)
  private val getDsts = getAddress(IPV4_DST_ADDR, IPV6_DST_ADDR)

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
  val nextHop = getAddress(IPV4_NEXT_HOP, IPV6_NEXT_HOP)

  val pkts = buf.getUnsignedInt(template.typeOffset(InPKTS_32))
  val bytes = buf.getUnsignedInt(template.typeOffset(InBYTES_32))

  val proto = buf.getUnsignedByte(template.typeOffset(PROT)).toInt
  val tos = buf.getUnsignedByte(template.typeOffset(SRC_TOS)).toInt

  info(toString)
}
