package io.netflow.flows.cflow

import io.netflow.flows._
import io.wasted.util._

import io.netty.buffer._
import java.net.InetSocketAddress

import scala.language.postfixOps
import scala.util.{ Try, Failure, Success }
import scala.concurrent.duration._

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
object NetFlowV9Packet extends Logger {
  private val headerSize = 20

  /**
   * Parse a v9 Flow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param actor Actor responsible for this sender
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, actor: Wactor.Address): Try[NetFlowV9Packet] = Try[NetFlowV9Packet] {
    val version = buf.getInteger(0, 2).toInt
    if (version != 9) return Failure(new InvalidFlowVersionException(version))
    val packet = NetFlowV9Packet(sender, buf.readableBytes)

    val senderIP = sender.getAddress.getHostAddress
    val senderPort = sender.getPort
    if (packet.length < headerSize)
      return Failure(new IncompleteFlowPacketHeaderException)

    packet.count = buf.getInteger(2, 2).toInt
    packet.uptime = buf.getInteger(4, 4) / 1000
    packet.date = new org.joda.time.DateTime(buf.getInteger(8, 4) * 1000)
    packet.flowSequence = buf.getInteger(12, 4)
    packet.sourceId = buf.getInteger(16, 4)

    var flowsetCounter = 0
    var packetOffset = headerSize

    // we use a mutable array here in order not to bash the garbage collector so badly
    // because whenever we append something to our vector, the old vectors need to get GC'd
    val flows = scala.collection.mutable.ArrayBuffer[Flow[_]]()
    while (flowsetCounter < packet.count && packetOffset < packet.length) {
      val flowsetId = buf.getInteger(packetOffset, 2).toInt
      val flowsetLength = buf.getInteger(packetOffset + 2, 2).toInt
      if (flowsetLength == 0) return Failure(new IllegalFlowSetLengthException)
      if (packetOffset + flowsetLength > packet.length) return Failure(new ShortFlowPacketException)

      flowsetId match {
        case 0 | 2 => // template flowset - 0 NetFlow v9, 2 IPFIX
          var templateOffset = packetOffset + 4 // add the 4 byte flowset Header
          debug("Template FlowSet (" + flowsetId + ") from " + senderIP + "/" + senderPort)
          do {
            val fieldCount = buf.getUnsignedShort(templateOffset + 2)
            val templateSize = fieldCount * 4 + 4
            if (templateOffset + templateSize < packet.length) {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV9Template(sender, buffer, flowsetId) match {
                case Success(tmpl) =>
                  actor ! tmpl
                  flows += tmpl
                case Failure(e) => warn(e.toString)
              }
              flowsetCounter += 1
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case 1 | 3 => // template flowset - 1 NetFlow v9, 3 IPFIX
          debug("OptionTemplate FlowSet (" + flowsetId + ") from " + senderIP + "/" + senderPort)
          var templateOffset = packetOffset + 4 // add the 4 byte flowset Header
          do {
            val scopeLen = buf.getInteger(templateOffset + 2, 2).toInt
            val optionLen = buf.getInteger(templateOffset + 4, 2).toInt
            val templateSize = scopeLen + optionLen + 6
            if (templateOffset + templateSize < packet.length) {
              val buffer = buf.slice(templateOffset, templateSize)
              NetFlowV9Template(sender, buffer, flowsetId) match {
                case Success(tmpl) =>
                  actor ! tmpl
                  flows += tmpl
                case Failure(e) => warn(e.toString); e.printStackTrace
              }
              flowsetCounter += 1
            }
            templateOffset += templateSize
          } while (templateOffset - packetOffset < flowsetLength)

        case a: Int if a > 255 => // flowset - templateId == flowsetId
          NetFlowV9Template(sender, flowsetId) match {
            case Some(tmpl) =>
              val option = (tmpl.flowsetId == 1)
              var recordOffset = packetOffset + 4 // add the 4 byte flowset Header
              while (recordOffset - packetOffset + tmpl.length <= flowsetLength) {
                val buffer = buf.slice(recordOffset, tmpl.length)
                val flow =
                  if (option) NetFlowV9Option(sender, buffer, tmpl, packet.uptime)
                  else NetFlowV9Data(sender, buffer, tmpl, packet.uptime)

                flow match {
                  case Success(flow) => flows += flow
                  case Failure(e) => warn(e.toString)
                }
                flowsetCounter += 1
                recordOffset += tmpl.length
              }
            case _ =>
          }
        case a: Int => debug("Unexpected TemplateId (" + a + ")")
      }
      packetOffset += flowsetLength
    }
    packet.flows = flows.toVector
    packet
  }
}

case class NetFlowV9Packet(sender: InetSocketAddress, length: Int) extends FlowPacket {
  def version = "NetFlowV9 Packet"
  var flowSequence: Long = -1L
  var sourceId: Long = -1L
}

object NetFlowV9Data extends Logger {
  import TemplateFields._
  val parseExtraFields = Config.getBool("netflow.extraFields", true)

  /**
   * Parse a Version 9 Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param template NetFlow Template for this Flow
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, template: NetFlowV9Template, uptime: Long): Try[NetFlowV9Data] = Try[NetFlowV9Data] {
    val flow = NetFlowV9Data(sender, buf.readableBytes(), template.id)
    flow.srcPort = buf.getInteger(template, L4_SRC_PORT).get.toInt
    flow.dstPort = buf.getInteger(template, L4_DST_PORT).get.toInt

    flow.srcAS = (buf.getInteger(template, SRC_AS) getOrElse -1L).toInt
    flow.dstAS = (buf.getInteger(template, DST_AS) getOrElse -1L).toInt
    flow.proto = (buf.getInteger(template, PROT) getOrElse 0L).toInt
    flow.tos = (buf.getInteger(template, SRC_TOS) getOrElse 0L).toInt

    // Add 0 seconds to router uptime if the flow has no data stamps
    flow.start = uptime + (buf.getInteger(template, FIRST_SWITCHED).getOrElse(0L).toInt / 1000)
    flow.stop = uptime + (buf.getInteger(template, LAST_SWITCHED).getOrElse(0L).toInt / 1000)

    flow.tcpflags = (buf.getInteger(template, TCP_FLAGS) getOrElse -1L).toInt

    val getSrcs = buf.getInetAddress(template, IPV4_SRC_ADDR, IPV6_SRC_ADDR)
    val getDsts = buf.getInetAddress(template, IPV4_DST_ADDR, IPV6_DST_ADDR)
    flow.nextHop = buf.getInetAddress(template, IPV4_NEXT_HOP, IPV6_NEXT_HOP)

    val direction = buf.getInteger(template, DIRECTION)

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
      case Some(x) if x > 1 => return Failure(new IllegalFlowDirectionException(x.toInt, flow))
      case _ =>
    }

    if (parseExtraFields) flow.extraFields = template.getExtraFields(buf)
    flow
  }

}

case class NetFlowV9Data(val sender: InetSocketAddress, val length: Int, val template: Int) extends NetFlowData[NetFlowV9Data] {
  def version = "NetFlowV9Data " + template

  var extraFields = Map[String, Long]()
  override lazy val jsonExtra = ",  " + extraFields.map(b => "\"" + b._1 + "\": " + b._2).mkString(", ")

  override lazy val stringExtra = "- Template %s".format(template)
}

object NetFlowV9Option extends Logger {
  import TemplateFields._

  /**
   * Parse a Version 9 Option Flow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   * @param template NetFlow Template for this Flow
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf, template: NetFlowV9Template, uptime: Long): Try[NetFlowV9Option] = Try[NetFlowV9Option] {
    val flow = NetFlowV9Option(sender, buf.readableBytes(), template.id)

    flow.extraFields = template.getExtraFields(buf)
    flow
  }

}

case class NetFlowV9Option(sender: InetSocketAddress, length: Int, template: Int) extends Flow[NetFlowV9Option] {
  def version = "NetFlowV9Option " + template

  var extraFields = Map[String, Long]()
  lazy val json = "{" + extraFields.map(b => "\"" + b._1 + "\": " + b._2).mkString(", ") + "}"

}

