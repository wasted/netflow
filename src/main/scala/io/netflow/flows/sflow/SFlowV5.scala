package io.netflow.flows.sflow

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import io.netflow.lib._
import io.netty.buffer._
import io.wasted.util.Logger
import net.liftweb.json.JsonDSL._
import org.joda.time.DateTime

import scala.util.{ Failure, Try }

/**
 * sFlow Version 5 Packet
 *
 * On sFlows which don't have srcAS and dstAS, we simply set them to 0.
 *
 * Flow coming from a v4 Agent
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-3   | version       | The version of sFlow records exported 005            |
 * *-------*---------------*------------------------------------------------------*
 * | 4-7   | agent version | The InetAddress version of the Agent (1=v4, 2=v6)    |
 * *-------*---------------*------------------------------------------------------*
 * | 8-11  | agent IPv4    | IPv4 Address of the Agent                            |
 * *-------*---------------*------------------------------------------------------*
 * | 12-15 | agent sub id  | Agent Sub ID                                         |
 * *-------*---------------*------------------------------------------------------*
 * | 16-19 | sequence id   | Sequence counter of total flows exported             |
 * *-------*---------------*------------------------------------------------------*
 * | 20-23 | sysuptime     | System Uptime                                        |
 * *-------*---------------*------------------------------------------------------*
 * | 24-27 | samples       | Number of samples                                    |
 * *-------*---------------*------------------------------------------------------*
 *
 * Flow coming from a v6 Agent
 *
 * *-------*---------------*------------------------------------------------------*
 * | Bytes | Contents      | Description                                          |
 * *-------*---------------*------------------------------------------------------*
 * | 0-3   | version       | The version of sFlow records exported 005            |
 * *-------*---------------*------------------------------------------------------*
 * | 4-7   | agent version | The InetAddress version of the Agent (1=v4, 2=v6)    |
 * *-------*---------------*------------------------------------------------------*
 * | 8-23  | agent IPv6    | IPv6 Address of the Agent                            |
 * *-------*---------------*------------------------------------------------------*
 * | 24-27 | agent sub id  | Agent Sub ID                                         |
 * *-------*---------------*------------------------------------------------------*
 * | 28-31 | sequence id   | Sequence counter of total flows exported             |
 * *-------*---------------*------------------------------------------------------*
 * | 32-35 | sysuptime     | System Uptime                                        |
 * *-------*---------------*------------------------------------------------------*
 * | 36-39 | samples       | Number of samples                                    |
 * *-------*---------------*------------------------------------------------------*
 */

object SFlowV5Packet extends Logger {

  /**
   * Parse a Version 5 sFlow Packet
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  def apply(sender: InetSocketAddress, buf: ByteBuf): Try[SFlowV5Packet] = Try[SFlowV5Packet] {
    val version = buf.getUnsignedInteger(0, 4).toInt
    if (version != 5) return Failure(new InvalidFlowVersionException(version))

    buf.readerIndex(0)

    if (buf.readableBytes < 28)
      return Failure(new IncompleteFlowPacketHeaderException)

    val agentIPversion = if (buf.getUnsignedInteger(4, 4) == 1L) 4 else 6
    val agentLength = if (agentIPversion == 4) 4 else 16
    val agent = buf.getInetAddress(8, agentLength)

    var offset = 8 + agentLength
    val agentSubId = buf.getUnsignedInteger(offset, 4)
    val sequenceId = buf.getUnsignedInteger(offset + 4, 4)
    val uptime = buf.getUnsignedInteger(offset + 8, 4)
    val count = buf.getUnsignedInteger(offset + 12, 4).toInt
    val id = UUIDs.timeBased()
    offset = offset + 16

    //packet.flows = Vector.range(0, count) map { i =>
    //  val flow = apply(5, sender, buf.slice(offset, buf.readableBytes - offset), id)
    //  offset += flow.length + 8
    //  flow
    //}
    val flows: List[SFlowV5] = List()
    SFlowV5Packet(id, sender, buf.readableBytes, agent, agentSubId, sequenceId, uptime, flows)
  }

  /**
   * Parse a Version 5 sFlow
   *
   * @param sender The sender's InetSocketAddress
   * @param buf Netty ByteBuf containing the UDP Packet
   */
  //def apply(version: Int, sender: InetSocketAddress, buf: ByteBuf, fpId: UUID): IFFlowData = {
  // Since sFlows have dynamic length, we need to keep count
  //var recordLength = 0
  //}
}

case class SFlowV5Packet(id: UUID, sender: InetSocketAddress, length: Int, agent: InetAddress,
                         agentSubId: Long, sequenceId: Long, uptime: Long, flows: List[SFlowV5]) extends FlowPacket {
  def version = "sFlowV5 Packet"
  def count = flows.length
  lazy val timestamp = DateTime.now
  def persist(): Unit = {
    // FIXME persist
  }
}

case class SFlowV5(id: UUID, sender: InetSocketAddress, length: Int, packet: UUID) extends Flow[SFlowV5] {
  def version = "sFlowV5 Flow"

  lazy val json = {
    ("flowVersion" -> version) ~ ("flowSender" -> "%s:%s".format(senderIP, senderPort))
  }.toString
}
