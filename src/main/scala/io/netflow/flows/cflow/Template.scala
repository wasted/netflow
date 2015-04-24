package io.netflow.flows.cflow

import java.net.{ InetAddress, InetSocketAddress }
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.twitter.util.Future
import io.netflow.lib._
import io.netflow.storage
import io.netty.buffer._
import io.wasted.util.Tryo
import net.liftweb.json.JsonDSL._
import net.liftweb.json.Serialization
import org.joda.time.DateTime

import scala.util.{ Failure, Try }

private[netflow] abstract class TemplateMeta[T <: Template](implicit m: Manifest[T]) {
  def apply(sender: InetSocketAddress, buf: ByteBuf, fpId: UUID, flowsetId: Int, timestamp: DateTime): Try[T] = Try[T] {
    val templateId = buf.getUnsignedShort(0)
    if (!(templateId < 0 || templateId > 255)) // 0-255 reserved for flowset ID
      return Failure(new IllegalTemplateIdException(templateId))

    var map = Map[String, Int]("flowsetId" -> flowsetId)
    var idx, dataFlowSetOffset = 0
    flowsetId match {
      case 0 | 2 =>
        val fieldCount = buf.getUnsignedShort(2)
        var offset = 4
        while (idx < fieldCount) {
          val typeName = buf.getUnsignedShort(offset)
          val typeLen = buf.getUnsignedShort(offset + 2)
          if (typeName < 93 && typeName > 0) {
            map ++= Map("offset_" + typeName -> dataFlowSetOffset, "length_" + typeName -> typeLen)
          }
          dataFlowSetOffset += typeLen
          offset += 4
          idx += 1
        }
      case 1 | 3 =>
        val scopeLen = buf.getUnsignedInteger(2, 2).toInt
        val optionLen = buf.getUnsignedInteger(4, 2).toInt
        var offset = 6
        var curLen = 0
        while (curLen < scopeLen + optionLen) {
          val typeName = buf.getUnsignedShort(offset)
          val scopeBool = if (curLen < scopeLen) 1 else 0
          map ++= Map("scope_" + typeName -> scopeBool)
          val typeLen = buf.getUnsignedShort(offset + 2)
          if (typeName < 93 && typeName > 0) {
            map ++= Map("offset_" + typeName -> dataFlowSetOffset, "length_" + typeName -> typeLen)
          }
          dataFlowSetOffset += typeLen
          offset += 4
          curLen += 4
        }
    }
    map ++= Map("length" -> dataFlowSetOffset)
    this(sender, templateId, fpId, timestamp, map)
  }

  def apply(sender: InetSocketAddress, id: Int, fpId: UUID, timestamp: DateTime, map: Map[String, Int]): T
}

private[netflow] trait Template extends Flow[Template] {
  def versionNumber: Int
  def number: Int
  def map: Map[String, Int]

  lazy val version = "NetFlowV" + versionNumber + { if (isOptionTemplate) "Option" else "" } + "Template " + number
  lazy val stringMap = map.mapValues(_.toString)
  lazy val arrayMap: Array[String] = map.flatMap(b => Array(b._1, b._2.toString)).toArray
  def objectMap = map.map(b => (b._1, b._2.toString)).toMap

  lazy val length: Int = map.get("length") match {
    case Some(len: Int) => len
    case _ => -1
  }

  lazy val flowsetId: Int = map.get("flowsetId") match {
    case Some(flowsetId: Int) => flowsetId
    case _ => -1
  }

  lazy val isOptionTemplate = flowsetId == 1 || flowsetId == 3

  def typeOffset(typeName: TemplateFields.Value): Int = map.get("offset_" + typeName.id) match {
    case Some(offset: Int) => offset
    case _ => -1
  }

  def typeLen(typeName: TemplateFields.Value): Int = map.get("length_" + typeName.id) match {
    case Some(len: Int) => len
    case _ => 0
  }

  def key() = (sender, id)
  def hasField(typeName: TemplateFields.Value): Boolean = map.contains("offset_" + typeName.id)

  import io.netflow.flows.cflow.TemplateFields._
  lazy val hasSrcAS = hasField(SRC_AS)
  lazy val hasDstAS = hasField(DST_AS)
  lazy val hasDirection = hasField(DIRECTION)

  lazy val fields = map.keys.filter(_.startsWith("offset_")).map(b => TemplateFields(b.replace("offset_", "").toInt))

  private val excludeFields = List(
    SRC_AS, DST_AS, PROT, SRC_TOS,
    L4_SRC_PORT, L4_DST_PORT,
    IPV4_SRC_ADDR, IPV4_DST_ADDR, IPV4_NEXT_HOP,
    IPV6_SRC_ADDR, IPV6_DST_ADDR, IPV6_NEXT_HOP,
    DIRECTION, InPKTS, OutPKTS, InBYTES, OutBYTES,
    FIRST_SWITCHED, LAST_SWITCHED, TCP_FLAGS // SNMP_INPUT, SNMP_OUTPUT, SRC_MASK, DST_MASK
    )

  lazy val extraFields = map.keys
    .filter(_.startsWith("offset_"))
    .flatMap(b => Tryo(TemplateFields(b.replace("offset_", "").toInt)))
    .filterNot(excludeFields.contains)

  def getExtraFields(buf: ByteBuf): Map[String, Long] =
    extraFields.foldRight(Map[String, Long]()) { (field, map) =>
      buf.getUnsignedInteger(this, field) match {
        case Some(v) => map ++ Map(field.toString -> v)
        case _ => map
      }
    }

  lazy val json = Serialization.write {
    ("template" -> number) ~ ("fields" -> map)
  }
}

private[netflow] case class NetFlowV9Template(number: Int, sender: InetSocketAddress, packet: UUID, last: DateTime, map: Map[String, Int]) extends Template {
  val versionNumber = 9
  lazy val id = UUIDs.timeBased()
}

private[netflow] object NetFlowV9Template extends TemplateMeta[NetFlowV9Template] {
  def apply(sender: InetSocketAddress, id: Int, packet: UUID, timestamp: DateTime, map: Map[String, Int]) =
    NetFlowV9Template(id, sender, packet, timestamp, map)

  private def doLayer[T](f: NetFlowTemplateMeta[NetFlowV9Template] => Future[T]): Future[T] = NodeConfig.values.storage match {
    case Some(StorageLayer.Cassandra) => f(storage.cassandra.NetFlowV9TemplateRecord)
    case Some(StorageLayer.Redis) => f(storage.redis.NetFlowV9TemplateRecord)
    case _ => Future.exception(NoBackendDefined)
  }

  def findAll(inet: InetAddress): Future[Seq[NetFlowV9Template]] = doLayer(_.findAll(inet))
}
