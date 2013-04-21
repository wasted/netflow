package io.netflow.flows.cflow

import io.netflow.flows._
import io.netflow.backends.Storage
import io.wasted.util._

import io.netty.buffer._

import scala.collection.immutable.HashMap

import java.net.InetSocketAddress
import scala.util.{ Try, Failure }

abstract class TemplateMeta[T <: Template](implicit m: Manifest[T]) {
  private val cache = scala.collection.concurrent.TrieMap[(InetSocketAddress, Int), T]()
  def clear(sender: InetSocketAddress) {
    cache.keys.filter(_._1 == sender).foreach(cache.remove)
  }

  def apply(sender: InetSocketAddress, buf: ByteBuf, flowsetId: Int): Try[T] = Try[T] {
    val templateId = buf.getUnsignedShort(0)
    if (!(templateId < 0 || templateId > 255)) // 0-255 reserved for flowset ID
      return Failure(new IllegalTemplateIdException(templateId))

    var map = HashMap[String, AnyVal]("flowsetId" -> flowsetId)
    var idx, dataFlowSetOffset = 0
    flowsetId match {
      case 0 | 2 =>
        val fieldCount = buf.getUnsignedShort(2).toInt
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
        val scopeLen = buf.getInteger(2, 2).toInt
        val optionLen = buf.getInteger(4, 2).toInt
        var offset = 6
        var curLen = 0
        while (curLen < scopeLen + optionLen) {
          val typeName = buf.getUnsignedShort(offset)
          map ++= Map("scope_" + typeName -> (curLen < scopeLen))
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
    val tmpl: T = this(sender, templateId, map)
    val key = (sender, templateId)
    cache.remove(key)
    cache.putIfAbsent(key, tmpl)
    tmpl
  }

  def apply(sender: InetSocketAddress, id: Int): Option[T] =
    cache.get((sender, id)) orElse {
      val backend = Storage.start().get
      val ret = backend.ciscoTemplateFields(sender, id) match {
        case Some(fields: HashMap[String, AnyVal]) => Some(this(sender, id, fields))
        case None => None
      }
      Storage.stop(backend)
      ret
    }

  def apply(sender: InetSocketAddress, id: Int, map: HashMap[String, AnyVal]): T
}

trait Template extends Flow[Template] {
  def versionNumber: Int
  def id: Int
  def sender: InetSocketAddress
  def map: HashMap[String, AnyVal]

  lazy val version = "NetFlowV" + versionNumber + { if (isOptionTemplate) "Option" else "" } + "Template " + id
  lazy val stringMap = map.foldRight(HashMap[String, String]()) { (m, hm) => hm ++ Map(m._1 -> m._2.toString) }
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

  import TemplateFields._
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
      buf.getInteger(this, field) match {
        case Some(v) => map ++ Map(field.toString -> v)
        case _ => map
      }
    }

  lazy val json = """{
  "TemplateId": %s,
  "Fields": %s
}""".format(id, map.map(b => "\"" + b._1 + "\": " + b._2).mkString(", "))
}

case class NetFlowV9Template(id: Int, sender: InetSocketAddress, map: HashMap[String, AnyVal]) extends Template {
  val versionNumber = 9
}

case class NetFlowV10Template(id: Int, sender: InetSocketAddress, map: HashMap[String, AnyVal]) extends Template {
  val versionNumber = 10
}

object NetFlowV9Template extends TemplateMeta[NetFlowV9Template] {
  def apply(sender: InetSocketAddress, id: Int, map: HashMap[String, AnyVal]) = NetFlowV9Template(id, sender, map)
}

object NetFlowV10Template extends TemplateMeta[NetFlowV10Template] {
  def apply(sender: InetSocketAddress, id: Int, map: HashMap[String, AnyVal]) = NetFlowV10Template(id, sender, map)
}

