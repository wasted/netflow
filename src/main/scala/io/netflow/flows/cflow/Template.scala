package io.netflow.flows.cflow

import io.netflow.flows._
import io.netflow.Service
import io.wasted.util._

import io.netty.buffer._

import scala.collection.immutable.HashMap

import java.net.InetSocketAddress
import scala.util.Try

abstract class TemplateMeta[T <: Template](implicit m: Manifest[T]) {

  def apply(sender: InetSocketAddress, buf: ByteBuf, flowsetId: Int): Try[T] = Try[T] {
    val templateId = buf.getUnsignedShort(0)
    if (!(templateId < 0 || templateId > 255)) // 0-255 reserved for flowset ID
      throw new IllegalTemplateIdException(sender, templateId)

    var map = HashMap[String, Int]()
    val fieldCount = buf.getUnsignedShort(2).toInt
    var offset = 4
    var idx, dataFlowSetOffset = 0
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
    map ++= Map("length" -> dataFlowSetOffset, "flowsetId" -> flowsetId)
    this(sender, templateId, map)
  }

  def apply(sender: InetSocketAddress, id: Int): Option[T] =
    Service.backend.ciscoTemplateFields(sender, id) match {
      case Some(fields: HashMap[String, Int]) => Some(this(sender, id, fields))
      case None => None
    }

  def apply(sender: InetSocketAddress, id: Int, map: HashMap[String, Int]): T
}

trait Template extends Flow[Template] {
  def versionNumber: Int
  def id: Int
  def sender: InetSocketAddress
  def map: HashMap[String, Int]

  lazy val version = "NetFlowV" + versionNumber + { if (isOptionTemplate) "Option" else "" } + "Template " + id
  lazy val stringMap = map.foldRight(HashMap[String, String]()) { (m, hm) => hm ++ Map(m._1 -> m._2.toString) }
  lazy val arrayMap: Array[String] = map.flatMap(b => Array(b._1, b._2.toString)).toArray
  lazy val objectMap: Array[Object] = Array(arrayMap: _*)

  def length() = map.get("length") getOrElse -1
  def flowsetId() = map.get("flowsetId") getOrElse -1

  lazy val isOptionTemplate = flowsetId == 1 || flowsetId == 3

  def typeOffset(typeName: TemplateFields.Value): Int = map.get("offset_" + typeName.id) getOrElse -1
  def typeLen(typeName: TemplateFields.Value): Int = map.get("length_" + typeName.id) getOrElse 0
  def key() = (sender, id)
  def hasField(typeName: TemplateFields.Value): Boolean = map.contains("offset_" + typeName.id)

  import TemplateFields._
  lazy val hasSrcAS = hasField(SRC_AS)
  lazy val hasDstAS = hasField(DST_AS)
  lazy val hasDirection = hasField(DIRECTION)

  // TODO Field-ID resolution has to be done through TemplateFields
  lazy val fields = map.keys.filter(_.startsWith("offset_")).map(_.replaceAll("offset_", ""))

  // TODO implement JSON serialization
  lazy val json = ""
}

case class NetFlowV9Template(id: Int, sender: InetSocketAddress, map: HashMap[String, Int]) extends Template {
  val versionNumber = 9
}

case class NetFlowV10Template(id: Int, sender: InetSocketAddress, map: HashMap[String, Int]) extends Template {
  val versionNumber = 10
}

object NetFlowV9Template extends TemplateMeta[NetFlowV9Template] {
  def apply(sender: InetSocketAddress, id: Int, map: HashMap[String, Int]) = NetFlowV9Template(id, sender, map)
}

object NetFlowV10Template extends TemplateMeta[NetFlowV10Template] {
  def apply(sender: InetSocketAddress, id: Int, map: HashMap[String, Int]) = NetFlowV10Template(id, sender, map)
}

