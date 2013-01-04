package io.netflow.flows.cisco

import io.netflow.flows.Flow
import io.netflow.Service
import io.wasted.util._

import io.netty.buffer._

import scala.collection.immutable.HashMap

import java.net.InetSocketAddress

private[netflow] object Template {

  def apply(sender: InetSocketAddress, buf: ByteBuf, flowsetId: Int): Option[Template] = Tryo {
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
    Template(templateId, sender, map)
  }

  def apply(sender: InetSocketAddress, id: Int): Option[Template] =
    Service.backend.ciscoTemplateFields(sender, id) match {
      case Some(fields: HashMap[String, Int]) => this(sender, id, fields)
      case None => None
    }

  def apply(sender: InetSocketAddress, id: Int, fields: HashMap[String, Int]): Option[Template] =
    Tryo(Template(id, sender, fields))
}

private[netflow] case class Template(id: Int, sender: InetSocketAddress, map: HashMap[String, Int]) extends Flow {
  lazy val version = {
    val nfv = if (isNFv9) "9" else "10"
    "netflow:" + nfv + ":template"
  }
  lazy val stringMap = map.foldRight(HashMap[String, String]()) { (m, hm) => hm ++ Map(m._1 -> m._2.toString) }
  lazy val arrayMap: Array[String] = map.flatMap(b => Array(b._1, b._2.toString)).toArray
  lazy val objectMap: Array[Object] = Array(arrayMap: _*)

  def length() = map.get("length") getOrElse -1
  def flowsetId() = map.get("flowsetId") getOrElse -1

  def isOptionTemplate = false
  def isNFv9 = (flowsetId == 0 || flowsetId == 1)
  def isIPFIX = (flowsetId == 2 || flowsetId == 3)

  def typeOffset(typeName: Int): Int = map.get("offset_" + typeName) getOrElse -1
  def typeLen(typeName: Int): Int = map.get("length_" + typeName) getOrElse 0
  def key() = (sender, id)
  def hasField(typeName: Int): Boolean = map.contains("offset_" + typeName)

  import FieldDefinition._
  lazy val hasSrcAS = hasField(SRC_AS)
  lazy val hasDstAS = hasField(DST_AS)
  lazy val hasDirection = hasField(DIRECTION)
}
