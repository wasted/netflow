package io.netflow

import io.netty.buffer._

import scala.util.{ Try, Success, Failure }
import java.net.InetAddress

package object flows {
  import flows.cflow.TemplateFields

  val defaultAddr = InetAddress.getByName("0.0.0.0")

  implicit class RichByteBuf(val buf: ByteBuf) extends AnyVal {
    def getInetAddress(template: flows.cflow.Template, field: TemplateFields.Value): Option[InetAddress] = {
      if (!template.hasField(field)) return None
      Some(getInetAddress(template.typeOffset(field), template.typeLen(field)))
    }

    def getInetAddress(template: flows.cflow.Template, field1: TemplateFields.Value, field2: TemplateFields.Value): InetAddress =
      buf.getInetAddress(template, field1) orElse
        buf.getInetAddress(template, field2) getOrElse
        defaultAddr

    def getInetAddress(offset: Int, length: Int = 4): InetAddress = Try[InetAddress] {
      val buffer = buf.slice(offset, length)
      val data = (1 to length).map(a => buffer.readUnsignedByte.toByte).toArray
      InetAddress.getByAddress(data)
    } match {
      case Success(addr) => addr
      case Failure(e) => defaultAddr
    }

    def getInteger(template: flows.cflow.Template, field: TemplateFields.Value): Option[Long] = {
      if (!template.hasField(field)) return None
      Some(getInteger(template.typeOffset(field), template.typeLen(field)))
    }

    def getInteger(template: flows.cflow.Template, field1: TemplateFields.Value, field2: TemplateFields.Value): Long =
      getInteger(template, field1) orElse getInteger(template, field2) getOrElse 0L

    def getInteger(offset: Int, length: Int): Long = length match {
      case 1 => buf.getUnsignedByte(offset).toLong
      case 2 => buf.getUnsignedShort(offset).toLong
      case 3 => buf.getUnsignedMedium(offset).toLong
      case 4 => buf.getUnsignedInt(offset)
      case 8 => scala.math.BigInt((0 to 7).toArray.map(b => buf.getByte(offset + b))).toLong
      case _ => 0L
    }
  }

}

