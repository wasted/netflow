package io.netflow

import io.wasted.util._

import io.netty.buffer._

import java.util.Properties
import java.net.InetAddress
import scala.collection.JavaConversions._

package object flows {

  implicit class RichByteBuf(buf: ByteBuf) {
    def getInetAddress(template: cisco.Template, field: Int): Option[InetAddress] = {
      if (!template.hasField(field)) return None
      Tryo(getInetAddress(template.typeOffset(field), template.typeLen(field)))
    }

    def getInetAddress(template: cisco.Template, field1: Int, field2: Int): InetAddress =
      buf.getInetAddress(template, field1) orElse
        buf.getInetAddress(template, field2) getOrElse
        InetAddress.getByName("0.0.0.0")

    def getInetAddress(offset: Int, length: Int = 4): InetAddress = {
      val buffer = buf.slice(offset, length)
      val data = (1 to length).map(a => buffer.readUnsignedByte.toByte).toArray
      InetAddress.getByAddress(data)
    }

    def getInteger(template: cisco.Template, field: Int): Option[Long] = {
      if (!template.hasField(field)) None else getInteger(template.typeOffset(field), template.typeLen(field))
    }

    def getInteger(template: cisco.Template, field1: Int, field2: Int): Long =
      getInteger(template, field1) orElse getInteger(template, field2) getOrElse 0L

    def getInteger(offset: Int, length: Int): Option[Long] = length match {
      case 2 => Tryo(buf.getUnsignedShort(offset).toLong)
      case 4 => Tryo(buf.getUnsignedInt(offset).toLong)
      case 8 => Tryo(scala.math.BigInt((0 to 7).toArray.map(b => buf.getByte(offset + b))).toLong)
      case _ => None
    }
  }

}

