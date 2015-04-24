package io.netflow

import java.net.InetAddress

import io.netty.buffer._
import io.wasted.util._
import net.liftweb.json._
import org.joda.time.DateTime

import scala.util.{ Failure, Success, Try }

package object lib {

  import io.netflow.flows.cflow.TemplateFields
  implicit val wheelTimer = WheelTimer()
  implicit val formats = net.liftweb.json.DefaultFormats + new InetPrefixSerializer + new DirectionSerializer + new DateTimeSerializer

  final val NoBackendDefined = new IllegalArgumentException("No backend is defined") with scala.util.control.NoStackTrace

  val defaultAddr = InetAddress.getByName("0.0.0.0")

  def string2prefix(str: String): Option[InetPrefix] = {
    val split = str.split("/")
    if (split.length != 2) None else for {
      len <- Tryo(split(1).toInt)
      base <- Tryo(InetAddress.getByName(split(0)))
    } yield InetPrefix(base, len)
  }

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

    def getUnsignedInteger(template: flows.cflow.Template, field: TemplateFields.Value): Option[Long] = {
      if (!template.hasField(field)) return None
      Some(getUnsignedInteger(template.typeOffset(field), template.typeLen(field)))
    }

    def getUnsignedInteger(template: flows.cflow.Template, field1: TemplateFields.Value, field2: TemplateFields.Value): Option[Long] =
      getUnsignedInteger(template, field1) orElse getUnsignedInteger(template, field2)

    def getUnsignedInteger(offset: Int, length: Int): Long = length match {
      case 1 => buf.getUnsignedByte(offset).toLong
      case 2 => buf.getUnsignedShort(offset).toLong
      case 3 => buf.getUnsignedMedium(offset).toLong
      case 4 => buf.getUnsignedInt(offset)
      case 8 => buf.getLong(offset) & 0x00000000ffffffffL
      case _ => 0L
    }
  }

  class InetPrefixSerializer extends CustomSerializer[InetPrefix](format => (
    {
      case JObject(JField("prefix", JString(prefix)) :: JField("prefixLen", JInt(prefixLen)) :: Nil) =>
        InetPrefix(InetAddress.getByName(prefix), prefixLen.intValue())
    },
    {
      case x: InetAddress => JString(x.getHostAddress)
    }))

  class DirectionSerializer extends CustomSerializer[TrafficType.Value](format => (
    {
      case JObject(JField("direction", JString(name)) :: Nil) => TrafficType.withName(name)
    },
    {
      case x: TrafficType.Value => JString(x.toString)
    }))

  class DateTimeSerializer extends CustomSerializer[DateTime](format => (
    {
      case JObject(JField("date", JString(date)) :: Nil) => new DateTime(date)
    },
    {
      case x: DateTime => JString(format.dateFormat.format(x.toDate))
    }))
}

