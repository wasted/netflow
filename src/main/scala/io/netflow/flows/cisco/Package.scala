package io.netflow.flows

import io.netflow.flows.cisco._
import io.wasted.util._

import io.netty.buffer._

import java.util.Properties
import java.net.InetAddress
import scala.collection.JavaConversions._

class OurByteBuf(buf: ByteBuf) extends Logger {
  def getInetAddress(template: Template, field: Int): InetAddress = {
    getInetAddress(template.typeOffset(field), template.typeLen(field))
  }

  def getInetAddress(offset: Int, length: Int = 4): InetAddress = {
    val buffer = buf.slice(offset, length)
    val data = (1 to length).map(a => buffer.readUnsignedByte.toByte).toArray
    InetAddress.getByAddress(data)
  }
}

class OurProps(props: Properties) {
  def getNumericKeys() = props.propertyNames flatMap (b => Tryo(b.toString.toInt))
}

package object cisco {
  implicit val ourByteBuf = (buf: ByteBuf) => new OurByteBuf(buf)
  implicit val ourProps = (props: Properties) => new OurProps(props)
}

