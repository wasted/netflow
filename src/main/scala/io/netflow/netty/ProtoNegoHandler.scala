package io.netflow.netty

import io.netflow.lib.NodeConfig
import io.netty.buffer._
import io.netty.channel._
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.compression.{ZlibCodecFactory, ZlibWrapper}
import io.netty.handler.codec.http._
import io.netty.handler.ssl.SslHandler
import io.netty.handler.stream.ChunkedWriteHandler
import io.wasted.util._
import io.wasted.util.http.ExceptionHandler

/**
 * Manipulates the current pipeline dynamically to switch protocols or enable SSL or GZIP.
 */
private[netty] object ProtoNegoHandler extends Logger
private[netflow] class ProtoNegoHandler(
  detectSsl: Boolean = NodeConfig.values.ssl.certPass.isDefined && NodeConfig.values.ssl.certPath.isDefined,
  detectGzip: Boolean = NodeConfig.values.http.gzip) extends ByteToMessageDecoder {

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause match {
      case e: javax.net.ssl.SSLException => //ignore this
      case e: Throwable =>
        ExceptionHandler(ctx, cause) foreach { c =>
          if (!NodeConfig.values.debugStackTraces) ProtoNegoHandler.debug(cause.toString, cause)
          else ProtoNegoHandler.debug(stackTraceToString(cause))
        }
    }
  }

  override def decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: java.util.List[Object]) {
    // Will use the first two bytes to detect a protocol.
    if (buffer.readableBytes() < 5) return

    val magic1 = buffer.getUnsignedByte(buffer.readerIndex())
    val magic2 = buffer.getUnsignedByte(buffer.readerIndex() + 1)
    val p = ctx.pipeline

    if (detectSsl) {
      // Check for encrypted bytes
      NodeConfig.values.sslEngine.map { sslEngine =>
        if (SslHandler.isEncrypted(buffer)) p.addLast("ssl", new SslHandler(sslEngine.self))
      }

      p.addLast("nego_gzip", new ProtoNegoHandler(false, detectGzip))
      p.remove(this)
    } else if (detectGzip) {
      // Check for gzip compression
      if (isGzip(magic1, magic2)) {
        p.addLast("gzipdeflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP))
        p.addLast("gzipinflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP))
      }
      p.addLast("nego_http", new ProtoNegoHandler(detectSsl, false))
      p.remove(this)
    } else if (isHttp(magic1, magic2)) {
      val httpc = NodeConfig.values.http
      p.addLast("decoder", new HttpRequestDecoder(httpc.maxInitialLineLength.toInt, httpc.maxHeaderSize.toInt, httpc.maxChunkSize.toInt))
      p.addLast("aggregator", new HttpObjectAggregator(httpc.maxContentLength.toInt))
      p.addLast("encoder", new HttpResponseEncoder())
      if (detectGzip) p.addLast("deflater", new HttpContentCompressor())
      p.addLast("chunkedWriter", new ChunkedWriteHandler())
      p.addLast("dispatchHandler", AdminAPIHandler)
      p.remove(this)
    } else {
      ProtoNegoHandler.info("unknown protocol: " + ctx.channel.remoteAddress.asInstanceOf[java.net.InetSocketAddress].getAddress.getHostAddress)
      // Unknown protocol; discard everything and close the connection.
      buffer.clear()
      ctx.close()
    }
  }

  private def isGzip(magic1: Int, magic2: Int): Boolean = {
    magic1 == 31 && magic2 == 139
  }

  private def isHttp(magic1: Int, magic2: Int): Boolean = {
    magic1 == 'G' && magic2 == 'E' || // GET
      magic1 == 'P' && magic2 == 'O' || // POST
      magic1 == 'P' && magic2 == 'U' || // PUT
      magic1 == 'H' && magic2 == 'E' || // HEAD
      magic1 == 'O' && magic2 == 'P' || // OPTIONS
      magic1 == 'P' && magic2 == 'A' || // PATCH
      magic1 == 'D' && magic2 == 'E' || // DELETE
      magic1 == 'T' && magic2 == 'R' || // TRACE
      magic1 == 'C' && magic2 == 'O' // CONNECT
  }

}