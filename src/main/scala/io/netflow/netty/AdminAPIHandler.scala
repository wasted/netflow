package io.netflow.netty

import java.net.{ InetAddress, InetSocketAddress }

import com.websudos.phantom.Implicits._
import io.netflow.flows.FlowSender
import io.netflow.lib._
import io.netty.channel.{ ChannelFutureListener, ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler }
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders._
import io.netty.handler.codec.http.HttpMethod._
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http._
import io.netty.handler.logging.LogLevel
import io.wasted.util._
import io.wasted.util.http.ExceptionHandler

@ChannelHandler.Sharable
private[netty] object AdminAPIHandler extends SimpleChannelInboundHandler[FullHttpRequest] with Logger {

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    ExceptionHandler(ctx, cause) foreach { c =>
      if (!NodeConfig.values.debugStackTraces) ProtoNegoHandler.debug(cause.toString, cause)
      else ProtoNegoHandler.debug(stackTraceToString(cause))
    }
  }

  private def log(level: LogLevel, ctx: ChannelHandlerContext, request: HttpRequest, message: String) {
    val remoteIP = ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
    val prefix = s"${request.getMethod} - $remoteIP - ${request.getUri.split("\\?").head}: %s"

    level match {
      case LogLevel.DEBUG => debug(prefix, message)
      case LogLevel.ERROR => error(prefix, message)
      case LogLevel.INFO => info(prefix, message)
      case LogLevel.WARN => warn(prefix, message)
      case LogLevel.TRACE => error(prefix, message)
    }
  }

  private def sendError(ctx: ChannelHandlerContext, request: HttpRequest, status: HttpResponseStatus) {
    log(LogLevel.INFO, ctx, request, status.code.toString)
    val response = new DefaultHttpResponse(HTTP_1_1, status)
    response.headers().set(SERVER, "netflow.io " + BuildInfo.version)
    setContentLength(response, 0)
    if (!isKeepAlive(request))
      response.headers().set(CONNECTION, HttpHeaders.Values.CLOSE)
    // Close the connection as soon as the error message is sent.
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
  }

  def channelRead0(ctx: ChannelHandlerContext, request: FullHttpRequest): Unit = {
    // If this is a GET request which results in content, we forward it
    (request.getMethod, request.getUri.split("/").toList.drop(1)) match {
      case (GET, Nil) =>
        val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
        response.headers().set(SERVER, "netflow.io " + BuildInfo.version)
        setContentLength(response, 0)
        if (!isKeepAlive(request))
          response.headers().set(CONNECTION, HttpHeaders.Values.CLOSE)
        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)

      case (PUT, "sender" :: ip :: prefixes) =>
        Tryo(InetAddress.getByName(ip)) match {
          case Some(ipaddr) =>
            val tba = prefixes.grouped(2).map(_.mkString("/")).toSet
            FlowSender.update.where(_.ip eqs ipaddr).modify(_.prefixes addAll tba).future()
            val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
            response.headers().set(SERVER, "netflow.io " + BuildInfo.version)
            setContentLength(response, 0)
            if (!isKeepAlive(request))
              response.headers().set(CONNECTION, HttpHeaders.Values.CLOSE)
            // Close the connection as soon as the error message is sent.
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)

          case _ =>
            sendError(ctx, request, HttpResponseStatus.NOT_ACCEPTABLE)
        }

      case (DELETE, "sender" :: ip :: Nil) =>
        Tryo(InetAddress.getByName(ip)) match {
          case Some(ipaddr) =>
            FlowSender.delete.where(_.ip eqs ipaddr).future()
            val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
            response.headers().set(SERVER, "netflow.io " + BuildInfo.version)
            setContentLength(response, 0)
            if (!isKeepAlive(request))
              response.headers().set(CONNECTION, HttpHeaders.Values.CLOSE)
            // Close the connection as soon as the error message is sent.
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)

          case _ =>
            sendError(ctx, request, HttpResponseStatus.NOT_ACCEPTABLE)
        }

      case (DELETE, "sender" :: ip :: prefixes) =>
        Tryo(InetAddress.getByName(ip)) match {
          case Some(ipaddr) =>
            val tbr = prefixes.grouped(2).map(_.mkString("/")).toSet
            FlowSender.update.where(_.ip eqs ipaddr).modify(_.prefixes removeAll tbr).future()
            val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
            response.headers().set(SERVER, "netflow.io " + BuildInfo.version)
            setContentLength(response, 0)
            if (!isKeepAlive(request))
              response.headers().set(CONNECTION, HttpHeaders.Values.CLOSE)
            // Close the connection as soon as the error message is sent.
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)

          case _ =>
            sendError(ctx, request, HttpResponseStatus.NOT_ACCEPTABLE)
        }

      case _ =>
        sendError(ctx, request, HttpResponseStatus.NOT_FOUND)
    }
  }
}