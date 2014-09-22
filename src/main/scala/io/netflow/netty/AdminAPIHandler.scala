package io.netflow.netty

import java.net.{ InetAddress, InetSocketAddress }

import com.websudos.phantom.Implicits._
import io.netflow.flows.FlowSender
import io.netflow.lib._
import io.netty.buffer.Unpooled
import io.netty.channel.{ ChannelFutureListener, ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler }
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders._
import io.netty.handler.codec.http.HttpMethod._
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http._
import io.netty.handler.logging.LogLevel
import io.netty.util.CharsetUtil
import io.wasted.util._
import io.wasted.util.http.ExceptionHandler
import net.liftweb.json.{ JsonParser, Serialization }

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

  private def sendError(ctx: ChannelHandlerContext, request: HttpRequest, status: HttpResponseStatus, body: Option[String] = None) {
    log(LogLevel.INFO, ctx, request, status.code.toString)
    val f = ctx.writeAndFlush(WastedHttpResponse.apply(status, body, keepAlive = isKeepAlive(request)))
    if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)
  }

  private object InetAddressParam {
    def unapply(a: String): Option[InetAddress] = Tryo(InetAddress.getByName(a))
  }
  private object InetPrefixesParam {
    def unapply(a: List[String]): Option[Set[String]] = Some(a.grouped(2).map(_.mkString("/")).toSet)
  }

  def channelRead0(ctx: ChannelHandlerContext, request: FullHttpRequest): Unit = {
    val headers = WastedHttpHeaders.get(request)
    AuthValidator(request, headers, handleAuthorized(ctx, request, _))
  }

  private def handleAuthorized(ctx: ChannelHandlerContext, request: FullHttpRequest, authed: Boolean) {
    if (!authed) sendError(ctx, request, HttpResponseStatus.UNAUTHORIZED)
    else {
      // If this is a GET request which results in content, we forward it
      (request.getMethod, request.getUri.split("/").toList.drop(1)) match {
        case (GET, Nil) =>
          val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK, None, keepAlive = isKeepAlive(request)))
          if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)

        case (GET, "senders" :: Nil) =>
          val f = FlowSender.select.fetch()
          f.onFailure { case t => sendError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR) }
          f.onSuccess {
            case senders =>
              val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK,
                Some(Serialization.write(senders.seq)), Some("text/json"), isKeepAlive(request)))
              if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)
          }

        case (PUT, "sender" :: InetAddressParam(ipaddr) :: InetPrefixesParam(tba)) =>
          FlowSender.update.where(_.ip eqs ipaddr).modify(_.prefixes addAll tba).future()
          val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK, None, keepAlive = isKeepAlive(request)))
          if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)

        case (DELETE, "sender" :: InetAddressParam(ipaddr) :: Nil) =>
          FlowSender.delete.where(_.ip eqs ipaddr).future()
          val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK, None, keepAlive = isKeepAlive(request)))
          if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)

        case (DELETE, "sender" :: InetAddressParam(ipaddr) :: InetPrefixesParam(tbr)) =>
          FlowSender.update.where(_.ip eqs ipaddr).modify(_.prefixes removeAll tbr).future()
          val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK, None, keepAlive = isKeepAlive(request)))
          if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)

        case (POST, "stats" :: InetAddressParam(ipaddr) :: InetPrefixesParam(pfxs)) =>
          val requestBody = request.content().toString(CharsetUtil.UTF_8)
          val requestJson = JsonParser.parse(requestBody)
          //get/10.4.20.5/10.4.20.0/24/2001:4cea::/32  { years: [2014,2013,2012,2011], month: [201401, 201402, 201403] }
          println(requestJson)

          return ctx.close()
          val f = FlowSender.select.fetch()
          f.onFailure { case t => sendError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR) }
          f.onSuccess {
            case senders =>

              val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK,
                Some(Serialization.write(senders.seq)), Some("text/json"), isKeepAlive(request)))
              if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)
          }

        case _ =>
          sendError(ctx, request, HttpResponseStatus.NOT_FOUND)
      }
    }
  }
}