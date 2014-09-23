package io.netflow.netty

import java.net.{ InetAddress, InetSocketAddress }

import com.websudos.phantom.Implicits._
import com.websudos.phantom.column.{ LowPriorityImplicits => _ }
import io.netflow.flows.FlowSender
import io.netflow.lib._
import io.netflow.timeseries._
import io.netty.channel._
import io.netty.handler.codec.http.HttpHeaders._
import io.netty.handler.codec.http.HttpMethod._
import io.netty.handler.codec.http._
import io.netty.handler.logging.LogLevel
import io.netty.util.CharsetUtil
import io.wasted.util._
import io.wasted.util.http.ExceptionHandler
import net.liftweb.json._

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

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

        case (GET, "senders" :: InetAddressParam(ipaddr) :: Nil) =>
          val f = FlowSender.select.where(_.ip eqs ipaddr).get()
          f.onFailure { case t => sendError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR) }
          f.onSuccess {
            case senders =>
              val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK,
                Some(Serialization.write(senders.seq)), Some("text/json"), isKeepAlive(request)))
              if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)
          }

        case (PUT, "senders" :: InetAddressParam(ipaddr) :: Nil) =>
          val requestBody = request.content().toString(CharsetUtil.UTF_8)
          JsonParser.parseOpt(requestBody) match {
            case None => sendError(ctx, request, HttpResponseStatus.BAD_REQUEST)
            case Some(json) =>
              val prefixes: Set[InetPrefix] = (json \ "prefixes").extractOpt[List[InetPrefix]].getOrElse(Nil).toSet
              FlowSender.update.where(_.ip eqs ipaddr)
                .modify(_.prefixes addAll prefixes.map(x => x.prefix.getHostAddress + "/" + x.prefixLen))
                .future()

              val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK, None, keepAlive = isKeepAlive(request)))
              if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)
          }

        case (DELETE, "senders" :: InetAddressParam(ipaddr) :: Nil) =>
          val requestBody = request.content().toString(CharsetUtil.UTF_8)
          if (requestBody.isEmpty) FlowSender.delete.where(_.ip eqs ipaddr).future()
          else JsonParser.parseOpt(requestBody) match {
            case None => sendError(ctx, request, HttpResponseStatus.BAD_REQUEST)
            case Some(json) =>
              val prefixes: Set[InetPrefix] = (json \ "prefixes").extractOpt[List[InetPrefix]].getOrElse(Nil).toSet

              FlowSender.update.where(_.ip eqs ipaddr)
                .modify(_.prefixes removeAll prefixes.map(x => x.prefix.getHostAddress + "/" + x.prefixLen))
                .future()
          }
          val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK, None, keepAlive = isKeepAlive(request)))
          if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)

        case (POST, "stats" :: InetAddressParam(sender) :: Nil) =>
          val requestBody = request.content().toString(CharsetUtil.UTF_8)
          Future {
            JsonParser.parseOpt(requestBody) match {
              case Some(json: JObject) =>
                val result = json.obj.map {
                  case JField(prefix, fields: JObject) =>
                    val pfxResult = fields.obj.map {
                      case JField(name, keysJ: JArray) =>
                        val keys: Map[String, List[String]] = keysJ.arr.flatMap {
                          case JString(date) => Some(date -> List("all"))
                          case JObject(JField(date, profiles: JArray) :: Nil) => profiles.extractOpt[List[String]].map(date -> _)
                          case x => None
                        }.toMap
                        val seriesFutures = keys.map {
                          case (date, profiles) =>
                            Future.sequence(profiles.map { profile =>
                              NetFlowSeries.select
                                .where(_.sender eqs sender)
                                .and(_.prefix eqs prefix)
                                .and(_.date eqs date)
                                .and(_.name eqs profile).fetch()
                            }).map { rows =>
                              date -> Extraction.decompose(rows.flatten[NetFlowSeriesRecord]).transform {
                                case JField("prefix", _) => JNothing
                                case JField("sender", _) => JNothing
                                case JField("date", _) => JNothing
                              }
                            }
                        }
                        val series = Future.sequence(seriesFutures)
                        val awaitedResult = Await.result(series, 1 minute).toMap
                        name -> Extraction.decompose(awaitedResult)
                    }.toMap
                    prefix -> pfxResult
                }.toMap
                val f = ctx.writeAndFlush(WastedHttpResponse.apply(HttpResponseStatus.OK,
                  Some(Serialization.write(result)), Some("text/json"), isKeepAlive(request)))
                if (!isKeepAlive(request)) f.addListener(ChannelFutureListener.CLOSE)

              case _ => sendError(ctx, request, HttpResponseStatus.BAD_REQUEST)
            }
          } onFailure {
            case t =>
              if (NodeConfig.values.debugStackTraces) t.printStackTrace()
              sendError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR)
          }

        case _ =>
          sendError(ctx, request, HttpResponseStatus.NOT_FOUND)
      }
    }
  }
}
