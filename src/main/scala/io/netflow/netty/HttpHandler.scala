package io.netflow.netty

import java.net.InetAddress

import com.twitter.conversions.time._
import com.twitter.util.{Await, Future}
import io.netflow.lib._
import io.netflow.storage.{FlowSender, NetFlowSeries}
import io.netty.channel.Channel
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import io.wasted.util._
import net.liftweb.json.JsonDSL._
import net.liftweb.json._

private[netty] object InetAddressParam {
  def unapply(a: String): Option[InetAddress] = Tryo(InetAddress.getByName(a))
}

private[netty] object InetPrefixesParam {
  def unapply(a: List[String]): Option[Set[String]] = Some(a.grouped(2).map(_.mkString("/")).toSet)
}

private[netflow] object AdminAPIHandler extends Logger {
  def dispatch(chan: Channel, freq: Future[FullHttpRequest]): Future[HttpResponse] = freq.flatMap { request =>
    // If this is a GET request which results in content, we forward it
    val qsd = new QueryStringDecoder(request.getUri)
    val parts = qsd.path.replaceAll("^/", "").split("/").toList
    (request.getMethod, parts) match {
      case (HttpMethod.GET, Nil) => Future.value(HttpResponse.OK())

      case (HttpMethod.GET, "senders" :: Nil) =>
        FlowSender.findAll().map(Serialization.write(_)).map(HttpResponse.OK(_))

      case (HttpMethod.GET, "senders" :: InetAddressParam(ipaddr) :: Nil) =>
        FlowSender.find(ipaddr).map(Serialization.write(_)).map(HttpResponse.OK(_))

      case (HttpMethod.PUT, "senders" :: InetAddressParam(ipaddr) :: Nil) =>
        val requestBody = request.content().toString(CharsetUtil.UTF_8)
        JsonParser.parseOpt(requestBody) match {
          case None => Future.value(HttpResponse.BadRequest("Did not contain a JSON-payload"))
          case Some(json) =>
            val prefixes: Set[InetPrefix] = (json \ "prefixes").extractOpt[List[InetPrefix]].getOrElse(Nil).toSet
            FlowSender(ipaddr, None, prefixes).save().map(Serialization.write(_)).map(HttpResponse.OK(_))
        }

      case (HttpMethod.DELETE, "senders" :: InetAddressParam(ipaddr) :: Nil) =>
        val requestBody = request.content().toString(CharsetUtil.UTF_8)
        if (requestBody.isEmpty) FlowSender.delete(ipaddr).map { done => HttpResponse.OK() }
        else JsonParser.parseOpt(requestBody) match {
          case None => Future.value(HttpResponse.BadRequest("Did not contain a JSON-payload"))
          case Some(json) =>
            val prefixes: Set[InetPrefix] = (json \ "prefixes").extractOpt[List[InetPrefix]].getOrElse(Nil).toSet
            FlowSender.find(ipaddr).flatMap { sender =>
              sender.copy(prefixes = sender.prefixes -- prefixes).save().map { sender =>
                HttpResponse.OK(Serialization.write(sender))
              }
            }
        }

      case (HttpMethod.POST, "stats" :: InetAddressParam(sender) :: Nil) =>
        Future {
          val requestBody = request.content().toString(CharsetUtil.UTF_8)
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
                        case (date, profiles) => NetFlowSeries(date, profiles, sender, prefix).map(date -> _: JObject)
                      }.toList
                      val series = Future.collect(seriesFutures)
                      val awaitedResult = Await.result(series, 1 minute)
                      name -> Extraction.decompose(awaitedResult)
                  }.toMap
                  prefix -> pfxResult
              }.toMap
              HttpResponse.OK(Serialization.write(result))

            case _ => HttpResponse.NotFound()
          }
        }.rescue {
          case t: Throwable =>
            t.printStackTrace()
            Future(HttpResponse.InternalServerError(t.getMessage))
        }

      case _ => Future.value(HttpResponse.NotFound())
    }
  }
}
