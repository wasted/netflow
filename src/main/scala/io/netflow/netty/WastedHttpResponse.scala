package io.netflow.netty

import io.netflow.lib._
import io.netty.handler.codec.http._
import io.wasted.util._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.{ JObject, Serialization }

// Objects to produce some standard http responses.
private[this] object WastedHttpResponse extends http.HttpResponder("netflow.io") {
  override def apply(status: HttpResponseStatus, body: Option[String] = None, mime: Option[String] = None, keepAlive: Boolean = false, headers: Map[String, String] = Map.empty): FullHttpResponse = {
    val resp = super.apply(status, body, mime, keepAlive, headers)
    resp.headers.set(HttpHeaders.Names.CACHE_CONTROL, HttpHeaders.Values.NO_CACHE)
    resp
  }

  def respond(status: HttpResponseStatus, body: Option[String] = None, mime: Option[String] = None, keepAlive: Boolean = false, headers: Map[String, String] = Map.empty): HttpResponse = {
    apply(status, body, mime, keepAlive, headers)
  }

  object OK {
    def apply(): HttpResponse = respond(HttpResponseStatus.OK)
    def apply(resp: String): HttpResponse = respond(HttpResponseStatus.OK, Some(resp))
    def apply(jo: JObject): HttpResponse = respond(HttpResponseStatus.OK, Some(Serialization.write(jo)))
  }

  object NotFound {
    def apply(): HttpResponse = respond(HttpResponseStatus.NOT_FOUND, Some("""{"status":"NOT_FOUND"}"""))
    def apply(message: String) = {
      val json = Serialization.write(("status" -> "NOT_FOUND") ~ ("message" -> message))
      respond(HttpResponseStatus.NOT_FOUND, Some(json))
    }
  }

  object Unauthorized {
    def apply(): HttpResponse = respond(HttpResponseStatus.UNAUTHORIZED, Some("""{"status":"UNAUTHORIZED"}"""))
    def apply(message: String) = {
      val json = Serialization.write(("status" -> "UNAUTHORIZED") ~ ("message" -> message))
      respond(HttpResponseStatus.UNAUTHORIZED, Some(json))
    }
  }

  object InternalServerError {
    def apply(message: String) = {
      val json = Serialization.write(("status" -> "INTERNAL_SERVER_ERROR") ~ ("message" -> message))
      respond(HttpResponseStatus.INTERNAL_SERVER_ERROR, Some(json))
    }
  }

  object BadRequest {
    def apply(message: String) = {
      val json = Serialization.write(("status" -> "BAD_REQUEST") ~ ("message" -> message))
      respond(HttpResponseStatus.BAD_REQUEST, Some(json))
    }
  }
}