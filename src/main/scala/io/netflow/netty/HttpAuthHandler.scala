package io.netflow.netty

import com.twitter.util.Future
import io.netflow.lib.NodeConfig
import io.netty.channel.Channel
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import io.wasted.util._

import scala.collection.JavaConverters._

/**
 * Handles Requests and authorizes users
 *
 */
private[netflow] object HttpAuthHandler {
  /**
   * Authorize and validate JSON Signature
   *
   * @param authKey Platform authKey to be validated with
   * @param signHash Provided Hash of the body (or authKey)
   * @param body Optional JSON Body to be sent
   */
  private def authorizeWithPlatform(authKey: String, signHash: String, body: Option[String]): Boolean = {
    if (authKey != NodeConfig.values.admin.authKey.toString) return false
    val signKey = NodeConfig.values.admin.signKey
    val signedBody = body != None && Hashing.sign(signKey.toString, body.get) == signHash
    val signedAuthKey = Hashing.sign(signKey.toString, authKey) == signHash

    signedBody || signedAuthKey
  }

  def apply(chan: Channel, freq: Future[FullHttpRequest]): Future[HttpResponse] = freq.flatMap { request =>
    val uri = new QueryStringDecoder(request.getUri, CharsetUtil.UTF_8)
    val h = request.headers()
    val p: Map[String, String] = uri.parameters().asScala.map(v => v._1 -> v._2.asScala.mkString).toMap
    val resp: Option[Boolean] = for {
      authKey <- Option(h.get("X-Io-Auth")) orElse p.get("auth")
      signHash <- Option(h.get("X-Io-Sign")) orElse p.get("sign")
      body = (p.get("payload"), request.content().toString(CharsetUtil.UTF_8)) match {
        case (Some(b64j), _) => Some(Base64.decodeString(b64j))
        case (_, a: String) => Some(a)
        case _ => Some(request.getUri)
      }
    } yield authorizeWithPlatform(authKey, signHash, body)

    if (resp.exists(_ == true)) HttpHandler.dispatch(chan, freq)
    else Future.value(WastedHttpResponse.Unauthorized("API Key unknown or signature is bad"))
  }
}

