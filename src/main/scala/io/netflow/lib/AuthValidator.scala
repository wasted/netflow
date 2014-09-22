package io.netflow.lib

import io.wasted.util._

import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil

import scala.collection.JavaConversions._

/**
 * Handles Requests and authorizes users
 *
 */
private[netflow] object AuthValidator {
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

  /**
   * Handles HTTP Requests
   *
   * @param req Netty HttpRequest object
   * @param headers HttpHeaders for this request
   * @param handleAuth Function to handle things after the authentication
   */
  def apply(req: FullHttpRequest, headers: io.wasted.util.http.HttpHeaders, handleAuth: (Boolean) => Unit) = Tryo {
    val uri = new QueryStringDecoder(req.getUri, CharsetUtil.UTF_8)
    val h = headers
    val p = uri.parameters.map(v => v._1 -> v._2.toList.mkString).toMap
    def getHP(a: String, b: String) = for { str <- h.get(a) orElse p.get(b) } yield str.toString
    val resp = for {
      authKey <- getHP("X-Io-Auth", "auth")
      signHash <- getHP("X-Io-Sign", "sign")
    } yield {
      val body = (p.get("payload"), req.content.toString(CharsetUtil.UTF_8)) match {
        case (Some(b64j), _) => Some(Base64.decodeString(b64j))
        case (_, a: String) => Some(a)
        case _ => None
      }
      authorizeWithPlatform(authKey, signHash, body)
    }
    handleAuth(resp getOrElse false)
  }
}

