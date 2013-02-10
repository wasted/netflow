package io.netflow

import io.netflow.actors._
import io.netflow.backends._
import io.wasted.util._

import scala.collection.JavaConversions._
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.ConcurrentHashMap
import java.net.InetSocketAddress

/**
 * Our very own lookup service for all things netflow related
 */
object Service extends Logger {
  val backend: Storage = Try(Storage.start().get) match {
    case Success(ba) => ba
    case Failure(e) => throw new IllegalArgumentException("Unable to connect to the specified database host")
  }

  private val senderActors = new ConcurrentHashMap[(String, Int), Wactor.Address]()
  def findActorFor(osender: InetSocketAddress): Option[Wactor.Address] = {
    val sender = (osender.getAddress.getHostAddress, _: Int)
    Option(senderActors.get(sender(osender.getPort))) orElse Option(senderActors.get(sender(0))) match {
      case Some(actor) => Some(actor)
      case _ =>
        backend.acceptFrom(osender) match {
          case Some(osender) =>
            val actor = new SenderActor(osender, Storage.start().get)
            senderActors.put(sender(osender.getPort), actor)
            Some(actor)
          case _ => None
        }
    }
  }

  def removeActorFor(osender: InetSocketAddress) {
    val sender = (osender.getAddress.getHostAddress, osender.getPort)
    senderActors.remove(sender)
  }

  def start() {
    info("Starting up")
  }

  def stop() {
    senderActors.values foreach { _ ! Wactor.Die }
    info("Stopped")
  }
}

