package io.netflow.lib

import io.netflow.actors._
import io.wasted.util._

import scala.collection.JavaConversions._
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.ConcurrentHashMap
import java.net.InetSocketAddress

/**
 * Our very own lookup service for all things netflow related
 */
object Service extends Logger {
  private val senderActors = new ConcurrentHashMap[(String, Int), Wactor.Address]()
  def findActorFor(osender: InetSocketAddress): Option[Wactor.Address] = synchronized {
    val sender = (osender.getAddress.getHostAddress, _: Int)
    Option(senderActors.get(sender(osender.getPort))) orElse Option(senderActors.get(sender(0))) match {
      case Some(actor) => Some(actor)
      case _ =>
        val backendP = Storage.start().get
        backendP.acceptFrom(osender) match {
          case Some(osender) =>
            Storage.stop(backendP)
            val actor = new SenderActor(osender, Storage.start().get)
            senderActors.put(sender(osender.getPort), actor)
            Some(actor)
          case _ =>
            Storage.stop(backendP)
            None
        }
    }
  }

  def removeActorFor(osender: InetSocketAddress) {
    val sender = (osender.getAddress.getHostAddress, osender.getPort)
    senderActors.remove(sender)
  }

  def start() {
    info("Starting up")
    Try(Storage.start().get) match {
      case Success(ba) => Storage.stop(ba)
      case Failure(e) => throw new IllegalArgumentException("Unable to connect to the specified database host")
    }
  }

  def stop() {
    senderActors.values foreach { _ ! Wactor.Die }
    info("Stopped")
  }
}

