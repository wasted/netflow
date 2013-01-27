package io.netflow

import io.netflow.actors._
import io.netflow.backends._
import io.wasted.util._

import java.util.concurrent.ConcurrentHashMap

import java.net.InetSocketAddress
import akka.actor._

/**
 * Our very own lookup service for all things netflow related
 */
object Service extends Logger {
  val backend: Storage = Storage.start().get
  val system = ActorSystem("netflow")

  private val senderActors = new ConcurrentHashMap[InetSocketAddress, ActorRef]()
  def findActorFor(sender: InetSocketAddress): Option[ActorRef] = senderActors.get(sender) match {
    case actor: ActorRef if actor != null => Some(actor)
    case _ =>
      backend.acceptFrom(sender) match {
        case Some(sender) =>
          val actor = Tryo(system.actorOf(Props(new SenderActor(sender, Storage.start().get)), sender.toString.replaceAll("/", ""))) match {
            case Some(actor) =>
              senderActors.put(sender, actor)
              actor
            case None => system.actorFor("akka://netflow/user/" + sender.toString.replaceAll(",", ""))
          }
          Some(actor)
        case _ => None
      }
  }

  def removeActorFor(sender: InetSocketAddress) {
    senderActors.remove(sender)
  }

  def start() {
    info("Starting up")
  }

  def stop() {
    info("Stopped")
  }
}

