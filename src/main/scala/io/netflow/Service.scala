package io.netflow

import io.netflow.actors._
import io.netflow.backends._
import io.wasted.util._

import scala.collection.immutable.HashMap

import java.net.InetSocketAddress
import akka.actor._

/**
 * Our very own lookup service for all things netflow related
 */
private[netflow] object Service extends Logger {
  val backend: Storage = Storage.start().get
  val system = ActorSystem("netflow")

  private var senderActors = HashMap[InetSocketAddress, ActorRef]()
  def findActorFor(sender: InetSocketAddress): Option[ActorRef] = senderActors.get(sender) match {
    case Some(actor) => Some(actor)
    case None =>
      backend.acceptFrom(sender) match {
        case Some(sender) =>
          val actor = Tryo(system.actorOf(Props(new SenderActor(sender, Storage.start().get)), sender.toString.replaceAll("/", ""))) match {
            case Some(actor) =>
              senderActors ++= Map(sender -> actor)
              actor
            case None => system.actorFor("akka://netflow/user/" + sender.toString.replaceAll(",", ""))
          }
          Some(actor)
        case _ => None
      }
  }

  def removeActorFor(sender: InetSocketAddress) {
    senderActors -= sender
  }

  def start() {
    info("Starting up")
  }

  def stop() {
    info("Stopped")
  }
}

