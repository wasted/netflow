package io.netflow.actors

import java.net.InetAddress

import com.twitter.conversions.time._
import com.twitter.util.{ Await, Future, Promise }
import io.netflow.storage.FlowSender
import io.wasted.util._

import scala.collection.JavaConverters._

private[netflow] object SenderManager extends Wactor {
  info("Starting up")
  private val senderActors = LruMap[InetAddress, Wactor.Address](5000)

  private case class GetActor(p: Promise[Wactor.Address], addr: InetAddress)
  private case class KillActor(addr: InetAddress)

  def findActorFor(sender: InetAddress): Future[Wactor.Address] = {
    val p = Promise[Wactor.Address]()
    this ! GetActor(p, sender)
    p
  }

  def removeActorFor(sender: InetAddress) {
    this ! KillActor(sender)
  }

  def receive = {
    case KillActor(sender: InetAddress) =>
      senderActors.get(sender).foreach(_ ! Wactor.Die)
      senderActors.remove(sender)

    case GetActor(p: Promise[Wactor.Address], sender: InetAddress) =>
      senderActors.get(sender).map(p.setValue) getOrElse {
        val dbsender = FlowSender.find(sender)
        try {
          val result = Await.result(dbsender, 2.seconds)
          val actor = new SenderWorker(result)
          senderActors.put(sender, actor)
          p.setValue(actor)
        } catch {
          case t: Throwable => p.setException(t)
        }
      }
  }

  def stop() {
    senderActors.cache.asMap.entrySet.asScala.foreach { _.getValue.value ! Wactor.Die }
    senderActors.cache.invalidateAll()
    info("Stopped")
  }
}
