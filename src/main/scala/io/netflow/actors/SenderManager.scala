package io.netflow.actors

import java.net.InetAddress

import com.websudos.phantom.Implicits._
import io.netflow.flows.FlowSender
import io.netflow.lib._
import io.wasted.util._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }
import com.twitter.util._

private[netflow] object SenderManager extends Wactor {
  info("Starting up")
  private var senderActors = LruMap[InetAddress, Wactor.Address](5000)

  private case class GetActor(p: Promise[Wactor.Address], addr: InetAddress)
  private case class KillActor(addr: InetAddress)

  def findActorFor(sender: InetAddress): Future[Wactor.Address] = {
    val p = Promise[Wactor.Address]()
    this ! GetActor(p, sender)
    p.future
  }

  def removeActorFor(sender: InetAddress) {
    this ! KillActor(sender)
  }

  def receive = {
    case KillActor(sender: InetAddress) =>
      senderActors.get(sender).map(_ ! Wactor.Die)
      senderActors.remove(sender)

    case GetActor(p: Promise[Wactor.Address], sender: InetAddress) =>
      senderActors.get(sender).map(p.success) getOrElse Tryo {
        // Yes we are blocking here
        val duration = Duration(2, java.util.concurrent.TimeUnit.SECONDS)
        Await.result(FlowSender.select.where(_.ip eqs sender).get(), duration) match {
          case Some(psender) =>
            val actor = new SenderWorker(psender)
            senderActors.put(sender, actor)
            p.success(actor)
          case _ =>
            p.failure(new UnableToGetActorException(sender.getHostAddress))
        }
      }
  }

  def stop() {
    senderActors.cache.asMap.entrySet.asScala.foreach { _.getValue.value ! Wactor.Die }
    senderActors.cache.invalidateAll()
    info("Stopped")
  }
}
