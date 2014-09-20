package io.netflow.actors

import java.net.InetAddress

import com.websudos.phantom.Implicits._
import io.netflow.flows.FlowSender
import io.netflow.lib._
import io.wasted.util._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }

private[netflow] object SenderManager extends Wactor {
  info("Starting up")
  private var senderActors = Map[InetAddress, Wactor.Address]()

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
    case KillActor(addr: InetAddress) => senderActors -= addr
    case GetActor(p: Promise[Wactor.Address], sender: InetAddress) =>
      senderActors.get(sender).map(p.success) getOrElse {
        val f = FlowSender.select.where(_.ip eqsToken sender).get()
        f.onFailure(f => p.failure(f))

        f.onSuccess {
          case Some(psender) =>
            val actor = new SenderWorker(sender)
            senderActors += sender -> actor
            p.success(actor)
          case _ =>
            p.failure(new UnableToGetActorException(sender.getHostAddress))
        }
      }
  }

  def stop() {
    senderActors.values foreach { _ ! Wactor.Die }
    info("Stopped")
  }
}
