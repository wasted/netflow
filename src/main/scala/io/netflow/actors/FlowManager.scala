package io.netflow.actors

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicLong

import io.netflow.lib._
import io.netflow.storage._
import io.wasted.util._
import org.joda.time.DateTime

/**
 * This spawns just as many workers as configured
 */
private[netflow] object FlowManager extends Logger {
  if (NodeConfig.values.storage.isDefined) info("Starting up")

  private lazy val flowWorkers: Option[List[Wactor.Address]] = FlowWorker.get()
  private lazy val flowCounter = new AtomicLong()
  private def tryWorker = flowWorkers.map(_(flowCounter.getAndIncrement % NodeConfig.values.cores toInt))

  def stop() {
    flowWorkers.foreach { workers =>
      workers.foreach { _ ! Wactor.Die }
      info("Stopped")
    }
  }

  def bad(sender: InetSocketAddress): Unit = {
    tryWorker.foreach { _ ! BadDatagram(DateTime.now, sender.getAddress) }
  }

  def save(sender: InetSocketAddress, flowPacket: FlowPacket, pfx: List[InetPrefix]) = {
    tryWorker.foreach { _ ! SaveJob(sender, flowPacket, pfx) }
  }
}
