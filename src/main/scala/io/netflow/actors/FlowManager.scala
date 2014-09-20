package io.netflow.actors

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicLong

import io.netflow.lib._
import io.wasted.util._
import org.joda.time.DateTime

/**
 * This spawns just as many workers as configured
 */
private[netflow] object FlowManager extends Logger {
  info("Starting up")

  private val flowWorker: List[Wactor.Address] = (1 to NodeConfig.values.cores).toList.map(new FlowWorker(_))
  private val flowCounter = new AtomicLong()
  private def worker = flowWorker((flowCounter.getAndIncrement % NodeConfig.values.cores).toInt)

  def stop() {
    flowWorker foreach { _ ! Wactor.Die }
    info("Stopped")
  }

  def bad(sender: InetSocketAddress): Unit = {
    worker ! BadDatagram(DateTime.now, sender.getAddress)
  }

  def save(sender: InetSocketAddress, flowPacket: FlowPacket, pfx: List[InetPrefix], tpfx: List[InetPrefix]) = {
    worker ! SaveJob(sender, flowPacket, pfx, tpfx)
  }
}
