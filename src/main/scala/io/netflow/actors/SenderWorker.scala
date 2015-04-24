package io.netflow.actors

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import com.twitter.conversions.time._
import com.twitter.util.Await
import io.netflow.flows._
import io.netflow.lib._
import io.netflow.storage.FlowSender
import io.wasted.util._

private[netflow] class SenderWorker(config: FlowSender) extends Wactor with Logger {
  override protected def loggerName = config.ip.getHostAddress

  private[actors] val senderPrefixes = new AtomicReference(config.prefixes)

  cflow.NetFlowV9Template
  private var templateCache: Map[Int, cflow.Template] = {
    Await.result(cflow.NetFlowV9Template.findAll(config.ip), 30 seconds).map(x => x.number -> x).toMap
  }
  info("Starting up with templates: " + templateCache.keys.mkString(", "))

  def templates = templateCache
  def setTemplate(tmpl: cflow.Template): Unit = templateCache += tmpl.number -> tmpl
  private var cancellable = Shutdown.schedule()

  private def handleFlowPacket(osender: InetSocketAddress, handled: Option[FlowPacket]) = {
    if (NodeConfig.values.storage.isDefined) handled match {
      case Some(fp) =>
        FlowManager.save(osender, fp, senderPrefixes.get.toList)
      case _ =>
        warn("Unable to parse FlowPacket")
        FlowManager.bad(osender)
    }
  }

  def receive = {
    case NetFlow(osender, buf) =>
      Shutdown.avoid()
      val handled: Option[FlowPacket] = {
        Tryo(buf.getUnsignedShort(0)) match {
          case Some(1) => cflow.NetFlowV1Packet(osender, buf).toOption
          case Some(5) => cflow.NetFlowV5Packet(osender, buf).toOption
          case Some(6) => cflow.NetFlowV6Packet(osender, buf).toOption
          case Some(7) => cflow.NetFlowV7Packet(osender, buf).toOption
          case Some(9) => cflow.NetFlowV9Packet(osender, buf, this).toOption
          case Some(10) =>
            info("We do not handle NetFlow IPFIX yet"); None //Some(cflow.NetFlowV10Packet(sender, buf))
          case _ => None
        }
      }
      buf.release()
      if (NodeConfig.values.netflow.persist) handled.foreach(_.persist())
      handleFlowPacket(osender, handled)

    case SFlow(osender, buf) =>
      Shutdown.avoid()
      if (buf.readableBytes < 28) {
        warn("Unable to parse FlowPacket")
        FlowManager.bad(osender)
      } else {
        val handled: Option[FlowPacket] = {
          Tryo(buf.getLong(0)) match {
            case Some(3) =>
              info("We do not handle sFlow v3 yet"); None // sFlow 3
            case Some(4) =>
              info("We do not handle sFlow v4 yet"); None // sFlow 4
            case Some(5) =>
              //sflow.SFlowV5Packet(sender, buf)
              info("We do not handle sFlow v5 yet"); None // sFlow 5
            case _ => None
          }
        }
        if (NodeConfig.values.sflow.persist) handled.foreach(_.persist())
        handleFlowPacket(osender, handled)
      }
      buf.release()

    case Shutdown =>
      info("Shutting down")
      SenderManager.removeActorFor(config.ip)
      templateCache = Map.empty
      this ! Wactor.Die
  }

  private case object Shutdown {
    def schedule() = scheduleOnce(Shutdown, 5.minutes)
    def avoid() {
      cancellable.cancel()
      cancellable = schedule()
    }
  }
}
