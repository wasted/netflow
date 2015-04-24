package io.netflow.lib

import java.net.InetAddress

import com.twitter.util.Future
import io.netflow.flows.cflow._

trait NetFlowTemplateMeta[T <: Template] {
  def findAll(inet: InetAddress): Future[Seq[NetFlowV9Template]]
}
