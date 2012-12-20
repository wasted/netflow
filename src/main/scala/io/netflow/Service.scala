package io.netflow

import io.netflow.backends._
import io.wasted.util._

class OurBackend extends Redis with Backend[Redis]

/**
 * Our very own lookup service for all things netflow related
 */
private[netflow] object Service extends Logger {
  val backend: Backend[Redis] = new OurBackend()

  def start() {
    info("Starting up")
  }

  def stop() {
    info("Stopped")
  }
}

