package io.netflow

import io.netty.handler.logging.LoggingHandler
import io.wasted.util._

object Node extends App with Logger {
  def start() {
    // OS Checking
    val os = System.getProperty("os.name").toLowerCase
    if (!(os.contains("nix") || os.contains("nux") || os.contains("bsd") || os.contains("mac") || os.contains("sunos")) || os.contains("win")) {
      warn("netflow.io has not been tested on %s", System.getProperty("os.name"))
    }

    // JVM Checking
    val jvmVersion = System.getProperty("java.runtime.version")
    if (!jvmVersion.matches("^1.[78].*$")) {
      error("Java Runtime %s is not supported", jvmVersion)
      return
    }

    // Create this dummy here to initialize Netty's logging before Akka's
    new LoggingHandler()
    Server.start()
  }

  def stop(): Unit = Server.stop()

  start()
}

