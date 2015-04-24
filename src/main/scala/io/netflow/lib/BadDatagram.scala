package io.netflow.lib

import java.net.InetAddress

import org.joda.time.DateTime

case class BadDatagram(date: DateTime, sender: InetAddress)
