package io.netflow.flows

import java.net.InetSocketAddress

class FlowException(msg: String) extends Exception(msg)

class InvalidFlowVersionException(src: InetSocketAddress, version: Int) extends FlowException("Version " + version + " from " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalFlowDirectionException(src: InetSocketAddress, direction: Int, fd: NetFlowData[_]) extends FlowException("Direction " + direction + " from " + src.getAddress.getHostAddress + "/" + src.getPort + ": " + fd.toString)

class IncompleteFlowPacketHeaderException(src: InetSocketAddress) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)

class CorruptFlowPacketException(src: InetSocketAddress) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)

class CorruptFlowTemplateException(src: InetSocketAddress, template: Int) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalFlowSetLengthException(src: InetSocketAddress) extends FlowException("Length (0) from " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalTemplateIdException(src: InetSocketAddress, template: Int) extends FlowException("TemplateId " + template + " from " + src.getAddress.getHostAddress + "/" + src.getPort)

class UnhandledFlowPacketException(src: InetSocketAddress) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)
