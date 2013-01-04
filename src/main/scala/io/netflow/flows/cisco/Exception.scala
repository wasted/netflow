package io.netflow.flows.cisco

import io.netflow.flows.FlowData

import java.net.InetSocketAddress

class FlowException(msg: String) extends Exception(msg)

class IllegalFlowDirectionException(src: InetSocketAddress, direction: Int, fd: FlowData) extends FlowException("Direction " + direction + " from " + src.getAddress.getHostAddress + "/" + src.getPort + ":\n\t" + fd.toString)

class IncompleteFlowPacketHeaderException(src: InetSocketAddress) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)

class CorruptFlowPacketException(src: InetSocketAddress) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)

class CorruptFlowTemplateException(src: InetSocketAddress, template: Int) extends FlowException("From " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalFlowSetLengthException(src: InetSocketAddress) extends FlowException("Length (0) from " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalTemplateIdException(src: InetSocketAddress, template: Int) extends Exception("TemplateId " + template + " from " + src.getAddress.getHostAddress + "/" + src.getPort)

