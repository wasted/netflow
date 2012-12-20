package io.netflow.flows.cisco

import java.net.{ InetAddress, InetSocketAddress }

class FlowException(msg: String) extends Exception(msg)

class IncompleteFlowPacketHeaderException(src: InetSocketAddress) extends FlowException("Incomplete FlowPacket Header from " + src.getAddress.getHostAddress + "/" + src.getPort)

class CorruptFlowPacketException(src: InetSocketAddress) extends FlowException("Corrupt FlowPacket from " + src.getAddress.getHostAddress + "/" + src.getPort)

class CorruptFlowTemplateException(src: InetSocketAddress, template: Int) extends FlowException("Corrupt FlowTemplate from " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalFlowSetLengthException(src: InetSocketAddress) extends FlowException("Invalid FlowSet length (0) from " + src.getAddress.getHostAddress + "/" + src.getPort)

class IllegalTemplateIdException(src: InetSocketAddress, template: Int) extends Exception(s"Illegal TemplateId $template from " + src.getAddress.getHostAddress + "/" + src.getPort)

