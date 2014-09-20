package io.netflow.lib

// Since these expected errors are quite easy to distinguish and locate, we don't need expensive stacktraces
class FlowException(msg: String) extends Exception(msg) with util.control.NoStackTrace

class InvalidFlowVersionException(version: Int) extends FlowException("Version " + version)

class IllegalFlowDirectionException(direction: Int, fd: NetFlowData[_]) extends FlowException("Direction " + direction + ": " + fd.toString)

class IncompleteFlowPacketHeaderException extends FlowException("")

class CorruptFlowPacketException extends FlowException("")

class CorruptFlowTemplateException(template: Int) extends FlowException("TemplateId " + template)

class IllegalTemplateIdException(template: Int) extends FlowException("TemplateId " + template)

class UnhandledFlowPacketException extends FlowException("")

class IllegalFlowSetLengthException extends FlowException("FlowSetLength (0)")

class ShortFlowPacketException extends FlowException("")

class UnableToGetActorException(x: String) extends FlowException("Unable to get Actor for " + x)

class UnableToGetStorageException extends FlowException("")

