package io.netflow.lib

private[netflow] object WastedHttpResponse extends io.wasted.util.http.HttpResponder(
  "netflow.io " + BuildInfo.version, false, false)

private[netflow] object WastedHttpHeaders extends io.wasted.util.http.Headers("*")
