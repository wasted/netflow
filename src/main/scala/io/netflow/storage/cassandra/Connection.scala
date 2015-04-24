package io.netflow.storage.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ ConstantReconnectionPolicy, DefaultRetryPolicy }
import io.netflow.lib.NodeConfig
import io.netflow.storage.{ Connection => ConnectionMeta }
import io.wasted.util.Logger

import scala.collection.JavaConverters._

private[storage] object Connection extends ConnectionMeta with Logger {
  private val hosts = NodeConfig.values.cassandra.hosts.mkString(",")
  private val client = {
    info(s"Opening new connection to $hosts")

    val poolingOpts = new PoolingOptions
    poolingOpts.setMaxConnectionsPerHost(HostDistance.LOCAL, NodeConfig.values.cassandra.maxConns)
    poolingOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, NodeConfig.values.cassandra.minConns)

    poolingOpts.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, NodeConfig.values.cassandra.minSimRequests)
    poolingOpts.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, NodeConfig.values.cassandra.maxSimRequests)

    poolingOpts.setMaxConnectionsPerHost(HostDistance.REMOTE, NodeConfig.values.cassandra.maxConns)
    poolingOpts.setCoreConnectionsPerHost(HostDistance.REMOTE, NodeConfig.values.cassandra.minConns)

    poolingOpts.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, NodeConfig.values.cassandra.minSimRequests)
    poolingOpts.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, NodeConfig.values.cassandra.maxSimRequests)

    val socketOpts = new SocketOptions
    socketOpts.setConnectTimeoutMillis(NodeConfig.values.cassandra.connectTimeout)
    socketOpts.setReadTimeoutMillis(NodeConfig.values.cassandra.readTimeout)
    socketOpts.setKeepAlive(true)
    socketOpts.setTcpNoDelay(true)
    socketOpts.setReuseAddress(true)
    socketOpts.setSoLinger(0)

    //val queryOpts = new QueryOptions
    //withQueryOptions(queryOpts).

    Cluster.builder().
      addContactPoint(hosts).
      withCompression(ProtocolOptions.Compression.SNAPPY).
      withPoolingOptions(poolingOpts).
      withSocketOptions(socketOpts).
      withRetryPolicy(DefaultRetryPolicy.INSTANCE).
      withReconnectionPolicy(new ConstantReconnectionPolicy(NodeConfig.values.cassandra.reconnectTimeout)).
      build()
  }

  lazy val metadata = {
    val md = client.getMetadata
    md.getAllHosts.asScala.foreach { host =>
      debug("Datacenter: %s, Host: %s, Rack: %s", host.getDatacenter, host.getAddress, host.getRack)
    }
    md
  }

  def shutdown() {
    info("Shutting down")
    session.close()
  }

  implicit lazy val session = {
    val session = client.connect()
    metadata
    val keyspace = NodeConfig.values.cassandra.keyspace
    if (session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" + keyspace + "';").all().size() == 0) {
      info("Creating Keyspace")
      session.execute("CREATE KEYSPACE " + keyspace + " " + NodeConfig.values.cassandra.keyspaceConfig + ";")
    }
    session.execute("USE " + keyspace + ";")
    session
  }

  def start(): Unit = {
    val keyspace = NodeConfig.values.cassandra.keyspace
    debug("Opening new connection to " + hosts + " keyspace " + keyspace)
    session

    debug("Creating schema in database")
    FlowSenderRecord.create.execute()
    FlowSenderRecord.createIndexes()

    FlowSenderCountRecord.create.execute()
    FlowSenderCountRecord.createIndexes()

    NetFlowV1Record.create.execute()
    NetFlowV1Record.createIndexes()

    NetFlowV5Record.create.execute()
    NetFlowV5Record.createIndexes()

    NetFlowV6Record.create.execute()
    NetFlowV6Record.createIndexes()

    NetFlowV7Record.create.execute()
    NetFlowV7Record.createIndexes()

    NetFlowV9TemplateRecord.create.execute()
    NetFlowV9TemplateRecord.createIndexes()
    NetFlowV9DataRecord.create.execute()
    NetFlowV9DataRecord.createIndexes()
    NetFlowV9OptionRecord.create.execute()
    NetFlowV9OptionRecord.createIndexes()

    NetFlowSeries.create.execute()
    NetFlowSeries.createIndexes()

    NetFlowStats.create.execute()
    NetFlowStats.createIndexes()

    /* FIXME netflow 10
    NetFlowV10.create.execute()
    NetFlowV10.createIndexes()
    */
  }
}
