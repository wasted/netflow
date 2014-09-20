package io.netflow.lib

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ ConstantReconnectionPolicy, DefaultRetryPolicy }
import io.netflow.flows.FlowSender
import io.netflow.flows.cflow._
import io.wasted.util.Logger

import scala.collection.JavaConverters._

object CassandraConnection extends Logger {
  private val hosts = NodeConfig.values.cassandra.hosts.mkString(",")
  private val client = {
    debug(s"Opening new connection to $hosts")

    val poolingOpts = new PoolingOptions
    poolingOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, NodeConfig.values.cassandra.minConns)
    poolingOpts.setMaxConnectionsPerHost(HostDistance.LOCAL, NodeConfig.values.cassandra.maxConns)
    poolingOpts.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, NodeConfig.values.cassandra.minSimRequests)
    poolingOpts.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, NodeConfig.values.cassandra.maxSimRequests)

    poolingOpts.setCoreConnectionsPerHost(HostDistance.REMOTE, NodeConfig.values.cassandra.minConns)
    poolingOpts.setMaxConnectionsPerHost(HostDistance.REMOTE, NodeConfig.values.cassandra.maxConns)
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
    FlowSender.create.execute()
    FlowSender.createIndexes()

    NetFlowV1.create.execute()
    NetFlowV1.createIndexes()

    NetFlowV5.create.execute()
    NetFlowV5.createIndexes()

    NetFlowV6.create.execute()
    NetFlowV6.createIndexes()

    NetFlowV7.create.execute()
    NetFlowV7.createIndexes()

    NetFlowV9Template.create.execute()
    NetFlowV9Template.createIndexes()
    NetFlowV9Data.create.execute()
    NetFlowV9Data.createIndexes()
    NetFlowV9Option.create.execute()
    NetFlowV9Option.createIndexes()

    /* FIXME netflow 10
    NetFlowV10.create.execute()
    NetFlowV10.createIndexes()
    */
  }
}
