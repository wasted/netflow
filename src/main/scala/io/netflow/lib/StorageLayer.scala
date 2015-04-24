package io.netflow.lib

object StorageLayer extends Enumeration {
  val Cassandra = Value("cql")
  val Redis = Value("redis")
  //val ElasticSearch = Value("elastic")
}
