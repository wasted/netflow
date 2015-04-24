package io.netflow.storage

import io.netflow.lib._
import io.wasted.util.Wactor

object FlowWorker {
  def get(): Option[List[Wactor.Address]] = NodeConfig.values.storage.map { storage =>
    (1 to NodeConfig.values.cores).toList.map { n =>
      storage match {
        case StorageLayer.Redis => new redis.FlowWorker(n)
        case StorageLayer.Cassandra => new cassandra.FlowWorker(n)
        //case StorageLayer.ElasticSearch =>
      }
    }
  }
}
