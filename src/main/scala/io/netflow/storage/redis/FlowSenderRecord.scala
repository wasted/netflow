package io.netflow
package storage
package redis

import java.net.InetAddress

import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer }
import com.twitter.util.Future
import io.netflow.lib._

private[netflow] object FlowSenderRecord extends FlowSenderMeta {
  def findAll(): Future[List[FlowSender]] = {
    Connection.client.sMembers(StringToChannelBuffer("senders")).map(_.map(CBToString(_))).flatMap { senders =>
      Future.collect(senders.map { sender =>
        val key = "prefixes:" + sender
        Connection.client.sMembers(StringToChannelBuffer(key)).map(_.map(CBToString(_)))
          .map(_.flatMap(string2prefix)).map { addrs =>
            storage.FlowSender(InetAddress.getByName(sender), None, addrs)
          }
      }.toSeq).map(_.toList)
    }
  }

  def find(inet: InetAddress): Future[FlowSender] = {
    val key = "prefixes:" + inet.getHostAddress
    Connection.client.sMembers(StringToChannelBuffer(key)).map(_.map(CBToString(_)))
      .map(_.flatMap(string2prefix)).map { addrs =>
        storage.FlowSender(inet, None, addrs)
      }
  }

  def save(sender: FlowSender): Future[FlowSender] = {
    val pfxs = sender.prefixes.map(p => StringToChannelBuffer(p.toString)).toList
    Connection.client.sAdd(StringToChannelBuffer("senders"), StringToChannelBuffer(sender.ip.getHostAddress) :: Nil).flatMap {
      senderAdded =>
        Connection.client.sAdd(StringToChannelBuffer("prefixes:" + sender.ip.getHostAddress), pfxs).map {
          pfxsAdded => sender
        }
    }
  }

  def delete(inet: InetAddress): Future[Unit] = {
    Connection.client.del(StringToChannelBuffer("prefixes:" + inet.getHostAddress) :: Nil).flatMap { pfxsDeleted =>
      Connection.client.sRem(StringToChannelBuffer("senders"), StringToChannelBuffer(inet.getHostAddress) :: Nil).map {
        done => ()
      }
    }
  }
}
