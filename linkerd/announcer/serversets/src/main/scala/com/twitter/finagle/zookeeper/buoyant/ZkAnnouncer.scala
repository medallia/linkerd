package com.twitter.finagle.zookeeper.buoyant

import com.twitter.finagle.{Announcement, Announcer}
import com.twitter.finagle.zookeeper.{ZkAnnouncer => FZkAnnouncer, DefaultZkClientFactory}
import com.twitter.util.Future
import io.buoyant.config.types.HostAndPort
import java.net.InetSocketAddress

class ZkAnnouncer(zkAddrs: Seq[HostAndPort], pathPrefix: String) extends Announcer {
  override val scheme: String = "zk-serversets"

  val underlying = new FZkAnnouncer(DefaultZkClientFactory)

  private[this] val connect = zkAddrs.map(_.toString).mkString(",")

  override def announce(addr: InetSocketAddress, name: String): Future[Announcement] =
    underlying.announce(addr, s"$connect!$pathPrefix/$name!0")
}
