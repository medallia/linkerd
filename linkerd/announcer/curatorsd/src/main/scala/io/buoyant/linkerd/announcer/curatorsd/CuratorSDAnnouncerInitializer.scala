package io.buoyant.linkerd.announcer.curatorsd

import com.twitter.finagle.Announcer
import io.buoyant.linkerd.{AnnouncerConfig, AnnouncerInitializer}

class CuratorSDAnnouncerInitializer extends AnnouncerInitializer {
  override def configClass = classOf[CuratorSDConfig]
  override def configId = "io.l5d.curatorsd"
}

case class CuratorSDConfig(zkConnectStr: String, tenant: Option[String]) extends AnnouncerConfig {
  override def mk(): Announcer = new CuratorSDAnnouncer(zkConnectStr, tenant)
}
