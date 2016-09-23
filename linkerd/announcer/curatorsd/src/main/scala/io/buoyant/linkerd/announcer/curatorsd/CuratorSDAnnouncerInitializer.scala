package io.buoyant.linkerd.announcer.curatorsd

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.Path
import io.buoyant.linkerd.{Announcer, AnnouncerConfig, AnnouncerInitializer}

class CuratorSDAnnouncerInitializer extends AnnouncerInitializer {
  override def configClass = classOf[CuratorSDConfig]
  override def configId = "io.l5d.curatorsd"
}

case class CuratorSDConfig(zkConnectStr: String) extends AnnouncerConfig {

  @JsonIgnore
  override def defaultPrefix: Path = Path.read("/io.l5d.curatorsd")

  override def mk(): Announcer = new CuratorSDAnnouncer(zkConnectStr)
}

