package io.buoyant.namer.curator

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.{Stack, Path}
import io.buoyant.namer.{NamerConfig, NamerInitializer}

class CuratorInitializer extends NamerInitializer {
  val configClass = classOf[CuratorConfig]
  override def configId = "io.l5d.curator"
}

object CuratorInitializer extends CuratorInitializer

case class CuratorConfig(zkConnectStr: String) extends NamerConfig {

  @JsonIgnore
  override def defaultPrefix: Path = Path.read("/io.l5d.curator")

  /**
   * Construct a namer.
   */
  def newNamer(params: Stack.Params) = new CuratorNamer(zkConnectStr)
}
