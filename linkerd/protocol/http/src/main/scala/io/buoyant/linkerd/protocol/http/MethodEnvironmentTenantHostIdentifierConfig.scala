package io.buoyant.linkerd.protocol.http

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.{Dtab, Path}
import io.buoyant.linkerd.IdentifierInitializer
import io.buoyant.linkerd.protocol.HttpIdentifierConfig
import io.buoyant.router.http.MethodEnvironmentTenantHostIdentifier

class MethodEnvironmentTenantHostIdentifierInitializer extends IdentifierInitializer {
  val configClass = classOf[MethodEnvironmentTenantHostIdentifierConfig]
  override val configId = MethodEnvironmentTenantHostIdentifierConfig.kind
}

object MethodEnvironmentTenantHostIdentifierInitializer extends MethodEnvironmentTenantHostIdentifierInitializer

object MethodEnvironmentTenantHostIdentifierConfig {
  val kind = "com.medallia.methodEnvironmentTenantHost"
}

class MethodEnvironmentTenantHostIdentifierConfig extends HttpIdentifierConfig {

  @JsonIgnore
  override def newIdentifier(
    prefix: Path,
    baseDtab: () => Dtab = () => Dtab.base
  ) = MethodEnvironmentTenantHostIdentifier.mk(prefix, baseDtab)
}
