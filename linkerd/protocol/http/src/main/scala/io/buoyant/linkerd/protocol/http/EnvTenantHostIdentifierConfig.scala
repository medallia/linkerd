package io.buoyant.linkerd.protocol.http

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.{Dtab, Path, Stack}
import com.twitter.finagle.http.Request
import io.buoyant.linkerd.IdentifierInitializer
import io.buoyant.linkerd.protocol.HttpIdentifierConfig
import io.buoyant.router.http.EnvTenantHostIdentifier
import io.buoyant.router.RoutingFactory.Identifier

class EnvTenantHostIdentifierInitializer extends IdentifierInitializer {
  val configClass = classOf[EnvTenantHostIdentifierConfig]
  override val configId = EnvTenantHostIdentifierConfig.kind
}

object EnvTenantHostIdentifierInitializer extends EnvTenantHostIdentifierInitializer

object EnvTenantHostIdentifierConfig {
  val kind = "com.medallia.envTenantHost"
}

class EnvTenantHostIdentifierConfig extends HttpIdentifierConfig {
  val defaultTenant: Option[String] = None

  override def newIdentifier(
    prefix: Path,
    baseDtab: () => Dtab = () => Dtab.base,
    routerParams: Stack.Params = Stack.Params.empty
  ): Identifier[Request] = EnvTenantHostIdentifier.mk(prefix, baseDtab, defaultTenant)
}
