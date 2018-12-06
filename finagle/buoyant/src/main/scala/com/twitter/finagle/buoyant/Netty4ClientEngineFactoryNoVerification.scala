package com.twitter.finagle.buoyant

import com.twitter.finagle.Address
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslConfigurations
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.OpenSsl

/**
 * Similar to com.twitter.finagle.netty4.ssl.client.Netty4ClientEngineFactory, but this one doesn't
 * specify the HTTPS endpointIdentificationAlgorithm, to avoid the default hostname verification.
 *
 * Instead, we use the finagle IpSslClientSessionVerifier, which first checks the address ip and the uses
 * the platform's HostnameChecker. MSF-31 will address its implementation use of sun internal apis.
 *
 * One alternative path could be to build a TrustManager wrapper, which is a bit more involved.
 *
 * In any case, this is a temporary change until we can dynamically issue certificates using service names
 * instead of IPs.
 */
final class Netty4ClientEngineFactoryNoVerification(allocator: ByteBufAllocator, forceJdk: Boolean)
  extends SslClientEngineFactory {

  /**
   * Creates a new `Engine` based on an `Address` and an `SslClientConfiguration`.
   *
   * @param address A physical address which potentially includes metadata.
   *
   * @param config A collection of parameters which the engine factory should
   * consider when creating the TLS client `Engine`.
   *
   * @note `ApplicationProtocols` other than Unspecified are only supported
   * by using a native engine via netty-tcnative.
   */
  def apply(address: Address, config: SslClientConfiguration): Engine = {
    val context = Netty4ClientSslConfigurations.createClientContext(config, forceJdk)
    val engine =
      Netty4ClientSslConfigurations.createClientEngine(address, config, context, allocator)
    removeAlgorithm(engine.self)
    engine
  }

  private def removeAlgorithm(engine: javax.net.ssl.SSLEngine) {
    val sslParameters = engine.getSSLParameters
    sslParameters.setEndpointIdentificationAlgorithm(null)
    engine.setSSLParameters(sslParameters)
  }
}

object Netty4ClientEngineFactoryNoVerification {

  /**
   * Creates an instance of the [[Netty4ClientEngineFactoryNoVerification]] using the
   * allocator defined for use by default in Finagle-Netty4.
   *
   * @param forceJdk Indicates whether the underlying `SslProvider` should
   * be forced to be the Jdk version instead of the native version if
   * available.
   */
  def apply(forceJdk: Boolean): Netty4ClientEngineFactoryNoVerification = {
    val allocator = Allocator.allocatorParam.default.allocator
    new Netty4ClientEngineFactoryNoVerification(allocator, forceJdk)
  }

  /**
   * Creates an instance of the [[Netty4ClientEngineFactoryNoVerification]] using the
   * specified allocator.
   *
   * @param allocator The allocator which should be used as part of
   * `Engine` creation. See Netty's `SslContextBuilder` docs for
   * more information.
   * @note Whether this engine factory should be forced to use the
   * Jdk version is determined by whether Netty is able to load
   * a native engine library via netty-tcnative.
   */
  def apply(allocator: ByteBufAllocator): Netty4ClientEngineFactoryNoVerification =
    new Netty4ClientEngineFactoryNoVerification(allocator, !OpenSsl.isAvailable)

  /**
   * Creates an instance of the [[Netty4ClientEngineFactoryNoVerification]] using the
   * default allocator.
   *
   * @note Whether this engine factory should be forced to use the
   * Jdk version is determined by whether Netty is able to load
   * a native engine library via netty-tcnative.
   */
  def apply(): Netty4ClientEngineFactoryNoVerification =
    apply(!OpenSsl.isAvailable)

}