package com.twitter.finagle.buoyant

import java.net.{InetAddress, UnknownHostException}
import java.security.cert.X509Certificate

import com.twitter.finagle.{Address, SslHostVerificationException}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.util.Try
import javax.net.ssl.SSLSession
import sun.security.util.HostnameChecker
import sun.security.x509.X500Name

/** Custom SSLClientSessionVerifier that also verifies the resolved ip against the CN in the certificate */
object IpSslClientSessionVerifier extends SslClientSessionVerifier {

  private[this] val checker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)

  /**
   * Run hostname verification on the session.  This will fail with a
   * [[com.twitter.finagle.SslHostVerificationException]] if the certificate is
   * invalid for the given session.
   *
   * This uses [[sun.security.util.HostnameChecker]]. Any bugs are theirs.
   */
  def apply(
    address: Address,
    config: SslClientConfiguration,
    session: SSLSession
  ): Boolean = {

    config.hostname match {
      case Some(host) =>

        // We take the first certificate from the given `getPeerCertificates` array since the expected
        // array structure is peer's own certificate first followed by any certificate authorities.
        val isValid = session.getPeerCertificates.headOption.exists {
          case x509: X509Certificate =>
            matchesIpAddress(x509, address) || Try(checker.`match`(host, x509)).isReturn
          case _ => false
        }

        if (isValid)
          true
        else
          throw SslHostVerificationException(session.getPeerPrincipal.getName)

      case None =>
        SslClientSessionVerifier.AlwaysValid(address, config, session)
    }
  }

  private def matchesIpAddress(x509: X509Certificate, address: Address): Boolean = {
    address match {
      case Address.Inet(inetSocketAddress, _) =>
        if (inetSocketAddress.getAddress != null) {
          val ip = inetSocketAddress.getAddress.getHostAddress
          val x500Name = HostnameChecker.getSubjectX500Name(x509)
          val commonName = x500Name.findMostSpecificAttribute(X500Name.commonName_oid)

          commonName != null &&
            (ip == commonName.getAsString ||
              toIpAddress(commonName.getAsString).exists(_.equals(inetSocketAddress.getAddress)))
        } else {
          false
        }
      case _ =>
        false
    }
  }

  private def toIpAddress(address: String): Option[InetAddress] = {
    try {
      Some(InetAddress.getByName(address))
    } catch {
      case uhe: UnknownHostException => None
      case se: SecurityException => None
    }
  }
}
