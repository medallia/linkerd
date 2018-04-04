package com.twitter.finagle.buoyant

import java.net.InetSocketAddress
import java.security.Principal
import java.security.cert.{Certificate, CertificateFactory}

import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.{Address, SslHostVerificationException}
import javax.net.ssl.{SSLSession, SSLSessionContext}
import javax.security.auth.x500.X500Principal
import javax.security.cert.X509Certificate
import org.scalatest.FunSuite

class IpSslClientSessionVerifierTest extends FunSuite {

  test("happy ip path") {
    val address = Address.Inet(new InetSocketAddress("127.0.0.1", 1000), Map())
    val sslClientConfig = SslClientConfiguration(hostname = Some("hostDoesntMatter"))
    val sslSession = sslSessionForCert(readCert("localhostCert.pem"))

    assert(IpSslClientSessionVerifier.apply(address, sslClientConfig, sslSession))
  }

  test("happy ip path 2") {
    val address = Address.Inet(new InetSocketAddress("1.1.1.1", 1000), Map())
    val sslClientConfig = SslClientConfiguration(hostname = Some("hostDoesntMatter"))
    val sslSession = sslSessionForCert(readCert("allOnesCert.pem"))

    assert(IpSslClientSessionVerifier.apply(address, sslClientConfig, sslSession))
  }

  test("happy path service name") {
    val address = Address.Inet(new InetSocketAddress("1.1.1.1", 1000), Map())
    val sslClientConfig = SslClientConfiguration(hostname = Some("MYserVIcename"))
    val sslSession = sslSessionForCert(readCert("servicenameCert.pem"))

    assert(IpSslClientSessionVerifier.apply(address, sslClientConfig, sslSession))
  }

  test("wrong ip") {
    val address = Address.Inet(new InetSocketAddress("127.0.0.2", 1000), Map())
    val sslClientConfig = SslClientConfiguration(hostname = Some("hostDoesntMatter"))
    val sslSession = sslSessionForCert(readCert("localhostCert.pem"))

    assertThrows[SslHostVerificationException](IpSslClientSessionVerifier.apply(address, sslClientConfig, sslSession))
  }

  private def readCert(resourceName: String): Certificate = {
    CertificateFactory
      .getInstance("X.509")
      .generateCertificate(getClass.getResourceAsStream(resourceName))
  }

  private def sslSessionForCert(cert: Certificate): SSLSession = {
    new SSLSession {
      override def getPeerPort: Int = ???

      override def getCipherSuite: String = ???

      override def getPacketBufferSize: Int = ???

      override def getLocalPrincipal: Principal = ???

      override def getLocalCertificates: Array[Certificate] = ???

      override def getId: Array[Byte] = ???

      override def getLastAccessedTime: Long = ???

      override def getPeerHost: String = ???

      override def getPeerCertificates: Array[Certificate] = Array(cert)

      override def getPeerPrincipal: Principal = new X500Principal("CN=MockCN, OU=JavaSoft, O=Sun Microsystems, C=US")

      override def getSessionContext: SSLSessionContext = ???

      override def getValueNames: Array[String] = ???

      override def isValid: Boolean = ???

      override def getProtocol: String = ???

      override def invalidate(): Unit = ???

      override def getApplicationBufferSize: Int = ???

      override def getValue(s: String): AnyRef = ???

      override def removeValue(s: String): Unit = ???

      override def getPeerCertificateChain: Array[X509Certificate] = ???

      override def getCreationTime: Long = ???

      override def putValue(s: String, o: scala.Any): Unit = ???
    }
  }

}
