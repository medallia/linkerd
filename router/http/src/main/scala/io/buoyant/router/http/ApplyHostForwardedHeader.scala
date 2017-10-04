package io.buoyant.router.http

import com.twitter.finagle._
import com.twitter.finagle.http.{Fields, Request, Response}
import com.twitter.util.Future
import java.net.{Inet4Address, Inet6Address, InetSocketAddress, SocketAddress, URI, URISyntaxException}
import scala.collection.mutable
import scala.util.Random
import com.twitter.logging.Logger

/**
 * Applies the Host value in the [Forwarded](https://tools.ietf.org/html/rfc7239) header to the Request's Host header.
 *
 */
class ApplyHostForwardedHeader() extends SimpleFilter[Request, Response] {
  private val log = Logger(getClass)

  val forwardedHeader = "Forwarded"
  def apply(req: Request, svc: Service[Request, Response]): Future[Response] = {
    log.info(s"ApplyHostForwardedHeader for %s", req)

    log.info(s"Host is for %s", req.host)
    replaceHostWithForwardedHostIfExists(req)
    log.info(s"Host is for %s", req.host)

    svc(req)
  }

  private def getForwardedHost(headerMap: Map[String, String]): Option[String] = {
    headerMap.get(forwardedHeader)
      .flatMap { f =>
        f.split(";").toStream
          .map(_.trim)
          .find(x => x.startsWith("host="))
      }
      .map(fHost => fHost.replace("host=", ""))
  }

  private def replaceHostWithForwardedHostIfExists(req: Request): Any = {
    val forwardedHostOp = getForwardedHost(req.headerMap.toMap)
    forwardedHostOp.foreach(forwardedHost => req.headerMap.set(Fields.Host, forwardedHost))
  }
}

object ApplyHostForwardedHeader {

  class HostHeaderReplacementProxy(
    underlying: ServiceFactory[Request, Response]
  ) extends ServiceFactoryProxy(underlying) {

    override def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
      print("AAAAAAAAAAA")
      val filter = new ApplyHostForwardedHeader()
      self.apply(conn).map(filter.andThen(_))
    }
  }
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = Stack.Role("ApplyHostForwardedHeader")
      val description = "Applies the Host value in the [Forwarded](https://tools.ietf.org/html/rfc7239) header to the Request's Host header"
      def make(
        next: ServiceFactory[Request, Response]
      ) = new HostHeaderReplacementProxy(next)
    }
}

