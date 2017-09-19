package io.buoyant.router.http

import com.twitter.finagle.{Dtab, Path}
import com.twitter.finagle.buoyant.Dst
import com.twitter.finagle.http.{Fields, Request, Version}
import com.twitter.util.Future
import io.buoyant.router.RoutingFactory.{IdentifiedRequest, Identifier, RequestIdentification, UnidentifiedRequest}

object MethodEnvironmentTenantHostIdentifier {

  val TenantHeader = "X-Medallia-Rpc-Tenant"
  val EnvironmentHeader = "X-Medallia-Rpc-Environment"
  val ForwardedHeader = "Forwarded"

  def mk(
    prefix: Path,
    baseDtab: () => Dtab = () => Dtab.base
  ): Identifier[Request] = MethodEnvironmentTenantHostIdentifier(prefix, baseDtab, TenantHeader, EnvironmentHeader, ForwardedHeader)
}

case class MethodEnvironmentTenantHostIdentifier(
  prefix: Path,
  baseDtab: () => Dtab = () => Dtab.base,
  tenantHeader: String,
  environmentHeader: String,
  forwardedHeader: String
) extends Identifier[Request] {

  private[this] def mkPath(path: Path): Dst.Path =
    Dst.Path(prefix ++ path, baseDtab(), Dtab.local)

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

  def apply(req: Request): Future[RequestIdentification[Request]] = req.version match {
    case Version.Http11 =>
      req.host match {
        case Some(host) if host.nonEmpty =>
          req.headerMap.get(tenantHeader) match {
            case Some(tenant) =>
              val environment: String = req.headerMap.get(environmentHeader).getOrElse("_") //this is an optional header
              val dst = mkPath(Path.Utf8("1.1", req.method.toString, environment, tenant, host))
              replaceHostWithForwardedHostIfExists(req)
              Future.value(new IdentifiedRequest(dst, req))
            case None =>
              Future.value(new UnidentifiedRequest(s"$tenantHeader header is absent"))
          }
        case _ =>
          Future.value(
            new UnidentifiedRequest(s"${Version.Http11} request missing Host header")
          )
      }
    case _ =>
      Future.value(
        new UnidentifiedRequest(s"${req.version} not supported by this identifier")
      )
  }
}
