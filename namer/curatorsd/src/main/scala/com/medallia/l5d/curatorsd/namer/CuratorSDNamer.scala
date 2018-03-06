package com.medallia.l5d.curatorsd.namer

import java.net.URL

import com.medallia.l5d.curatorsd.common.CuratorSDCommon
import com.medallia.servicediscovery.{ServiceDiscoveryListener, ServiceId, ServiceInstanceInfo}
import com.twitter.finagle._
import com.twitter.logging.Logger
import com.twitter.util._
import org.apache.curator.x.discovery.ServiceInstance

import scala.collection.JavaConverters._

/**
 * The curator namer takes Paths of the form
 *
 * {{{
 * /#/com.medallia.curatorsd/environment/tenant/service/protocol
 * }}}
 *
 * and returns a dynamic representation of the resolution of the path into a
 * tree of Names.
 */
class CuratorSDNamer(zkConnectStr: String, backwardsCompatibility: Option[String]) extends Namer with Closable with CloseAwaitably {

  private val log = Logger(getClass)

  private val serviceDiscoveryInfo = CuratorSDCommon.createServiceDiscovery(zkConnectStr, backwardsCompatibility)

  private def getAddress(instances: Iterable[ServiceInstance[ServiceInstanceInfo]], protocol: String): Addr = {
    val addrs = instances
                  .map(instance => new URL(instance.getUriSpec.build()))
                  .filter(url => url.getProtocol() == protocol)
                  .map(url => Address(url.getHost, url.getPort))
                  .filter()
                  .toStream
                  .distinct
    log.info("Binding to addresses %s protocol %s", addrs, protocol)
    val metadata = Addr.Metadata(("ssl", protocol == "https"))
    Addr.Bound(addrs.toSet, metadata)
  }

  override def lookup(path: Path): Activity[NameTree[Name]] = {
    log.info("Binding for path %s", path)

    path match {
      case Path.Utf8(environment, tenant, serviceName, protocol) =>
        val serviceId = new ServiceId(serviceName, tenant, CuratorSDCommon.fromOptionalPathField(environment).orNull)

        log.info(s"Looking up %s, protocol: %s", serviceId, protocol)

        val addrInit = getAddress(serviceDiscoveryInfo.serviceDiscovery.lookupAll(serviceId).asScala)
        val addrVar = Var.async(addrInit) { update =>

          val listener = new ServiceDiscoveryListener {

            override def serviceInstancesChanged(): Unit = {
              log.info("Cache changed for %s", serviceName)
              update() = getAddress(serviceDiscoveryInfo.serviceDiscovery.lookupAll(serviceId).asScala)
            }

          }

          serviceDiscoveryInfo.serviceDiscovery.addServiceListener(serviceId, listener)

          Closable.make { deadline =>
            serviceDiscoveryInfo.serviceDiscovery.removeServiceListener(serviceId, listener)
            Future.Unit
          }
        }
        Activity.value(NameTree.Leaf(Name.Bound(addrVar, path, path)))
      case _ =>
        Activity.exception(new IllegalArgumentException(s"Expected curator namer format: /environment/tenant/serviceName/protocol, got $path"))
    }
  }

  override def close(deadline: Time): Future[Unit] = closeAwaitably(Future {
    log.info("Closing curator namer %s", zkConnectStr)
    serviceDiscoveryInfo.close()
    log.info("Curator namer closed")
  })

}
