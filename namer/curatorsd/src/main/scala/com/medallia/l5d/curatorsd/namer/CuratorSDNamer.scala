package com.medallia.l5d.curatorsd.namer

import java.net.URL

import com.medallia.l5d.curatorsd.common.CuratorSDCommon
import com.medallia.servicediscovery.ServiceInstanceInfo
import com.twitter.finagle._
import com.twitter.logging.Logger
import com.twitter.util._
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.discovery._
import org.apache.curator.x.discovery.details.ServiceCacheListener

import scala.collection.JavaConverters._

/**
 * The curator namer takes Paths of the form
 *
 * {{{
 * /#/com.medallia.curatorsd/tenant/service
 * }}}
 *
 * and returns a dynamic representation of the resolution of the path into a
 * tree of Names.
 */
class CuratorSDNamer(zkConnectStr: String) extends Namer with Closable with CloseAwaitably {

  private val log = Logger(getClass)

  private val serviceDiscoveryInfo = CuratorSDCommon.createServiceDiscovery(zkConnectStr)

  private def instanceToAddress(instance: ServiceInstance[ServiceInstanceInfo]): Address = {
    val address = instance.getAddress
    val port = instance.getPort
    if (address != null && port != null) {
      Address(address, port)
    } else {
      val url = new URL(instance.getUriSpec.build())
      Address(url.getHost, url.getPort) // TODO (future) support https and path
    }
  }

  private def getAddress(caches: List[ServiceCache[ServiceInstanceInfo]]): Addr = {
    val addrs = caches.toStream.filter(!_.getInstances.asScala.isEmpty).head.getInstances.asScala.map(instanceToAddress(_))
    log.info(s"Binding to addresses $addrs")
    Addr.Bound(addrs.toSet, Addr.Metadata.empty)
  }

  private def newServiceCache(serviceFullName: String): ServiceCache[ServiceInstanceInfo] = {
    val serviceCache = serviceDiscoveryInfo.serviceDiscovery.serviceCacheBuilder.name(serviceFullName).build
    serviceCache.start()
    serviceCache
  }

  override def lookup(path: Path): Activity[NameTree[Name]] = {
    log.info(s"Binding for path %s", path)

    path match {
      case Path.Utf8(environment, tenant, serviceName) =>
        log.info(s"environment: $environment - tenant: $tenant - serviceName: $serviceName")

        val serviceCacheSharedV1 = newServiceCache(CuratorSDCommon.getServiceFullPathV1(serviceName, None))
        val serviceCacheTenantV1 = newServiceCache(CuratorSDCommon.getServiceFullPathV1(serviceName, Some(tenant)))

        val serviceCacheSharedV2 = newServiceCache(CuratorSDCommon.getServiceFullPathV2(serviceName, None, None))
        val serviceCacheTenantV2 = newServiceCache(CuratorSDCommon.getServiceFullPathV2(serviceName, Some(tenant), None))
        val serviceCacheEnvironmentV2 = newServiceCache(CuratorSDCommon.getServiceFullPathV2(serviceName, None, Some(environment)))
        val serviceCacheEnvironmentTenantV2 = newServiceCache(CuratorSDCommon.getServiceFullPathV2(serviceName, Some(tenant), Some(environment)))

        val caches = List(
          serviceCacheEnvironmentTenantV2,
          serviceCacheEnvironmentV2,
          serviceCacheTenantV2,
          serviceCacheTenantV1,
          serviceCacheSharedV2,
          serviceCacheSharedV1
        )

        val addrInit = getAddress(caches)
        val addrVar = Var.async(addrInit) { update =>

          val listener = new ServiceCacheListener {

            override def cacheChanged(): Unit = {
              log.info("Cache changed for %s", serviceName)
              update() = getAddress(caches)
            }

            override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
              log.warning(s"Connection state changed $newState for service $serviceName")
            }

          }

          caches.foreach(_.addListener(listener))

          Closable.make { deadline =>
            caches.foreach(_.removeListener(listener))
            Future.Unit
          }
        }
        Activity.value(NameTree.Leaf(Name.Bound(addrVar, path, path)))
      case _ =>
        Activity.exception(new IllegalArgumentException(s"Expected curator namer format: /environment/tenant/serviceName, got $path"))
    }
  }

  override def close(deadline: Time): Future[Unit] = closeAwaitably(Future {
    log.info("Closing curator namer %s", zkConnectStr)
    serviceDiscoveryInfo.close()
    log.info("Curator namer closed")
  })

}
