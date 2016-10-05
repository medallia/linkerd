package io.buoyant.namer.curator

import java.io.ByteArrayOutputStream
import java.net.URL
import java.util.concurrent.TimeUnit._

import com.medallia.servicediscovery.ServiceInstanceInfo
import com.twitter.finagle._
import com.twitter.logging.Logger
import com.twitter.util.{Activity, Closable, Future, Var}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery._
import org.apache.curator.x.discovery.details.{InstanceSerializer, ServiceCacheListener, ServiceDiscoveryImpl}
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._

/**
 * The curator namer takes Paths of the form
 *
 * {{{
 * /#/io.l5d.curator/tenant/service
 * }}}
 *
 * and returns a dynamic representation of the resolution of the path into a
 * tree of Names.
 */
class CuratorNamer(zkConnectStr: String) extends Namer {

  private val log = Logger("curatorNamer")

  private val DefaultBaseSleepTimeMs = 1000
  private val DefaultMaxRetries = 3

  private val curatorClient = CuratorFrameworkFactory.builder
    .connectString(zkConnectStr)
    .retryPolicy(new ExponentialBackoffRetry(DefaultBaseSleepTimeMs, DefaultMaxRetries))
    .build

  private val serviceDiscovery = ServiceDiscoveryBuilder.builder(classOf[ServiceInstanceInfo])
    .client(curatorClient)
    .serializer(new ScalaJsonInstanceSerializer[ServiceInstanceInfo](classOf[ServiceInstanceInfo]))
    .basePath("")
    .build()

  curatorClient.start()
  curatorClient.blockUntilConnected(10, SECONDS)

  serviceDiscovery.start()
  
  private def instanceToAddress(instance: ServiceInstance[ServiceInstanceInfo]): Address = {
    val address = instance.getAddress
    val port = instance.getPort
    if (address != null && port != null) {
      Address(address, port)
    } else {
      val url: URL = new URL(instance.getUriSpec.build())

      // TODO (future) support https and path
      Address(url.getHost, url.getPort)
    }
  }

  private def getAddress(sharedCache: ServiceCache[ServiceInstanceInfo], tenantCache: ServiceCache[ServiceInstanceInfo]): Addr = {
    val addrs = (sharedCache.getInstances.asScala ++ tenantCache.getInstances.asScala).map(instanceToAddress)
    log.info(s"Binding to addresses $addrs")
    Addr.Bound(addrs.toSet, Addr.Metadata.empty)
  }

  private def newServiceCache(serviceFullName: String): ServiceCache[ServiceInstanceInfo] = {
    val serviceCache = serviceDiscovery.serviceCacheBuilder.name(serviceFullName).build
    serviceCache.start()
    serviceCache
  }

  override def lookup(path: Path): Activity[NameTree[Name]] = {
    log.info(s"Binding for path $path")

    path match {
      case Path.Utf8(tenant, serviceName) =>
        log.info(s"tenant $tenant serviceName $serviceName")

        val serviceCacheShared = newServiceCache(serviceName)
        val serviceCacheTenant = newServiceCache(CuratorCommon.getServiceFullPath(serviceName, Some(tenant)))

        val addrInit = getAddress(serviceCacheShared, serviceCacheTenant)
        val addrVar = Var.async(addrInit) { update =>

          val listener = new ServiceCacheListener {

            override def cacheChanged(): Unit = {
              log.info(s"Cache changed for $serviceName")
              update() = getAddress(serviceCacheShared, serviceCacheTenant)
            }

            override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
              log.warning(s"Connection state changed $newState for service $serviceName")
            }

          }

          serviceCacheShared.addListener(listener)
          serviceCacheTenant.addListener(listener)

          Closable.make { deadline =>
            serviceCacheShared.removeListener(listener)
            serviceCacheTenant.removeListener(listener)
            Future.Unit
          }
        }
        Activity.value(NameTree.Leaf(Name.Bound(addrVar, path, path)))
      case _ =>
        Activity.exception(new IllegalArgumentException(s"Expected curator namer format: /tenant/serviceName, got $path"))
    }
  }

}

object CuratorCommon {

  // TODO close objects
  // TODO share ZK (cache shared with announcer)

  def getServiceFullPath(serviceId: String, tenant: Option[String]): String =
    tenant.map(t => serviceId + "." + t).getOrElse(serviceId)

}

/** Scala version of JsonInstanceSerializer (supports scala properties) */
class ScalaJsonInstanceSerializer[T](val targetClass: Class[T]) extends InstanceSerializer[T] {

  private val objectMapper = new ObjectMapper()
  private val serviceInstanceClass = objectMapper.getTypeFactory.constructType(classOf[ServiceInstance[T]])

  override def deserialize(bytes: Array[Byte]): ServiceInstance[T] = {
    val rawServiceInstance: ServiceInstance[T] = objectMapper.readValue(bytes, serviceInstanceClass)
    targetClass.cast(rawServiceInstance.getPayload) // just to verify that it's the correct type
    rawServiceInstance.asInstanceOf[ServiceInstance[T]]
  }

  override def serialize(instance: ServiceInstance[T]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    objectMapper.writeValue(out, instance)
    out.toByteArray
  }
}