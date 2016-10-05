package io.buoyant.linkerd.announcer.curatorsd

import java.net.InetSocketAddress

import com.medallia.servicediscovery.ServiceInstanceInfo
import com.twitter.finagle.{Announcement, Path}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Time}
import io.buoyant.linkerd.FutureAnnouncer
import io.buoyant.namer.curator.{CuratorCommon, ScalaJsonInstanceSerializer}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery._
import org.apache.curator.x.discovery.details.ServiceDiscoveryImpl
import org.apache.log4j.{BasicConfigurator, Level}

/**
 * Announcer that uses the curator service discovery format.
 * <p>
 * Format used for the service name Path: /#/io.l5d.curatorsd/{full service name}[#{tenant}]
 * tenant is optional. If it's empty or the last part is missing, it will be registered as a multi-tenant service.
 */
class CuratorSDAnnouncer(zkConnectStr: String) extends FutureAnnouncer {

  override val scheme: String = "zk-curator"

  private val log = Logger("curatorAnnouncer")

  private val DefaultBaseSleepTimeMs = 1000
  private val DefaultMaxRetries = 3

  private val curatorClient = CuratorFrameworkFactory.builder
    .connectString(zkConnectStr)
    .retryPolicy(new ExponentialBackoffRetry(DefaultBaseSleepTimeMs, DefaultMaxRetries))
    .build

  private val serviceDiscovery: ServiceDiscovery[ServiceInstanceInfo] =
    new ServiceDiscoveryImpl(curatorClient, "", new ScalaJsonInstanceSerializer[ServiceInstanceInfo](classOf[ServiceInstanceInfo]), null, false)

  curatorClient.start()
  serviceDiscovery.start()

  private def announce(serviceId: String, tenant: Option[String], address: InetSocketAddress): Future[Announcement] = {
    val tenantStr = tenant.getOrElse("(multi-tenant)")
    log.info("Announcing %s, tenant: %s address: %s, ZK cluster: %s", serviceId, tenantStr, address, zkConnectStr)

    val serviceFullPath = CuratorCommon.getServiceFullPath(serviceId, tenant)

    // TODO (future) how to specify https? Handle this when we work on the Namer.
    val builder = ServiceInstance.builder[ServiceInstanceInfo]
      .name(serviceFullPath)
      .uriSpec(new UriSpec("http://" + address.getHostString + ":" + address.getPort))
      .port(address.getPort)
      .address(address.getHostString)
      .payload(new ServiceInstanceInfo(s"serviceId: $serviceId, tenant: $tenantStr"))
      .serviceType(ServiceType.DYNAMIC)

    val serviceInstance = builder.build

    serviceDiscovery.registerService(serviceInstance)

    log.info("Successfully announced %s %s %s", serviceFullPath, address, serviceInstance.getId)

    Future.value(new Announcement {
      def unannounce() = {
        log.info("Unannouncing %s %s", serviceFullPath, address)
        Future {
          serviceDiscovery.unregisterService(serviceInstance)
        }
      }
    })
  }

  override def announceAsync(addr: InetSocketAddress, name: Path): Future[Announcement] = {
    name.take(2) match {
      case id@Path.Utf8(serviceDef) =>
        // TODO (future) full semantic version could be a third element in the future
        val parts = serviceDef.split("#")
        if (parts.length != 1 && parts.length != 2)
          throw new IllegalArgumentException("Incorrect number of parts in announcer name (it should be serviceId[#tenant])" + serviceDef)
        val serviceId = parts(0)
        val tenant = if (parts.length > 1) Option(parts(1)).filter(_.trim.nonEmpty) else None
        announce(serviceId, tenant, addr)

      case _ => throw new IllegalArgumentException(s"Tenant information is missing in path: $name")
    }

  }

  override def close(deadline: Time) =
    Future {
      log.info("Closing curator announcer %s ", zkConnectStr)
      serviceDiscovery.close()
      curatorClient.close()
      log.info("Curator announcer closed")
    }

}
