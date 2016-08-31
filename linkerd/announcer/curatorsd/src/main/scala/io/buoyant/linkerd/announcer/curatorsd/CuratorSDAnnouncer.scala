package io.buoyant.linkerd.announcer.curatorsd

import java.net.InetSocketAddress

import com.twitter.finagle.{Announcement, Path}
import com.twitter.logging.Logger
import com.twitter.util.Future
import io.buoyant.linkerd.Announcer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery._
import org.apache.curator.x.discovery.details.{JsonInstanceSerializer, ServiceDiscoveryImpl}

/**
 * Announcer that uses the curator service discovery format.
 */
class CuratorSDAnnouncer(zkConnectStr: String, tenant: Option[String]) extends Announcer {

  override val scheme: String = "zk-curator"

  private val log = Logger.get("curator")

  private val curatorClient = CuratorFrameworkFactory.builder
    .connectString(zkConnectStr)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .build

  curatorClient.start

  // TODO payload
  private val serviceDiscovery: ServiceDiscovery[String] = new ServiceDiscoveryImpl(curatorClient, "", new JsonInstanceSerializer[String](classOf[String]), null, false)

  // TODO who closes?
  serviceDiscovery.start

  private def getServiceFullPath(serviceId: String, tenant: Option[String]): String =
    tenant.map(t => serviceId + "." + t).getOrElse(serviceId)

  override def announce(addr: InetSocketAddress, name: Path): Future[Announcement] = {
    val serviceId = name.take(1).show.stripPrefix("/") // TODO version could be the second element in the future
    log.info("Announcing %s %s %s %s", serviceId, tenant, addr, zkConnectStr)

    // TODO exception handling
    val builder: ServiceInstanceBuilder[String] = ServiceInstance.builder[String]
      .name(getServiceFullPath(serviceId, tenant))
      .uriSpec(new UriSpec("http://" + addr.getHostString + ":" + addr.getPort)) // TODO how to specify https?
      //          .payload(new ServiceInstanceInfo(description))
      .serviceType(ServiceType.DYNAMIC);

    val serviceInstance: ServiceInstance[String] = builder.build

    serviceDiscovery.registerService(serviceInstance)

    log.info("Successfully announced %s %s %s %s", serviceId, tenant, addr, serviceInstance.getId)

    // TODO verify who un-announces
    Future.value(new Announcement {
      def unannounce() = {
        log.info("Unannouncing %s %s %s", serviceId, tenant, addr)
        Future.value(serviceDiscovery.unregisterService(serviceInstance))
      }
    })
  }

}