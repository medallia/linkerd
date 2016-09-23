package io.buoyant.linkerd.announcer.curatorsd

import java.net.InetSocketAddress

import com.twitter.finagle.{Announcement, Path}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Time}
import io.buoyant.linkerd.FutureAnnouncer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery._
import org.apache.curator.x.discovery.details.{JsonInstanceSerializer, ServiceDiscoveryImpl}
import org.apache.log4j.{BasicConfigurator, Level}

/**
 * Announcer that uses the curator service discovery format.
 * <p>
 * Format used for the service name Path: /#/io.l5d.curatorsd/{full service name}[#{tenant}]
 * tenant is optional. If it's empty or the last part is missing, it will be registered as a multi-tenant service.
 */
class CuratorSDAnnouncer(zkConnectStr: String) extends FutureAnnouncer {

  override val scheme: String = "zk-curator"

  private val log = Logger("curator")

  private val DefaultBaseSleepTimeMs = 1000;
  private val DefaultMaxRetries = 3

  private val curatorClient = CuratorFrameworkFactory.builder
    .connectString(zkConnectStr)
    .retryPolicy(new ExponentialBackoffRetry(DefaultBaseSleepTimeMs, DefaultMaxRetries))
    .build

  private val serviceDiscovery: ServiceDiscovery[String] = new ServiceDiscoveryImpl(curatorClient, "", new JsonInstanceSerializer[String](classOf[String]), null, false)

  // Quick and dirty configuration of log4j to log to console, INFO level
  BasicConfigurator.configure
  org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO)

  curatorClient.start
  serviceDiscovery.start

  override def announceAsync(addr: InetSocketAddress, name: Path): Future[Announcement] = {
    val serviceFullPath = name.take(2) match {
      case id@Path.Utf8(serviceDef) =>
        // TODO (future) full semantic version could be a third element in the future
        val parts: Array[String] = serviceDef.split("#")
        if (parts.size != 1 && parts.size != 2)
          throw new IllegalArgumentException("Incorrect number of parts in announcer name (it should be serviceId[#tenant])" + serviceDef)
        val serviceId = parts(0)
        val tenant = if (parts.size > 1) Option(parts(1)).filter(_.trim.nonEmpty) else None
        log.info("Announcing %s, tenant: %s address: %s, ZK cluster: %s", serviceId, tenant.getOrElse("(multi-tenant)"), addr, zkConnectStr)
        getServiceFullPath(serviceId, tenant)

      case _ => throw new IllegalArgumentException("Tenant information is missing: " + name)
    }

    // TODO (future) how to specify https? Handle this when we work on the Namer.
    // TODO (future) payload. Not needed for now (it only containes a description)
    val builder: ServiceInstanceBuilder[String] = ServiceInstance.builder[String]
      .name(serviceFullPath)
      .uriSpec(new UriSpec("http://" + addr.getHostString + ":" + addr.getPort))
      .port(addr.getPort)
      .address(addr.getHostString)
      .serviceType(ServiceType.DYNAMIC)

    val serviceInstance = builder.build

    serviceDiscovery.registerService(serviceInstance)

    log.info("Successfully announced %s %s %s", serviceFullPath, addr, serviceInstance.getId)

    Future.value(new Announcement {
      def unannounce() = {
        log.info("Unannouncing %s %s", serviceFullPath, addr)
        Future {
          serviceDiscovery.unregisterService(serviceInstance)
        }
      }
    })
  }

  private def getServiceFullPath(serviceId: String, tenant: Option[String]): String =
    tenant.map(t => serviceId + "." + t).getOrElse(serviceId)

  override def close(deadline: Time) =
    Future {
      log.info("Closing curator announcer %s ", zkConnectStr)
      serviceDiscovery.close
      curatorClient.close
      log.info("Curator announcer closed")
    }

}
