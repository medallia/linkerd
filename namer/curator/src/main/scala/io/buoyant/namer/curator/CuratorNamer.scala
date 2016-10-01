package io.buoyant.namer.curator

import java.io.ByteArrayOutputStream
import java.net.URL
import java.util.concurrent.Callable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.cache.{Cache, CacheBuilder}
import com.twitter.finagle._
import com.twitter.logging.Logger
import com.twitter.util.{Activity, Var}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery.{ServiceDiscovery, ServiceInstance, ServiceProvider}
import org.apache.curator.x.discovery.details.{InstanceSerializer, ServiceDiscoveryImpl}
import org.apache.log4j.{BasicConfigurator, Level}

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

  private val serviceDiscovery: ServiceDiscovery[ServiceInstanceInfo] = new ServiceDiscoveryImpl(curatorClient, "",
    new ScalaJsonInstanceSerializer[ServiceInstanceInfo](classOf[ServiceInstanceInfo]), null, false)

  private val serviceProviders = CacheBuilder.newBuilder.build.asInstanceOf[Cache[String, ServiceProvider[ServiceInstanceInfo]]]

  // Quick and dirty configuration of log4j to log to console, INFO level
  BasicConfigurator.configure()
  org.apache.log4j.Logger.getRootLogger.setLevel(Level.INFO)

  curatorClient.start()
  serviceDiscovery.start()

  /** Resolve a resolver string to a Var[Addr]. */
  protected[this] def resolve(spec: String): Var[Addr] = Resolver.eval(spec) match {
    case Name.Bound(addr) => addr
    case _ => Var.value(Addr.Neg)
  }

  private def getServiceProvider(path: String): ServiceProvider[ServiceInstanceInfo] =
    serviceProviders.get(path, new Callable[ServiceProvider[ServiceInstanceInfo]] {
      def call: ServiceProvider[ServiceInstanceInfo] = {
        log.info(s"Getting service provider for $path")
        val serviceProvider = serviceDiscovery
          .serviceProviderBuilder()
          .serviceName(path)
          //        .providerStrategy(new RequestIdStrategy())
          .build()
        serviceProvider.start()
        serviceProvider
      }
    })

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

  protected[this] def resolveInstances(tenant: String, serviceName: String): Var[Addr] = {
    val sharedInstances = collectionAsScalaIterableConverter(getServiceProvider(serviceName).getAllInstances).asScala
    val tenantSpecificInstances = collectionAsScalaIterableConverter(getServiceProvider(
      CuratorCommon.getServiceFullPath(serviceName, Some(tenant))
    ).getAllInstances).asScala

    val addresses = (sharedInstances ++ tenantSpecificInstances).map(instanceToAddress).toSet

    Var(Addr.Bound(addresses, Addr.Metadata.empty))
  }

  override def lookup(path: Path): Activity[NameTree[Name]] = {
    log.info(s"Binding for path $path")
    path match {
      case Path.Utf8(tenant, serviceName) =>
        log.info(s"tenant $tenant serviceName $serviceName")

        val address = resolveInstances(tenant, serviceName)
        val activity = Activity(address.map(Activity.Ok(_)))

        activity.flatMap {
          case Addr.Neg =>
            Activity.value(NameTree.Neg)
          case Addr.Bound(_, _) =>
            Activity.value(NameTree.Leaf(Name.Bound(address, path)))
          case Addr.Pending =>
            Activity.pending
          case Addr.Failed(exc) =>
            Activity.exception(exc)
        }
      case _ =>
        Activity.exception(new IllegalArgumentException(s"Expected curator namer format: /tenant/serviceName, got $path"))
    }
  }

}

/** ServiceDiscovery payload, just a generic description for now */
case class ServiceInstanceInfo(description: String) {

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

  objectMapper.registerModule(DefaultScalaModule)

  override def deserialize(bytes: Array[Byte]): ServiceInstance[T] = {
    val rawServiceInstance: ServiceInstance[_] = objectMapper.readValue(bytes, serviceInstanceClass)
    targetClass.cast(rawServiceInstance.getPayload) // just to verify that it's the correct type
    rawServiceInstance.asInstanceOf[ServiceInstance[T]]
  }

  override def serialize(instance: ServiceInstance[T]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    objectMapper.writeValue(out, instance)
    out.toByteArray
  }
}