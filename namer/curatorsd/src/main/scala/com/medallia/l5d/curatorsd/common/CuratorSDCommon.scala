package com.medallia.l5d.curatorsd.common

import java.io.ByteArrayOutputStream
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.google.common.base.Joiner
import com.google.common.cache.CacheBuilder
import com.medallia.servicediscovery.ServiceInstanceInfo
import com.twitter.logging.Logger
import com.twitter.util.{Closable, Duration, Future, Time}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.discovery.details.InstanceSerializer
import org.apache.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceInstance}
import org.codehaus.jackson.map.ObjectMapper

object CuratorSDCommon {

  private val curatorClientCache = CacheBuilder.newBuilder().build[String, ServiceDiscoveryInfo]

  /**
   * @param zkConnectStr ZK connection string
   * @return Service Discovery set of objects which needs to be closed
   */
  def createServiceDiscovery(zkConnectStr: String): ServiceDiscoveryInfo = {
    val serviceDiscoveryInfo = curatorClientCache.get(zkConnectStr, new Callable[ServiceDiscoveryInfo] {
      def call = ServiceDiscoveryInfo(zkConnectStr)
    })
    serviceDiscoveryInfo.addReference()
    serviceDiscoveryInfo
  }

  def getServiceFullPath(serviceId: String, tenant: Option[String]): String = {
    tenant.map(t => Joiner.on(".").join(serviceId, t)).getOrElse(serviceId)
  }

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

case class ServiceDiscoveryInfo(zkConnectStr: String) extends RefCounted {

  private val log = Logger(getClass)

  private val DefaultBaseSleepTime = Duration.fromSeconds(1)
  private val DefaultMaxRetries = 3

  private val curatorClient = CuratorFrameworkFactory.builder
    .connectString(zkConnectStr)
    .retryPolicy(new ExponentialBackoffRetry(DefaultBaseSleepTime.inMillis.toInt, DefaultMaxRetries))
    .build

  val serviceDiscovery = ServiceDiscoveryBuilder.builder(classOf[ServiceInstanceInfo])
    .client(curatorClient)
    .serializer(new ScalaJsonInstanceSerializer[ServiceInstanceInfo](classOf[ServiceInstanceInfo]))
    .basePath("")
    .build()

  curatorClient.start()
  curatorClient.blockUntilConnected(10, SECONDS)

  serviceDiscovery.start()

  protected override def performClose() = {
    log.info("Physically closing curator service discovery %s", zkConnectStr)
    serviceDiscovery.close()
    curatorClient.close()
    log.info("Curator service discovery physically closed")
  }
}

trait RefCounted extends Closable {

  private val refCount = new AtomicInteger()
  private val isClosed = new AtomicBoolean()

  override def close(deadline: Time): Future[Unit] = Future {
    this.synchronized {
      if (refCount.decrementAndGet() <= 0 && !isClosed.get()) {
        performClose()
        isClosed.set(true)
      }
    }
  }

  private[common] def addReference(): Unit = {
    this.synchronized {
      if (isClosed.get())
        throw new IllegalStateException(s"Already closed $this")
      val _ = refCount.incrementAndGet()
    }
  }

  protected def performClose(): Unit

}
