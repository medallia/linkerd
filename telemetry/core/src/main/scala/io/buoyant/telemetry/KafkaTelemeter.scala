package io.buoyant.telemetry

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.util.{Awaitable, Closable}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.ConcurrentHashMap
import com.twitter.finagle.zipkin.core.SamplingTracer
import com.twitter.finagle.zipkin.core.Sampler

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.KafkaProducer
import com.twitter.util.Duration
import com.twitter.util.Awaitable.CanAwait
import com.twitter.util.Time
import com.twitter.util.Future
import com.twitter.util.Try
import com.google.common.collect.EvictingQueue
import com.twitter.finagle.tracing.Tracer
import java.util.concurrent.Semaphore

import com.twitter.finagle.{Stack, tracing}
import com.twitter.finagle.zipkin.thrift.ScribeRawZipkinTracer

import com.twitter.conversions.time._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import com.twitter.finagle.zipkin.core.RawZipkinTracer
import com.twitter.finagle.zipkin.core.Span
import com.twitter.finagle.zipkin.core.DeadlineSpanMap
import com.twitter.finagle.Codec
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.IOException
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.thrift.TSerializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import com.twitter.finagle.zipkin.core.TracerCache

import java.util.Map
import java.util.HashMap

import org.apache.kafka.clients.producer.ProducerConfig
import com.twitter.logging.Logger

class KafkaTelemeterInitializer extends TelemeterInitializer {
  type Config = KafkaTelemeterConfig
  override def configId = "io.buoyant.telemetry.KafkaTelemeterConfig"
  def configClass = classOf[KafkaTelemeterConfig]
}

case class KafkaTelemeterConfig(brokerList: String, sampleRate: Float, numRetries: Int) extends TelemeterConfig {
  private val log = Logger.get(getClass)
  def mk(params: Stack.Params): KafkaTelemeter = {
    log.info("Broker list is %s", brokerList)
    log.info("Number of retries per request is %s", numRetries)
    log.info("Sample rate is" + sampleRate)
    new KafkaTelemeter("zipkin", numRetries, sampleRate, brokerList)
  }
}

/**
 * Going with hybrid approach. Let kafka handle retries. Batching will be handled outside as it is being done today.
 * We will leave linger.ms unchanged for this reason at 1 ms.
 * The reason for this is that the API has been constructed in such a way that a given trace is annotated across multiple
 * different calls rather than gathering up all its annotations and then sending it on its way.
 */
case class KafkaTelemeter(topic: String, numRetries: Int, sampleRate: Float, brokerList: String) extends Telemeter {
  private val log = Logger.get(getClass)
  log.info("Initializing kafka telemeter")
  private val METADATA_FETCH_TIMEOUT_MS_CONFIG: Int = 3000
  /* We do not want to wait too long on the broker for the event propagated to the replicas */
  private val TIMEOUT_MS_CONFIG: Int = 300
  private val RETRY_BACKOFF_MS_CONFIG: Int = 10000
  private val RECONNECT_BACKOFF_MS_CONFIG: Int = 10000
  //Set this to 1 millisecond so that there is enough time for a batch of spans to be pushed to kafka local buffers.
  private val LINGER_MS_CONFIG = 1
  case class TracerTuple(kafkaRawZipkinTracer: KafkaRawZipkinTracer, kafkaTracer: KafkaTracer)
  val createTracer = new java.util.function.Function[String, TracerTuple] {
    override def apply(t: String): TracerTuple = {
      val underlyingTracer = new KafkaRawZipkinTracer(brokerList, numRetries, topic)
      new TracerTuple(underlyingTracer, new KafkaTracer(underlyingTracer, sampleRate))
    }
  }

  def tracer: Tracer = {
    KafkaRawZipkinTracer.tracerCache.computeIfAbsent(
      brokerList + topic,
      createTracer
    ).kafkaTracer
  }

  private def cleanup(): Unit = {
    log.info("Starting cleanup of the kafka telemeter")
    KafkaRawZipkinTracer.tracerCache.get(brokerList + topic).kafkaRawZipkinTracer.cleanup()
    log.info("Done cleaning up the kafka telemeter")
  }

  object KafkaRawZipkinTracer {
    val tracerCache = new ConcurrentHashMap[String, TracerTuple]
  }

  class KafkaTracer(tracer: Tracer, sampleRate: Float = Sampler.DefaultSampleRate)
    extends SamplingTracer(tracer, sampleRate)

  //Only support thrift for now.
  class KafkaRawZipkinTracer(
    brokerList: String,
    numRetries: Int = 3, //this is the kafka default.
    topic: String = "zipkin",
    timer: Timer = DefaultTimer.twitter,
    statsReceiver: StatsReceiver = DefaultStatsReceiver
  ) extends RawZipkinTracer(statsReceiver, timer) {
    log.info("Initializing kafka zipkin tracer")
    private val producer: KafkaProducer[Array[Byte], Array[Byte]] = {
      val configs: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]
      configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
      configs.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, METADATA_FETCH_TIMEOUT_MS_CONFIG.asInstanceOf[AnyRef])
      configs.put(ProducerConfig.TIMEOUT_CONFIG, TIMEOUT_MS_CONFIG.asInstanceOf[AnyRef])
      configs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_CONFIG.asInstanceOf[AnyRef])
      configs.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, RECONNECT_BACKOFF_MS_CONFIG.asInstanceOf[AnyRef])
      configs.put(ProducerConfig.RETRIES_CONFIG, numRetries.asInstanceOf[AnyRef])
      configs.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG.asInstanceOf[AnyRef])
      new KafkaProducer[Array[Byte], Array[Byte]](configs, new ByteArraySerializer(), new ByteArraySerializer())
    }

    //retries configured from the input.
    private[this] val scopedReceiver = statsReceiver.scope("log_span")
    private[this] val okCounter = scopedReceiver.counter("ok")
    private[this] val errorReceiver = scopedReceiver.scope("error")

    private[this] def serializeSpan(span: Span): Array[Byte] = {
      log.debug("Serializing span data to bytes")
      val tSerializer = new TSerializer()
      tSerializer.serialize(span.toThrift)
    }

    /**
     * Log the span data via kafka.
     */
    def sendSpans(spans: Seq[Span]): Future[Unit] = {
      //delegate batching to downstream for simplicity reasons rather than doing buffer pools here.
      //The one complication it adds is it is hard to do retries. Breaking it into the model if any one stat emission
      //fails the entire batch is considered to be failed. The batch will only fail if the retries for all the different
      //stats emitted as part of the batch are exhausted independently of each other
      // without being able to send it out successfully.
      // Will try to do early detection. Currently this will enable both
      //kafka level and higher level ret/**/ries if the framework supports it.
      var ex: Throwable = null
      Future({
        log.debug("Sending kafka %i trace events", spans.size)
        val statEmissionFailures: AtomicBoolean = new AtomicBoolean(false)
        val semaphore: Semaphore = new Semaphore((spans.size - 1) * -1)
        spans.foreach {
          span =>
            val bytes: Array[Byte] = serializeSpan(span)
            val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, bytes)

            producer.send(record, new Callback() {
              override def onCompletion(metadata: RecordMetadata, exception: java.lang.Exception) {
                if (exception != null) {
                  log.info("Error while sending message %s", exception.getMessage)
                  errorReceiver.counter("Failed retries").incr()
                  if (!statEmissionFailures.getAndSet(true)) {
                    ex = exception
                    semaphore.release(spans.size) //un-block final wait.
                  }
                } else {
                  log.debug("Kafka trace events set successfully.")
                  okCounter.incr()
                  semaphore.release(1)
                }
              }
            })
        }
        //wait for all the events to be sent successfully or a failure to occur.
        //we cannot throw exceptions from the callback since it is not our thread.
        log.debug("Waiting for %i kafka trace events to be sent out or errors to occur", spans.size)
        semaphore.acquire(1)
        log.debug("Done waiting for %i kafka trace events", spans.size)
        if (statEmissionFailures.get())
          throw ex
      })
    }

    def cleanup() = {
      //Take all the pending spans and flush them.
      Await.result(this.flush())
      producer.close()
    }
  }

  override def stats: StatsReceiver = NullStatsReceiver

  override def run(): Closable with Awaitable[Unit] = new Closable with CloseAwaitably {
    def close(deadline: Time): Future[Unit] = {
      closeAwaitably(Future({
        cleanup()
      }))
    }
  }
}

