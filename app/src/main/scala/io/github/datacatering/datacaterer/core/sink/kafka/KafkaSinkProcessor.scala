package io.github.datacatering.datacaterer.core.sink.kafka

import io.github.datacatering.datacaterer.api.model.Constants.KAFKA_TOPIC
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.model.RealTimeSinkResult
import io.github.datacatering.datacaterer.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import java.util.Properties
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Kafka sink processor for rate-limited streaming to Kafka topics.
 *
 * This processor is used when Kafka is configured with rowsPerSecond for rate limiting.
 * It sends messages one-by-one through the Pekko streaming writer with throttling.
 *
 * Expected DataFrame schema:
 * - key (optional): String - Kafka message key
 * - value: String - Kafka message value/payload
 * - partition (optional): Integer - Target partition
 * - headers (optional): Map[String, String] - Message headers
 */
class KafkaSinkProcessor extends RealTimeSinkProcessor[KafkaProducer[String, String]] with Serializable {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _
  private var producer: KafkaProducer[String, String] = _

  // Field names in the DataFrame
  private val KEY_FIELD = "key"
  private val VALUE_FIELD = "value"
  private val PARTITION_FIELD = "partition"

  override val expectedSchema: Map[String, String] = Map(
    VALUE_FIELD -> "string"
  )

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    this.connectionConfig = connectionConfig
    this.step = step
    this.producer = createProducer(connectionConfig)
    this
  }

  override def createConnection(connectionConfig: Map[String, String], step: Step): KafkaProducer[String, String] = {
    createProducer(connectionConfig)
  }

  private def createProducer(connectionConfig: Map[String, String]): KafkaProducer[String, String] = {
    val props = new Properties()

    // Extract bootstrap servers from connection config
    val bootstrapServers = connectionConfig.getOrElse("kafka.bootstrap.servers",
      connectionConfig.getOrElse("bootstrapServers", "localhost:9092"))
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    // Serializers
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Performance settings for rate-limited streaming
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1") // Send immediately for rate control
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0") // No batching delay

    // Pass through any additional Kafka producer configs
    connectionConfig.foreach { case (key, value) =>
      if (key.startsWith("kafka.") && !key.equals("kafka.bootstrap.servers")) {
        val kafkaKey = key.stripPrefix("kafka.")
        props.put(kafkaKey, value)
      }
    }

    LOGGER.debug(s"Creating Kafka producer with bootstrap servers: $bootstrapServers")
    new KafkaProducer[String, String](props)
  }

  override def close: Unit = {
    if (producer != null) {
      LOGGER.debug("Flushing and closing Kafka producer")
      Try(producer.flush())
      Try(producer.close())
    }
  }

  override def pushRowToSink(row: Row): RealTimeSinkResult = {
    val topic = getTopic
    val record = createProducerRecord(row, topic)

    Try {
      val metadata = producer.send(record).get()
      LOGGER.debug(s"Sent message to Kafka: topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()}")
      metadata
    } match {
      case Success(metadata) =>
        RealTimeSinkResult(s"""{"topic":"${metadata.topic()}","partition":${metadata.partition()},"offset":${metadata.offset()}}""")
      case Failure(ex) =>
        LOGGER.error(s"Failed to send message to Kafka: ${ex.getMessage}", ex)
        throw ex
    }
  }

  /**
   * Sends a message to Kafka asynchronously.
   * Returns a Future that completes when the message is acknowledged.
   */
  def pushRowToSinkAsync(row: Row): Future[String] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val topic = getTopic
    val record = createProducerRecord(row, topic)
    val promise = Promise[String]()

    producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
      if (exception != null) {
        LOGGER.error(s"Failed to send message to Kafka: ${exception.getMessage}", exception)
        promise.failure(exception)
      } else {
        LOGGER.debug(s"Sent message to Kafka: topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()}")
        promise.success(s"""{"topic":"${metadata.topic()}","partition":${metadata.partition()},"offset":${metadata.offset()}}""")
      }
    })

    promise.future
  }

  private def getTopic: String = {
    step.options.getOrElse(KAFKA_TOPIC,
      step.options.getOrElse("topic",
        connectionConfig.getOrElse(KAFKA_TOPIC,
          connectionConfig.getOrElse("topic",
            throw new IllegalArgumentException("Kafka topic not specified. Set 'topic' in step options or connection config.")))))
  }

  private def createProducerRecord(row: Row, topic: String): ProducerRecord[String, String] = {
    val schema = row.schema

    // Get value (required)
    val value = if (schema.fieldNames.contains(VALUE_FIELD)) {
      Option(row.getAs[String](VALUE_FIELD)).getOrElse("")
    } else {
      // If no 'value' field, serialize the entire row as JSON
      row.json
    }

    // Get key (optional)
    val key = if (schema.fieldNames.contains(KEY_FIELD)) {
      Option(row.getAs[String](KEY_FIELD)).orNull
    } else {
      null
    }

    // Get partition (optional)
    val partition = if (schema.fieldNames.contains(PARTITION_FIELD)) {
      Option(row.getAs[Any](PARTITION_FIELD)).map {
        case i: Int => Integer.valueOf(i)
        case l: Long => Integer.valueOf(l.toInt)
        case s: String => Integer.valueOf(s.toInt)
        case other => Integer.valueOf(other.toString.toInt)
      }.orNull
    } else {
      null
    }

    if (partition != null) {
      new ProducerRecord[String, String](topic, partition, key, value)
    } else if (key != null) {
      new ProducerRecord[String, String](topic, key, value)
    } else {
      new ProducerRecord[String, String](topic, value)
    }
  }
}
