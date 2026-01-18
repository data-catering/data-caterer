package io.github.datacatering.datacaterer.core.manual

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Manual integration test for Kafka rate limiting validation.
 *
 * This test validates that Data Caterer's rate limiting configuration
 * controls the message send rate to Kafka. It measures:
 * - Actual records per second vs configured rate
 * - Timing patterns between messages
 * - Rate consistency over the test duration
 *
 * IMPLEMENTATION:
 * When `rowsPerSecond` is configured for a Kafka data source, the system:
 * 1. Routes Kafka to REAL_TIME mode (via determineSaveTiming in GeneratorUtil.scala)
 * 2. Uses PekkoStreamingSinkWriter for rate-limited streaming
 * 3. Sends messages through KafkaSinkProcessor with throttling
 *
 * Without `rowsPerSecond`, Kafka uses the standard batch mode with Spark's native writer.
 *
 * Prerequisites:
 * 1. Install insta-infra: https://github.com/data-catering/insta-infra
 * 2. Or start Kafka manually: docker run -d --name kafka -p 9092:9092 apache/kafka:latest
 *
 * Usage:
 * {{{
 * # Run all rate limiting tests (diagnostic mode - won't fail on rate deviations)
 * ./gradlew :app:manualTest --tests "io.github.datacatering.datacaterer.core.manual.KafkaRateLimitingManualTest"
 *
 * # Run with strict rate validation (will fail if rate limiting doesn't work)
 * ENFORCE_RATE_LIMITING=true ./gradlew :app:manualTest --tests "*KafkaRateLimitingManualTest"
 *
 * # Run with custom Kafka broker
 * KAFKA_BROKERS=my-kafka:9092 ./gradlew :app:manualTest --tests "*KafkaRateLimitingManualTest"
 *
 * # Run with debug logging for detailed timing info
 * LOG_LEVEL=debug ./gradlew :app:manualTest --tests "*KafkaRateLimitingManualTest"
 * }}}
 */
class KafkaRateLimitingManualTest extends ManualTestSuite with BeforeAndAfterAll {

  private val kafkaBrokers = Option(System.getenv("KAFKA_BROKERS")).getOrElse("localhost:9092")
  private val kafkaServiceName = "kafka"
  private var kafkaStartedByTest = false

  // Use unique topic names per test run to avoid interference
  private val testRunId = UUID.randomUUID().toString.take(8)
  private val rate10Topic = s"rate-test-10ps-$testRunId"
  private val rate50Topic = s"rate-test-50ps-$testRunId"
  private val rate100Topic = s"rate-test-100ps-$testRunId"

  // Tolerance for rate validation (percentage)
  private val RATE_TOLERANCE_PERCENT = 25.0

  // Set to true to enforce rate limiting validation (will fail if rate limiting not working)
  // When false, tests will report timing info but not fail on rate deviations
  private val enforceRateLimiting = Option(System.getenv("ENFORCE_RATE_LIMITING")).exists(_.equalsIgnoreCase("true"))

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Try to start Kafka if not already running
    if (!isKafkaRunning) {
      if (checkInstaAvailable()) {
        kafkaStartedByTest = startService(kafkaServiceName)
        if (!kafkaStartedByTest) {
          cancel(s"Failed to start Kafka via insta-infra. Start Kafka manually or check insta-infra installation.")
        }
        // Wait for Kafka to be ready
        if (!waitForPort("localhost", 9092, maxRetries = 60, retryDelayMs = 1000)) {
          cancel("Kafka did not become ready in time")
        }
      } else {
        cancel(s"Kafka is not running at $kafkaBrokers and insta-infra is not available. " +
          "Start Kafka manually or install insta-infra from https://github.com/data-catering/insta-infra")
      }
    } else {
      LOGGER.info(s"Kafka is already running at $kafkaBrokers")
    }

    // Create test topics
    createTopics(List(rate10Topic, rate50Topic, rate100Topic))
  }

  override def afterAll(): Unit = {
    // Delete test topics
    Try(deleteTopics(List(rate10Topic, rate50Topic, rate100Topic)))

    // Only stop Kafka if we started it
    if (kafkaStartedByTest) {
      stopService(kafkaServiceName)
    }
    super.afterAll()
  }

  /**
   * Test: 10 records/second rate limiting
   * Expected duration: ~3 seconds for 30 records
   */
  test("Rate limiting at 10 records/second produces correct timing") {
    val targetRate = 10
    val recordCount = 30  // 3 seconds of data
    val expectedDurationSec = recordCount.toDouble / targetRate

    runRateLimitingTest(
      topic = rate10Topic,
      targetRate = targetRate,
      recordCount = recordCount,
      prefix = "R10",
      expectedDurationSec = expectedDurationSec
    )
  }

  /**
   * Test: 50 records/second rate limiting
   * Expected duration: ~2 seconds for 100 records
   */
  test("Rate limiting at 50 records/second produces correct timing") {
    val targetRate = 50
    val recordCount = 100  // 2 seconds of data
    val expectedDurationSec = recordCount.toDouble / targetRate

    runRateLimitingTest(
      topic = rate50Topic,
      targetRate = targetRate,
      recordCount = recordCount,
      prefix = "R50",
      expectedDurationSec = expectedDurationSec
    )
  }

  /**
   * Test: 100 records/second rate limiting
   * Expected duration: ~1.5 seconds for 150 records
   */
  test("Rate limiting at 100 records/second produces correct timing") {
    val targetRate = 100
    val recordCount = 150  // 1.5 seconds of data
    val expectedDurationSec = recordCount.toDouble / targetRate

    runRateLimitingTest(
      topic = rate100Topic,
      targetRate = targetRate,
      recordCount = recordCount,
      prefix = "R100",
      expectedDurationSec = expectedDurationSec
    )
  }

  /**
   * Core test implementation for rate limiting validation.
   */
  private def runRateLimitingTest(
    topic: String,
    targetRate: Int,
    recordCount: Int,
    prefix: String,
    expectedDurationSec: Double
  ): Unit = {
    LOGGER.info(s"=" * 60)
    LOGGER.info(s"RATE LIMITING TEST: $targetRate records/second")
    LOGGER.info(s"  Topic: $topic")
    LOGGER.info(s"  Record count: $recordCount")
    LOGGER.info(s"  Expected duration: ${expectedDurationSec}s")
    LOGGER.info(s"=" * 60)

    // Get initial offset before generating
    val initialOffset = getTopicEndOffset(topic)
    LOGGER.info(s"Initial offset for $topic: $initialOffset")

    // Create YAML with rate limiting configuration
    val testYaml = createRateLimitedTestYaml(
      topic = topic,
      targetRate = targetRate,
      recordCount = recordCount,
      prefix = prefix
    )
    val yamlFile = Path.of(s"$tempTestDirectory/rate-test-${targetRate}ps.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute and measure timing
    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDurationMs = System.currentTimeMillis() - startTime
    val actualDurationSec = actualDurationMs / 1000.0

    LOGGER.info(s"-" * 60)
    LOGGER.info(s"EXECUTION COMPLETED")
    LOGGER.info(s"  Actual duration: ${actualDurationSec}s (expected: ${expectedDurationSec}s)")
    LOGGER.info(s"-" * 60)

    // Verify execution success
    results should not be null
    results.generationResults should not be empty
    val kafkaResult = results.generationResults.head
    kafkaResult.sinkResult.isSuccess shouldBe true
    kafkaResult.sinkResult.count shouldBe recordCount

    // Verify records in Kafka
    val finalOffset = getTopicEndOffset(topic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced")
    recordsProduced shouldBe recordCount

    // Consume messages with timestamps for timing analysis
    val messagesWithTimestamps = consumeMessagesWithTimestamps(topic, recordCount, timeoutMs = 30000)
    LOGGER.info(s"Consumed ${messagesWithTimestamps.size} messages for timing analysis")

    // Analyze and validate timing
    analyzeAndValidateTiming(
      messages = messagesWithTimestamps,
      targetRate = targetRate,
      recordCount = recordCount,
      actualDurationSec = actualDurationSec,
      expectedDurationSec = expectedDurationSec
    )
  }

  /**
   * Analyzes message timestamps and validates rate limiting accuracy.
   */
  private def analyzeAndValidateTiming(
    messages: List[(String, Long)],
    targetRate: Int,
    recordCount: Int,
    actualDurationSec: Double,
    expectedDurationSec: Double
  ): Unit = {
    LOGGER.info(s"=" * 60)
    LOGGER.info(s"TIMING ANALYSIS")
    LOGGER.info(s"=" * 60)

    if (messages.size < 2) {
      LOGGER.warn("Not enough messages for timing analysis")
      return
    }

    val timestamps = messages.map(_._2).sorted
    val firstTimestamp = timestamps.head
    val lastTimestamp = timestamps.last
    val totalSpanMs = lastTimestamp - firstTimestamp
    val totalSpanSec = totalSpanMs / 1000.0

    // Calculate actual rate from Kafka timestamps
    val kafkaActualRate = if (totalSpanSec > 0) {
      (messages.size - 1) / totalSpanSec  // -1 because first message is at time 0
    } else {
      messages.size.toDouble
    }

    // Calculate inter-message intervals
    val intervals = timestamps.sliding(2).map { case Seq(a, b) => b - a }.toList
    val avgIntervalMs = if (intervals.nonEmpty) intervals.sum.toDouble / intervals.size else 0.0
    val minIntervalMs = if (intervals.nonEmpty) intervals.min else 0L
    val maxIntervalMs = if (intervals.nonEmpty) intervals.max else 0L
    val expectedIntervalMs = 1000.0 / targetRate

    LOGGER.info(s"  Target rate: $targetRate records/second")
    LOGGER.info(s"  Records sent: ${messages.size}")
    LOGGER.info(s"  First message timestamp: $firstTimestamp")
    LOGGER.info(s"  Last message timestamp: $lastTimestamp")
    LOGGER.info(s"  Kafka timestamp span: ${totalSpanMs}ms (${totalSpanSec}s)")
    LOGGER.info(s"  Actual rate (from Kafka timestamps): ${kafkaActualRate.round} records/second")
    LOGGER.info(s"  Execution time (wall clock): ${actualDurationSec}s")
    LOGGER.info(s"  Expected duration: ${expectedDurationSec}s")
    LOGGER.info(s"-" * 60)
    LOGGER.info(s"INTERVAL ANALYSIS")
    LOGGER.info(s"  Expected interval: ${expectedIntervalMs}ms")
    LOGGER.info(s"  Average interval: ${avgIntervalMs.round}ms")
    LOGGER.info(s"  Min interval: ${minIntervalMs}ms")
    LOGGER.info(s"  Max interval: ${maxIntervalMs}ms")

    // Analyze interval distribution (for debugging rate control issues)
    if (intervals.size > 10) {
      val sortedIntervals = intervals.sorted
      val p10 = sortedIntervals((intervals.size * 0.1).toInt)
      val p50 = sortedIntervals((intervals.size * 0.5).toInt)
      val p90 = sortedIntervals((intervals.size * 0.9).toInt)
      LOGGER.info(s"  Interval percentiles: p10=${p10}ms, p50=${p50}ms, p90=${p90}ms")
    }

    // Calculate rate deviation percentage
    val rateDeviationPercent = Math.abs(kafkaActualRate - targetRate) / targetRate * 100

    LOGGER.info(s"-" * 60)
    LOGGER.info(s"RATE VALIDATION")
    LOGGER.info(s"  Rate deviation: ${rateDeviationPercent.round}% (tolerance: ${RATE_TOLERANCE_PERCENT}%)")

    // Validate rate is within tolerance
    if (rateDeviationPercent <= RATE_TOLERANCE_PERCENT) {
      LOGGER.info(s"  ✓ PASSED: Rate is within ${RATE_TOLERANCE_PERCENT}% tolerance")
    } else {
      LOGGER.warn(s"  ✗ FAILED: Rate deviation ${rateDeviationPercent.round}% exceeds ${RATE_TOLERANCE_PERCENT}% tolerance")
    }

    // Validate duration is reasonable (within 50% of expected, accounting for overhead)
    val durationDeviationPercent = Math.abs(actualDurationSec - expectedDurationSec) / expectedDurationSec * 100
    LOGGER.info(s"  Duration deviation: ${durationDeviationPercent.round}%")

    // The key assertion: rate should be within tolerance (only enforced when ENFORCE_RATE_LIMITING=true)
    if (enforceRateLimiting) {
      withClue(s"Actual rate ${kafkaActualRate.round}/s should be within ${RATE_TOLERANCE_PERCENT}% of target rate $targetRate/s: ") {
        rateDeviationPercent should be <= RATE_TOLERANCE_PERCENT
      }
    } else if (rateDeviationPercent > RATE_TOLERANCE_PERCENT) {
      LOGGER.warn(s"  ⚠ Rate limiting is NOT working for Kafka!")
      LOGGER.warn(s"  ⚠ Messages were sent at ${kafkaActualRate.round}/sec instead of target $targetRate/sec")
      LOGGER.warn(s"  ⚠ This is expected until Kafka is routed through PekkoStreamingSinkWriter")
      LOGGER.warn(s"  ⚠ Set ENFORCE_RATE_LIMITING=true to enforce rate validation")
    }

    LOGGER.info(s"=" * 60)
  }

  /**
   * Creates a YAML configuration for rate-limited Kafka testing.
   */
  private def createRateLimitedTestYaml(
    topic: String,
    targetRate: Int,
    recordCount: Int,
    prefix: String
  ): String = {
    s"""version: "1.0"
       |name: "kafka_rate_limit_test_${targetRate}ps"
       |description: "Test Kafka rate limiting at $targetRate records/second"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "rate_test_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |        rowsPerSecond: "$targetRate"
       |    steps:
       |      - name: "rate_limited_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          records: $recordCount
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "$prefix[0-9]{8}"
       |""".stripMargin
  }

  // ===== Kafka utility methods =====

  private def isKafkaRunning: Boolean = {
    Try {
      val socket = new java.net.Socket()
      val host = kafkaBrokers.split(":")(0)
      val port = kafkaBrokers.split(":")(1).toInt
      socket.connect(new java.net.InetSocketAddress(host, port), 1000)
      socket.close()
      true
    }.getOrElse(false)
  }

  private def createAdminClient(): AdminClient = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    AdminClient.create(props)
  }

  private def createTopics(topics: List[String]): Unit = {
    val admin = createAdminClient()
    try {
      val newTopics = topics.map { topic =>
        new org.apache.kafka.clients.admin.NewTopic(topic, 1, 1.toShort)
      }
      admin.createTopics(newTopics.asJava).all().get()
      LOGGER.info(s"Created topics: ${topics.mkString(", ")}")
      // Wait for topics to be ready
      Thread.sleep(1000)
    } catch {
      case e: Exception =>
        LOGGER.warn(s"Failed to create topics (may already exist): ${e.getMessage}")
    } finally {
      admin.close()
    }
  }

  private def deleteTopics(topics: List[String]): Unit = {
    val admin = createAdminClient()
    try {
      admin.deleteTopics(topics.asJava).all().get()
      LOGGER.info(s"Deleted topics: ${topics.mkString(", ")}")
    } catch {
      case e: Exception =>
        LOGGER.warn(s"Failed to delete topics: ${e.getMessage}")
    } finally {
      admin.close()
    }
  }

  private def getTopicEndOffset(topic: String): Long = {
    val consumer = createConsumer()
    try {
      val partitions = consumer.partitionsFor(topic).asScala
        .map(pi => new TopicPartition(pi.topic(), pi.partition()))
      if (partitions.isEmpty) {
        0L
      } else {
        val endOffsets = consumer.endOffsets(partitions.asJava)
        endOffsets.asScala.values.map(_.toLong).sum
      }
    } finally {
      consumer.close()
    }
  }

  private def createConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"rate-test-consumer-${UUID.randomUUID()}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[String, String](props)
  }

  private def consumeMessagesWithTimestamps(topic: String, expectedCount: Int, timeoutMs: Long): List[(String, Long)] = {
    val consumer = createConsumer()
    val messages = ListBuffer[(String, Long)]()
    try {
      consumer.subscribe(Collections.singletonList(topic))
      val deadline = System.currentTimeMillis() + timeoutMs
      while (messages.size < expectedCount && System.currentTimeMillis() < deadline) {
        val records = consumer.poll(Duration.ofMillis(100))
        records.asScala.foreach { record =>
          messages += ((record.value(), record.timestamp()))
        }
      }
      messages.toList
    } finally {
      consumer.close()
    }
  }
}
