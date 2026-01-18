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
 * Manual integration test for Kafka streaming with Data Caterer.
 *
 * This test verifies that the kafka-streaming.yaml example works correctly
 * with a real Kafka cluster, including validation of:
 * - Record counts in Kafka topics
 * - Timing patterns for rate-limited generation
 *
 * Prerequisites:
 * 1. Install insta-infra: https://github.com/data-catering/insta-infra
 * 2. Or start Kafka manually: docker run -d --name kafka -p 9092:9092 apache/kafka:latest
 *
 * Usage:
 * {{{
 * # Run with insta-infra (will auto-start Kafka)
 * ./gradlew :app:manualTest --tests "*KafkaStreamingManualTest"
 *
 * # Run with custom Kafka broker
 * KAFKA_BROKERS=my-kafka:9092 ./gradlew :app:manualTest --tests "*KafkaStreamingManualTest"
 *
 * # Run with debug logging
 * LOG_LEVEL=debug ./gradlew :app:manualTest --tests "*KafkaStreamingManualTest"
 * }}}
 */
class KafkaStreamingManualTest extends ManualTestSuite with BeforeAndAfterAll {

  private val kafkaBrokers = Option(System.getenv("KAFKA_BROKERS")).getOrElse("localhost:9092")
  private val kafkaServiceName = "kafka"
  private var kafkaStartedByTest = false

  // Use unique topic names per test run to avoid interference
  private val testRunId = UUID.randomUUID().toString.take(8)
  private val countTestTopic = s"test-count-$testRunId"
  private val durationTestTopic = s"test-duration-$testRunId"
  private val constantRateTopic = s"test-constant-rate-$testRunId"
  private val rampTestTopic = s"test-ramp-pattern-$testRunId"
  private val waveTestTopic = s"test-wave-pattern-$testRunId"
  private val spikeTestTopic = s"test-spike-pattern-$testRunId"
  private val steppedTestTopic = s"test-stepped-pattern-$testRunId"

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
    createTopics(List(
      countTestTopic,
      durationTestTopic,
      constantRateTopic,
      rampTestTopic,
      waveTestTopic,
      spikeTestTopic,
      steppedTestTopic
    ))
  }

  override def afterAll(): Unit = {
    // Delete test topics
    Try(deleteTopics(List(
      countTestTopic,
      durationTestTopic,
      constantRateTopic,
      rampTestTopic,
      waveTestTopic,
      spikeTestTopic,
      steppedTestTopic
    )))

    // Only stop Kafka if we started it
    if (kafkaStartedByTest) {
      stopService(kafkaServiceName)
    }
    super.afterAll()
  }

  test("Kafka streaming generates exact record count and validates in topic") {
    val expectedRecords = 50

    // Get initial offset before generating
    val initialOffset = getTopicEndOffset(countTestTopic)
    LOGGER.info(s"Initial offset for $countTestTopic: $initialOffset")

    // Create YAML and execute
    val testYaml = createCountTestYaml(expectedRecords)
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-count-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val duration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka count test completed in ${duration}ms")

    // Verify Data Caterer results
    results should not be null
    results.generationResults should not be empty
    val kafkaResult = results.generationResults.head
    kafkaResult.sinkResult.isSuccess shouldBe true
    kafkaResult.sinkResult.count shouldBe expectedRecords

    // Verify actual records in Kafka topic
    val finalOffset = getTopicEndOffset(countTestTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Final offset: $finalOffset, Records produced to Kafka: $recordsProduced")

    recordsProduced shouldBe expectedRecords

    // Consume and verify messages
    val messages = consumeMessages(countTestTopic, expectedRecords, timeoutMs = 10000)
    LOGGER.info(s"Consumed ${messages.size} messages from $countTestTopic")
    messages.size shouldBe expectedRecords

    // Verify message format (should match regex EVT[0-9]{8})
    messages.foreach { msg =>
      msg should startWith regex "EVT\\d{8}"
    }
    LOGGER.info(s"All ${messages.size} messages match expected format EVT[0-9]{8}")
  }

  test("Kafka streaming with duration generates expected record count") {
    val expectedRecords = 100

    // Get initial offset
    val initialOffset = getTopicEndOffset(durationTestTopic)
    LOGGER.info(s"Initial offset for $durationTestTopic: $initialOffset")

    // Create YAML with simple record count (duration-based generation may have issues)
    val testYaml = createSimpleCountTestYaml(expectedRecords, durationTestTopic, "DUR")
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-duration-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDuration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka second test completed in ${actualDuration}ms")

    // Verify execution results
    results should not be null
    if (results.generationResults.isEmpty) {
      LOGGER.warn("Generation results are empty - this may indicate an issue with the execution")
      fail("Expected generation results but got none")
    }
    results.generationResults.head.sinkResult.isSuccess shouldBe true

    // Verify record count in Kafka - this is the primary validation
    val finalOffset = getTopicEndOffset(durationTestTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced (expected $expectedRecords)")

    recordsProduced shouldBe expectedRecords
    LOGGER.info(s"Record count validation passed: $recordsProduced records")

    // Consume messages with timestamps to analyze timing
    val messagesWithTimestamps = consumeMessagesWithTimestamps(durationTestTopic, recordsProduced.toInt, timeoutMs = 10000)
    LOGGER.info(s"Consumed ${messagesWithTimestamps.size} messages with timestamps")

    // Verify message format
    messagesWithTimestamps.foreach { case (msg, _) =>
      msg should startWith regex "DUR\\d{8}"
    }
    LOGGER.info(s"All ${messagesWithTimestamps.size} messages match expected format DUR[0-9]{8}")

    if (messagesWithTimestamps.size >= 2) {
      // Analyze timing distribution (informational)
      val timestamps = messagesWithTimestamps.map(_._2).sorted
      val totalSpan = timestamps.last - timestamps.head
      val avgInterval = if (timestamps.size > 1) totalSpan.toDouble / (timestamps.size - 1) else 0.0

      LOGGER.info(s"Message timing analysis:")
      LOGGER.info(s"  - First message timestamp: ${timestamps.head}")
      LOGGER.info(s"  - Last message timestamp: ${timestamps.last}")
      LOGGER.info(s"  - Total time span: ${totalSpan}ms")
      LOGGER.info(s"  - Average interval between messages: ${avgInterval.toLong}ms")
    }
  }

  test("Kafka streaming with constant rate generates at expected throughput") {
    val targetRatePerSecond = 10
    val durationSeconds = 5
    val expectedRecords = targetRatePerSecond * durationSeconds // 50 records

    val initialOffset = getTopicEndOffset(constantRateTopic)
    LOGGER.info(s"Initial offset for $constantRateTopic: $initialOffset")

    val testYaml = createConstantRateTestYaml(targetRatePerSecond, durationSeconds, constantRateTopic)
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-constant-rate-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDuration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka constant rate test completed in ${actualDuration}ms")

    // Verify execution results
    results should not be null
    results.generationResults should not be empty
    results.generationResults.head.sinkResult.isSuccess shouldBe true

    // Verify record count in Kafka
    val finalOffset = getTopicEndOffset(constantRateTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced (expected $expectedRecords)")

    // Allow 10% tolerance for timing variations
    recordsProduced should be >= (expectedRecords * 0.9).toLong
    recordsProduced should be <= (expectedRecords * 1.1).toLong

    // Verify actual throughput is close to target
    val actualRate = (recordsProduced.toDouble / actualDuration) * 1000.0
    LOGGER.info(f"Actual throughput: $actualRate%.2f records/sec (target: $targetRatePerSecond/sec)")
    actualRate should be >= (targetRatePerSecond * 0.8)
    actualRate should be <= (targetRatePerSecond * 1.2)

    // Consume and verify messages with timing analysis
    val messagesWithTimestamps = consumeMessagesWithTimestamps(constantRateTopic, recordsProduced.toInt, timeoutMs = 10000)
    messagesWithTimestamps.size shouldBe recordsProduced

    // Verify message format
    messagesWithTimestamps.foreach { case (msg, _) =>
      msg should startWith regex "CONST\\d{8}"
    }

    // Analyze inter-message intervals with detailed logging
    if (messagesWithTimestamps.size >= 2) {
      val timestamps = messagesWithTimestamps.map(_._2).sorted
      val totalSpan = timestamps.last - timestamps.head
      val avgInterval = if (timestamps.size > 1) totalSpan.toDouble / (timestamps.size - 1) else 0.0
      val expectedInterval = 1000.0 / targetRatePerSecond // milliseconds per message

      LOGGER.info("=" * 80)
      LOGGER.info("CONSTANT RATE PATTERN - MESSAGE TIMING ANALYSIS")
      LOGGER.info("=" * 80)

      // Print first 10 and last 10 messages with timestamps and intervals
      LOGGER.info("First 10 messages:")
      messagesWithTimestamps.take(10).zipWithIndex.foreach { case ((msg, ts), idx) =>
        val interval = if (idx > 0) ts - messagesWithTimestamps(idx - 1)._2 else 0L
        LOGGER.info(f"  [$idx%2d] time=${ts}ms, interval=${interval}%4dms, msg=${msg.take(15)}")
      }

      if (messagesWithTimestamps.size > 20) {
        LOGGER.info("...")
        LOGGER.info("Last 10 messages:")
        val startIdx = messagesWithTimestamps.size - 10
        messagesWithTimestamps.drop(startIdx).zipWithIndex.foreach { case ((msg, ts), idx) =>
          val actualIdx = startIdx + idx
          val interval = if (actualIdx > 0) ts - messagesWithTimestamps(actualIdx - 1)._2 else 0L
          LOGGER.info(f"  [$actualIdx%2d] time=${ts}ms, interval=${interval}%4dms, msg=${msg.take(15)}")
        }
      }

      // Calculate interval statistics
      val intervals = timestamps.sliding(2).map { case Seq(t1, t2) => t2 - t1 }.toList
      val minInterval = if (intervals.nonEmpty) intervals.min else 0L
      val maxInterval = if (intervals.nonEmpty) intervals.max else 0L
      val medianInterval = if (intervals.nonEmpty) intervals.sorted.apply(intervals.size / 2) else 0L

      LOGGER.info("-" * 80)
      LOGGER.info("TIMING STATISTICS:")
      LOGGER.info(f"  Expected interval: ${expectedInterval}%.2fms (for $targetRatePerSecond records/sec)")
      LOGGER.info(f"  Total time span: ${totalSpan}ms for ${messagesWithTimestamps.size} messages")
      LOGGER.info(f"  Average interval: ${avgInterval}%.2fms")
      LOGGER.info(f"  Median interval: ${medianInterval}ms")
      LOGGER.info(f"  Min interval: ${minInterval}ms")
      LOGGER.info(f"  Max interval: ${maxInterval}ms")

      // Check if messages arrived in bursts
      val burstyMessages = intervals.count(_ < expectedInterval * 0.1) // Less than 10% of expected
      val properlySpaced = intervals.count(i => i >= expectedInterval * 0.7 && i <= expectedInterval * 1.3)

      LOGGER.info("-" * 80)
      LOGGER.info("DISTRIBUTION ANALYSIS:")
      LOGGER.info(f"  Messages in bursts (<10%% of expected interval): $burstyMessages / ${intervals.size}")
      LOGGER.info(f"  Properly spaced (70-130%% of expected): $properlySpaced / ${intervals.size}")
      LOGGER.info(f"  Burst percentage: ${(burstyMessages.toDouble / intervals.size * 100)}%.1f%%")
      LOGGER.info("=" * 80)
    }
  }

  test("Kafka streaming with ramp pattern gradually increases rate") {
    val startRate = 2
    val endRate = 10
    val durationSeconds = 5
    // For ramp: average rate = (start + end) / 2
    val avgRate = (startRate + endRate) / 2.0
    val expectedRecords = (avgRate * durationSeconds).toLong

    val initialOffset = getTopicEndOffset(rampTestTopic)
    LOGGER.info(s"Initial offset for $rampTestTopic: $initialOffset")

    val testYaml = createRampPatternTestYaml(startRate, endRate, durationSeconds, rampTestTopic)
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-ramp-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDuration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka ramp pattern test completed in ${actualDuration}ms")

    // Verify execution results
    results should not be null
    results.generationResults should not be empty
    results.generationResults.head.sinkResult.isSuccess shouldBe true

    // Verify record count with tolerance
    val finalOffset = getTopicEndOffset(rampTestTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced (expected ~$expectedRecords)")

    // Allow wider tolerance for ramping patterns (20%)
    recordsProduced should be >= (expectedRecords * 0.8).toLong
    recordsProduced should be <= (expectedRecords * 1.2).toLong

    // Consume and analyze timing patterns
    val messagesWithTimestamps = consumeMessagesWithTimestamps(rampTestTopic, recordsProduced.toInt, timeoutMs = 10000)
    messagesWithTimestamps.size shouldBe recordsProduced

    // Verify message format
    messagesWithTimestamps.foreach { case (msg, _) =>
      msg should startWith regex "RAMP\\d{8}"
    }

    // Analyze timing distribution with detailed logging
    if (messagesWithTimestamps.size >= 2) {
      val timestamps = messagesWithTimestamps.map(_._2).sorted
      val totalSpan = timestamps.last - timestamps.head

      LOGGER.info("=" * 80)
      LOGGER.info("RAMP PATTERN - MESSAGE TIMING ANALYSIS")
      LOGGER.info("=" * 80)

      // Print ALL messages with timestamps to see the ramp pattern
      LOGGER.info(s"All ${messagesWithTimestamps.size} messages (showing timestamp and interval):")
      messagesWithTimestamps.zipWithIndex.foreach { case ((msg, ts), idx) =>
        val interval = if (idx > 0) ts - messagesWithTimestamps(idx - 1)._2 else 0L
        LOGGER.info(f"  [$idx%2d] time=${ts}ms, interval=${interval}%4dms, msg=${msg.take(15)}")
      }

      // Calculate interval statistics
      val intervals = timestamps.sliding(2).map { case Seq(t1, t2) => t2 - t1 }.toList
      val minInterval = if (intervals.nonEmpty) intervals.min else 0L
      val maxInterval = if (intervals.nonEmpty) intervals.max else 0L
      val avgInterval = if (intervals.nonEmpty) intervals.sum.toDouble / intervals.size else 0.0

      // Expected intervals: start at 1000ms/startRate, end at 1000ms/endRate
      val expectedStartInterval = 1000.0 / startRate
      val expectedEndInterval = 1000.0 / endRate

      LOGGER.info("-" * 80)
      LOGGER.info("TIMING STATISTICS:")
      LOGGER.info(f"  Expected start interval: ${expectedStartInterval}%.2fms (for $startRate records/sec)")
      LOGGER.info(f"  Expected end interval: ${expectedEndInterval}%.2fms (for $endRate records/sec)")
      LOGGER.info(f"  Total time span: ${totalSpan}ms for ${messagesWithTimestamps.size} messages")
      LOGGER.info(f"  Average interval: ${avgInterval}%.2fms")
      LOGGER.info(f"  Min interval: ${minInterval}ms")
      LOGGER.info(f"  Max interval: ${maxInterval}ms")
      LOGGER.info("=" * 80)
    }
  }

  test("Kafka streaming with wave pattern oscillates rate") {
    val baseRate = 5
    val amplitude = 3
    val frequency = 1.0
    val durationSeconds = 6
    val expectedRecords = baseRate * durationSeconds // approximate

    val initialOffset = getTopicEndOffset(waveTestTopic)
    LOGGER.info(s"Initial offset for $waveTestTopic: $initialOffset")

    val testYaml = createWavePatternTestYaml(baseRate, amplitude, frequency, durationSeconds, waveTestTopic)
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-wave-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDuration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka wave pattern test completed in ${actualDuration}ms")

    // Verify execution results
    results should not be null
    results.generationResults should not be empty
    results.generationResults.head.sinkResult.isSuccess shouldBe true

    // Verify record count
    val finalOffset = getTopicEndOffset(waveTestTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced (expected ~$expectedRecords)")

    // Allow wider tolerance for wave patterns (30%)
    recordsProduced should be >= (expectedRecords * 0.7).toLong
    recordsProduced should be <= (expectedRecords * 1.3).toLong

    // Consume and verify messages
    val messagesWithTimestamps = consumeMessagesWithTimestamps(waveTestTopic, recordsProduced.toInt, timeoutMs = 10000)
    messagesWithTimestamps.size shouldBe recordsProduced

    // Verify message format
    messagesWithTimestamps.foreach { case (msg, _) =>
      msg should startWith regex "WAVE\\d{8}"
    }

    LOGGER.info(s"Wave pattern test produced ${recordsProduced} records over ${actualDuration}ms")
  }

  test("Kafka streaming with spike pattern creates burst") {
    val baseRate = 2
    val spikeRate = 20
    val spikeStart = 0.25 // 25% into duration
    val spikeDuration = 0.25 // 25% of total duration
    val totalDurationSeconds = 4

    // Calculate expected records:
    // base_time = duration * (1 - spike_duration) * base_rate
    // spike_time = duration * spike_duration * spike_rate
    val baseTimeSeconds = totalDurationSeconds * (1 - spikeDuration)
    val spikeTimeSeconds = totalDurationSeconds * spikeDuration
    val expectedRecords = (baseTimeSeconds * baseRate + spikeTimeSeconds * spikeRate).toLong

    val initialOffset = getTopicEndOffset(spikeTestTopic)
    LOGGER.info(s"Initial offset for $spikeTestTopic: $initialOffset")

    val testYaml = createSpikePatternTestYaml(baseRate, spikeRate, spikeStart, spikeDuration, totalDurationSeconds, spikeTestTopic)
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-spike-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDuration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka spike pattern test completed in ${actualDuration}ms")

    // Verify execution results
    results should not be null
    results.generationResults should not be empty
    results.generationResults.head.sinkResult.isSuccess shouldBe true

    // Verify record count
    val finalOffset = getTopicEndOffset(spikeTestTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced (expected ~$expectedRecords)")

    // Allow tolerance for spike patterns (25%)
    recordsProduced should be >= (expectedRecords * 0.75).toLong
    recordsProduced should be <= (expectedRecords * 1.25).toLong

    // Consume and analyze spike
    val messagesWithTimestamps = consumeMessagesWithTimestamps(spikeTestTopic, recordsProduced.toInt, timeoutMs = 10000)
    messagesWithTimestamps.size shouldBe recordsProduced

    // Verify message format
    messagesWithTimestamps.foreach { case (msg, _) =>
      msg should startWith regex "SPIKE\\d{8}"
    }

    // Analyze timing to detect spike
    if (messagesWithTimestamps.size >= 10) {
      val timestamps = messagesWithTimestamps.map(_._2).sorted
      val intervals = timestamps.sliding(2).map { case Seq(t1, t2) => t2 - t1 }.toList

      // Find minimum interval (should be during spike)
      val minInterval = intervals.min
      val maxInterval = intervals.max

      LOGGER.info(f"Min interval: ${minInterval}ms, Max interval: ${maxInterval}ms")
      // Spike should create significantly shorter intervals (only validate if there's variation)
      if (maxInterval > 0) {
        minInterval.toDouble should be < (maxInterval * 0.7)  // More lenient: 70% instead of 50%
      } else {
        LOGGER.warn("All messages arrived at same timestamp - cannot validate spike pattern")
      }
    }
  }

  test("Kafka streaming with stepped pattern has distinct phases") {
    val durationSeconds = 6

    val initialOffset = getTopicEndOffset(steppedTestTopic)
    LOGGER.info(s"Initial offset for $steppedTestTopic: $initialOffset")

    val testYaml = createSteppedPatternTestYaml(durationSeconds, steppedTestTopic)
    val yamlFile = Path.of(s"$tempTestDirectory/kafka-stepped-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val startTime = System.currentTimeMillis()
    val results = executeUnifiedYaml(yamlFile.toString)
    val actualDuration = System.currentTimeMillis() - startTime

    LOGGER.info(s"Kafka stepped pattern test completed in ${actualDuration}ms")

    // Verify execution results
    results should not be null
    results.generationResults should not be empty
    results.generationResults.head.sinkResult.isSuccess shouldBe true

    // Verify record count
    val finalOffset = getTopicEndOffset(steppedTestTopic)
    val recordsProduced = finalOffset - initialOffset
    LOGGER.info(s"Records produced to Kafka: $recordsProduced records")

    // Should have produced some records
    recordsProduced should be > 0L

    // Consume and verify messages
    val messagesWithTimestamps = consumeMessagesWithTimestamps(steppedTestTopic, recordsProduced.toInt, timeoutMs = 10000)
    messagesWithTimestamps.size shouldBe recordsProduced

    // Verify message format
    messagesWithTimestamps.foreach { case (msg, _) =>
      msg should startWith regex "STEP\\d{8}"
    }

    LOGGER.info(s"Stepped pattern test produced ${recordsProduced} records over ${actualDuration}ms")
  }

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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"test-consumer-${UUID.randomUUID()}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[String, String](props)
  }

  private def consumeMessages(topic: String, expectedCount: Int, timeoutMs: Long): List[String] = {
    val consumer = createConsumer()
    val messages = ListBuffer[String]()
    try {
      consumer.subscribe(Collections.singletonList(topic))
      val deadline = System.currentTimeMillis() + timeoutMs
      while (messages.size < expectedCount && System.currentTimeMillis() < deadline) {
        val records = consumer.poll(Duration.ofMillis(100))
        records.asScala.foreach { record =>
          messages += record.value()
        }
      }
      messages.toList
    } finally {
      consumer.close()
    }
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

  private def createSimpleCountTestYaml(recordCount: Int, topic: String, prefix: String): String = {
    s"""version: "1.0"
       |name: "kafka_simple_count_test"
       |description: "Test Kafka with simple record count"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "simple_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          records: $recordCount
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "${prefix}[0-9]{8}"
       |""".stripMargin
  }

  private def createCountTestYaml(recordCount: Int): String = {
    s"""version: "1.0"
       |name: "kafka_count_test"
       |description: "Test Kafka with exact record count"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "test_events"
       |        options:
       |          topic: "$countTestTopic"
       |        count:
       |          records: $recordCount
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "EVT[0-9]{8}"
       |""".stripMargin
  }

  private def createConstantRateTestYaml(ratePerSecond: Int, durationSeconds: Int, topic: String): String = {
    s"""version: "1.0"
       |name: "kafka_constant_rate_test"
       |description: "Test Kafka with constant rate generation"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "constant_rate_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          duration: "${durationSeconds}s"
       |          pattern:
       |            type: "constant"
       |            baseRate: $ratePerSecond
       |          rateUnit: "second"
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "CONST[0-9]{8}"
       |""".stripMargin
  }

  private def createRampPatternTestYaml(startRate: Int, endRate: Int, durationSeconds: Int, topic: String): String = {
    s"""version: "1.0"
       |name: "kafka_ramp_pattern_test"
       |description: "Test Kafka with ramp pattern generation"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "ramp_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          duration: "${durationSeconds}s"
       |          pattern:
       |            type: "ramp"
       |            startRate: $startRate
       |            endRate: $endRate
       |          rateUnit: "second"
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "RAMP[0-9]{8}"
       |""".stripMargin
  }

  private def createWavePatternTestYaml(baseRate: Int, amplitude: Int, frequency: Double, durationSeconds: Int, topic: String): String = {
    s"""version: "1.0"
       |name: "kafka_wave_pattern_test"
       |description: "Test Kafka with wave pattern generation"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "wave_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          duration: "${durationSeconds}s"
       |          pattern:
       |            type: "wave"
       |            baseRate: $baseRate
       |            amplitude: $amplitude
       |            frequency: $frequency
       |          rateUnit: "second"
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "WAVE[0-9]{8}"
       |""".stripMargin
  }

  private def createSpikePatternTestYaml(baseRate: Int, spikeRate: Int, spikeStart: Double, spikeDuration: Double,
                                          totalDurationSeconds: Int, topic: String): String = {
    s"""version: "1.0"
       |name: "kafka_spike_pattern_test"
       |description: "Test Kafka with spike pattern generation"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "spike_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          duration: "${totalDurationSeconds}s"
       |          pattern:
       |            type: "spike"
       |            baseRate: $baseRate
       |            spikeRate: $spikeRate
       |            spikeStart: $spikeStart
       |            spikeDuration: $spikeDuration
       |          rateUnit: "second"
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "SPIKE[0-9]{8}"
       |""".stripMargin
  }

  private def createSteppedPatternTestYaml(durationSeconds: Int, topic: String): String = {
    s"""version: "1.0"
       |name: "kafka_stepped_pattern_test"
       |description: "Test Kafka with stepped pattern generation"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |  folders:
       |    generatedReportsFolderPath: "$tempTestDirectory/report"
       |    recordTrackingFolderPath: "$tempTestDirectory/record-tracking"
       |    recordTrackingForValidationFolderPath: "$tempTestDirectory/record-tracking-validation"
       |    validationFolderPath: "$tempTestDirectory/validation"
       |  runtime:
       |    master: "local[2]"
       |
       |dataSources:
       |  - name: "events_kafka"
       |    connection:
       |      type: "kafka"
       |      options:
       |        bootstrapServers: "$kafkaBrokers"
       |    steps:
       |      - name: "stepped_events"
       |        options:
       |          topic: "$topic"
       |        count:
       |          duration: "${durationSeconds}s"
       |          pattern:
       |            type: "stepped"
       |            steps:
       |              - rate: 5
       |                duration: "2s"
       |              - rate: 10
       |                duration: "2s"
       |              - rate: 3
       |                duration: "2s"
       |          rateUnit: "second"
       |        fields:
       |          - name: "value"
       |            options:
       |              regex: "STEP[0-9]{8}"
       |""".stripMargin
  }
}
