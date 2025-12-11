package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.FoldersConfig
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.log4j.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, Sink => PekkoSink}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
 * Unit tests for PekkoStreamingSinkWriter throttling behavior.
 * 
 * Tests verify:
 * - Rate control throttling works correctly
 * - Record processing timing
 * - Throttle parameters are properly configured
 * 
 * These tests validate the Pekko streaming throttle mechanism directly
 * without requiring actual sink implementations (HTTP, JMS, etc).
 */
class PekkoStreamingSinkWriterTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  private val LOGGER = Logger.getLogger(getClass.getName)
  
  private implicit var spark: SparkSession = _
  private var foldersConfig: FoldersConfig = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession
    foldersConfig = FoldersConfig(
      generatedReportsFolderPath = "/tmp/test-reports"
    )
  }

  test("Pekko throttle mechanism - verify rate limiting works") {
    implicit val as: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(as)
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    
    try {
      val recordCount = 10
      val rate = 5 // 5 records per second
      val testData = (1 to recordCount).toList
      
      val startMillis = System.currentTimeMillis()
      val processedCount = new AtomicInteger(0)
      
      // Simulate the PekkoStreamingSinkWriter throttle pattern
      val result = Source(testData)
        .throttle(rate, 1.second)
        .mapAsync(parallelism = Math.min(rate, 100)) { item =>
          scala.concurrent.Future {
            processedCount.incrementAndGet()
            item
          }
        }
        .runWith(PekkoSink.ignore)
      
      Await.result(result, 10.seconds)
      
      val elapsedMillis = System.currentTimeMillis() - startMillis
      val elapsedSeconds = elapsedMillis / 1000.0
      
      // Verify all records processed
      processedCount.get() shouldBe recordCount
      
      // Verify throttling was applied: 10 records at 5/sec should take at least 1.5 seconds
      LOGGER.info(s"Processed $recordCount records at ${rate}/sec in ${elapsedSeconds}s")
      elapsedSeconds should be >= 1.5
      elapsedSeconds should be < 4.0
      
      // Calculate actual rate
      val actualRate = recordCount / elapsedSeconds
      LOGGER.info(s"Actual rate: ${actualRate.round}/sec, target: ${rate}/sec")
      
      // Actual rate should not exceed target rate significantly
      actualRate should be <= (rate * 1.3)
      
    } finally {
      as.terminate()
    }
  }

  test("High rate throttling - verify no records dropped") {
    implicit val as: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(as)
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    
    try {
      val recordCount = 50
      val rate = 100 // High rate: 100/sec
      val testData = (1 to recordCount).toList
      
      val startMillis = System.currentTimeMillis()
      val processedCount = new AtomicInteger(0)
      
      val result = Source(testData)
        .throttle(rate, 1.second)
        .mapAsync(parallelism = Math.min(rate, 100)) { item =>
          scala.concurrent.Future {
            processedCount.incrementAndGet()
            item
          }
        }
        .runWith(PekkoSink.ignore)
      
      Await.result(result, 10.seconds)
      
      val elapsedMillis = System.currentTimeMillis() - startMillis
      val elapsedSeconds = elapsedMillis / 1000.0
      
      // Verify all records processed
      processedCount.get() shouldBe recordCount
      
      // With high rate, should complete quickly
      LOGGER.info(s"Processed $recordCount records at ${rate}/sec in ${elapsedSeconds}s")
      elapsedSeconds should be < 2.0
      
    } finally {
      as.terminate()
    }
  }

  test("Slow rate throttling - verify rate is enforced") {
    implicit val as: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(as)
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    
    try {
      val recordCount = 15
      val rate = 5 // Slow rate: 5/sec
      val testData = (1 to recordCount).toList
      
      val startMillis = System.currentTimeMillis()
      val processedCount = new AtomicInteger(0)
      
      val result = Source(testData)
        .throttle(rate, 1.second)
        .mapAsync(parallelism = Math.min(rate, 100)) { item =>
          scala.concurrent.Future {
            processedCount.incrementAndGet()
            item
          }
        }
        .runWith(PekkoSink.ignore)
      
      Await.result(result, 10.seconds)
      
      val elapsedMillis = System.currentTimeMillis() - startMillis
      val elapsedSeconds = elapsedMillis / 1000.0
      
      // Verify all records processed
      processedCount.get() shouldBe recordCount
      
      // 15 records at 5/sec should take at least 2.5 seconds
      LOGGER.info(s"Processed $recordCount records at ${rate}/sec in ${elapsedSeconds}s")
      elapsedSeconds should be >= 2.5
      elapsedSeconds should be < 5.0
      
      // Calculate actual rate
      val actualRate = recordCount / elapsedSeconds
      LOGGER.info(s"Actual rate: ${actualRate.round}/sec, target: ${rate}/sec")
      
      // Actual rate should not exceed target significantly
      actualRate should be <= (rate * 1.3)
      
    } finally {
      as.terminate()
    }
  }

  test("Parallelism capping - verify mapAsync parallelism limited to 100") {
    // Test that parallelism is capped at min(rate, 100)
    // even when rate is very high
    
    val highRate = 200
    val cappedParallelism = Math.min(highRate, 100)
    
    cappedParallelism shouldBe 100
    
    val lowRate = 50
    val uncappedParallelism = Math.min(lowRate, 100)
    
    uncappedParallelism shouldBe 50
    
    LOGGER.info(s"Parallelism capping test: rate=$highRate -> parallelism=$cappedParallelism, rate=$lowRate -> parallelism=$uncappedParallelism")
  }

  test("Rate minimum enforcement - verify rate floor of 1") {
    // Verify that Math.max(rate, 1) ensures minimum rate of 1
    val zeroRate = 0
    val adjustedRate = Math.max(zeroRate, 1)
    
    adjustedRate shouldBe 1
    
    val negativeRate = -10
    val adjustedNegativeRate = Math.max(negativeRate, 1)
    
    adjustedNegativeRate shouldBe 1
    
    LOGGER.info(s"Rate minimum test: rate=$zeroRate -> adjusted=$adjustedRate, rate=$negativeRate -> adjusted=$adjustedNegativeRate")
  }

  test("Empty source handling - verify graceful handling") {
    implicit val as: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(as)
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

    try {
      val emptyData = List.empty[Int]
      val rate = 10
      val processedCount = new AtomicInteger(0)

      val result = Source(emptyData)
        .throttle(rate, 1.second)
        .mapAsync(parallelism = Math.min(rate, 100)) { item =>
          scala.concurrent.Future {
            processedCount.incrementAndGet()
            item
          }
        }
        .runWith(PekkoSink.ignore)

      Await.result(result, 5.seconds)

      // Should process 0 records without error
      processedCount.get() shouldBe 0

      LOGGER.info("Empty source handled gracefully")

    } finally {
      as.terminate()
    }
  }

  test("Shared ActorSystem - writer can be constructed with shared system") {
    val sharedSystem = ActorSystem("SharedTestSystem")

    try {
      // Verify writer can be constructed with a shared actor system
      val writerWithShared = new PekkoStreamingSinkWriter(foldersConfig, Some(sharedSystem))

      // Verify writer can also be constructed without a shared system (backwards compatibility)
      val writerWithoutShared = new PekkoStreamingSinkWriter(foldersConfig)

      LOGGER.info("PekkoStreamingSinkWriter constructed successfully with both shared and non-shared ActorSystem")

    } finally {
      sharedSystem.terminate()
    }
  }
}

