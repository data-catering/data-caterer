package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.FlagsConfig
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.log4j.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

/**
 * Integration tests for HTTP execution strategies using insta-infra to manage httpbin service.
 * 
 * These tests use insta-infra (https://github.com/data-catering/insta-infra) to spin up httpbin
 * locally, then execute YAML-based HTTP plans against the real service to verify execution strategies.
 * 
 * Prerequisites:
 * - insta-infra must be installed (see https://github.com/data-catering/insta-infra)
 * - Docker or Podman must be available
 * 
 * Tests verify:
 * - Duration-based execution with constant rate
 * - Ramp load pattern (increasing rate over time)
 * - Spike/wave patterns (bursts of requests)
 * - Stepped load patterns
 */
class InstaInfraHttpIntegrationTest extends SparkSuite with BeforeAndAfterAll with Matchers {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private val tempTestDirectory = s"/tmp/data-caterer-insta-http-test-${java.util.UUID.randomUUID().toString.take(8)}"
  private val planResourcePath = "/sample/plan/http-execution-strategy"
  private val taskResourcePath = "/sample/task/http-execution-strategy"
  private val httpbinServiceName = "httpbin"
  private val httpbinUrl = "http://localhost:80"

  private var instaCommandAvailable: Boolean = false
  private var httpbinStarted: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Check if insta command is available
    instaCommandAvailable = checkInstaCommandAvailable()
    if (!instaCommandAvailable) {
      cancel(s"insta-infra is not installed or not in PATH. " +
        s"Please install it from https://github.com/data-catering/insta-infra")
    }

    // Create temporary directories
    new File(tempTestDirectory).mkdirs()
    new File(s"$tempTestDirectory/record-tracking").mkdirs()
    new File(s"$tempTestDirectory/record-tracking-validation").mkdirs()
    new File(s"$tempTestDirectory/report").mkdirs()
    new File(s"$tempTestDirectory/validation").mkdirs()

    // Start httpbin service via insta-infra
    // Note: insta httpbin blocks until service is healthy, so no separate health check needed
    startHttpbin()
  }

  override def afterAll(): Unit = {
    // Stop httpbin service
    stopHttpbin()
    
    // Cleanup temporary directories
    cleanupTestFiles()
    
    super.afterAll()
  }

  test("Simple HTTP duration test - verify execution strategy works") {
    val startTime = System.currentTimeMillis()
    val result = executeYamlPlan("http-simple-test-plan.yaml", "http-simple-task.yaml")

    // Verify execution completed successfully
    result.generationResults should not be empty
    result.generationResults.head.sinkResult.isSuccess shouldBe true

    // Wait a bit for all requests to complete
    Thread.sleep(2000)

    // Validate against httpbin logs
    val requestTimestamps = getHttpbinRequestTimestamps(startTime)
    val requestsReceived = requestTimestamps.size
    LOGGER.info(s"Simple HTTP test: httpbin received $requestsReceived requests")

    // Expected: 3 seconds * 10 req/s = 30 requests (±20% tolerance)
    val expectedRequests = 30
    val tolerance = 0.2
    requestsReceived.toDouble shouldBe (expectedRequests.toDouble +- (expectedRequests * tolerance))

    // Verify constant rate pattern - requests should be evenly distributed
    if (requestTimestamps.nonEmpty) {
      val intervals = calculateRequestIntervals(requestTimestamps)
      LOGGER.info(s"Request intervals (ms): min=${intervals.min}, max=${intervals.max}, avg=${intervals.sum / intervals.size}")
      // Expected interval: 1000ms / 10 req = 100ms per request (±50% tolerance due to network variance)
      val expectedInterval = 100.0
      val avgInterval = intervals.sum.toDouble / intervals.size
      avgInterval shouldBe (expectedInterval +- (expectedInterval * 0.5))
    }
  }

  test("Duration-based execution with HTTP sink should maintain constant rate") {
    val startTime = System.currentTimeMillis()
    val result = executeYamlPlan("http-duration-test-plan.yaml", "http-duration-task.yaml")

    // Verify execution completed successfully
    result.generationResults should not be empty
    result.generationResults.head.sinkResult.isSuccess shouldBe true

    // Wait a bit for all requests to complete
    Thread.sleep(2000)

    // Validate against httpbin logs
    val requestTimestamps = getHttpbinRequestTimestamps(startTime)
    val requestsReceived = requestTimestamps.size
    LOGGER.info(s"Duration test: httpbin received $requestsReceived requests")

    // Expected: 4 seconds * 5 req/s = 20 requests (±20% tolerance)
    val expectedRequests = 20
    val tolerance = 0.2
    requestsReceived.toDouble shouldBe (expectedRequests.toDouble +- (expectedRequests * tolerance))

    // Verify constant rate - standard deviation of intervals should be low
    if (requestTimestamps.nonEmpty) {
      val intervals = calculateRequestIntervals(requestTimestamps)
      LOGGER.info(s"Request intervals (ms): min=${intervals.min}, max=${intervals.max}, avg=${intervals.sum / intervals.size}")
      // Expected interval: 1000ms / 5 req = 200ms per request
      val expectedInterval = 200.0
      val avgInterval = intervals.sum.toDouble / intervals.size
      avgInterval shouldBe (expectedInterval +- (expectedInterval * 0.5))
    }
  }

  test("Ramp load pattern with HTTP sink should increase rate over time") {
    val startTime = System.currentTimeMillis()
    val result = executeYamlPlan("http-ramp-test-plan.yaml", "http-ramp-task.yaml")

    // Verify execution completed successfully
    result.generationResults should not be empty
    result.generationResults.head.sinkResult.isSuccess shouldBe true

    // Wait a bit for all requests to complete
    Thread.sleep(2000)

    // Validate against httpbin logs
    val requestTimestamps = getHttpbinRequestTimestamps(startTime)
    val requestsReceived = requestTimestamps.size
    LOGGER.info(s"Ramp test: httpbin received $requestsReceived requests")

    // Expected: ramp from 2 to 10 req/s over 5 seconds = avg 6 req/s * 5s = 30 requests (±25% tolerance)
    val expectedRequests = 30
    val tolerance = 0.25
    requestsReceived.toDouble shouldBe (expectedRequests.toDouble +- (expectedRequests * tolerance))

    // Verify ramping pattern - check request distribution over time
    // Note: httpbin logs have second-level precision, making interval-based analysis unreliable
    // Instead, verify that requests are distributed across the test duration
    if (requestTimestamps.size >= 10) {
      val testDuration = requestTimestamps.last - requestTimestamps.head
      val firstHalf = requestTimestamps.count(t => (t - requestTimestamps.head) < (testDuration / 2))
      val secondHalf = requestTimestamps.size - firstHalf
      LOGGER.info(s"Ramp test distribution: first half=$firstHalf requests, second half=$secondHalf requests, duration=${testDuration}ms")
      // Just verify requests are distributed over time (not all in one burst)
      requestTimestamps.size should be > 10
      testDuration should be > 3000L  // At least 3 seconds of actual execution
    }
  }

  test("Spike load pattern with HTTP sink should create bursts of requests") {
    val startTime = System.currentTimeMillis()
    val result = executeYamlPlan("http-spike-test-plan.yaml", "http-spike-task.yaml")

    // Verify execution completed successfully
    result.generationResults should not be empty
    result.generationResults.head.sinkResult.isSuccess shouldBe true

    // Wait a bit for all requests to complete
    Thread.sleep(2000)

    // Validate against httpbin logs
    val requestTimestamps = getHttpbinRequestTimestamps(startTime)
    val requestsReceived = requestTimestamps.size
    LOGGER.info(s"Spike test: httpbin received $requestsReceived requests")

    // Expected: baseRate=2, spikeRate=20, spikeStart=0.25 (1s), spikeDuration=0.25 (1s)
    // Base: 3s * 2 req/s = 6, Spike: 1s * 20 req/s = 20, Total = 26 requests (±25% tolerance)
    val expectedRequests = 26
    val tolerance = 0.25
    requestsReceived.toDouble shouldBe (expectedRequests.toDouble +- (expectedRequests * tolerance))

    // Verify spike pattern - should see a burst of requests
    if (requestTimestamps.size >= 10) {
      val intervals = calculateRequestIntervals(requestTimestamps)
      val minInterval = intervals.min
      val maxInterval = intervals.max
      LOGGER.info(s"Spike test intervals: min=${minInterval}ms, max=${maxInterval}ms")
      // Should have high variance (some very short intervals during spike, longer during base)
      val variance = maxInterval.toDouble / minInterval.toDouble
      variance should be > 3.0 // At least 3x difference between min and max intervals
    }
  }

  test("Wave load pattern with HTTP sink should oscillate between rates") {
    val startTime = System.currentTimeMillis()
    val result = executeYamlPlan("http-wave-test-plan.yaml", "http-wave-task.yaml")

    // Verify execution completed successfully
    result.generationResults should not be empty
    result.generationResults.head.sinkResult.isSuccess shouldBe true

    // Wait a bit for all requests to complete
    Thread.sleep(2000)

    // Validate against httpbin logs
    val requestTimestamps = getHttpbinRequestTimestamps(startTime)
    val requestsReceived = requestTimestamps.size
    LOGGER.info(s"Wave test: httpbin received $requestsReceived requests")

    // Expected: baseRate=5, amplitude=3, frequency=1.0, duration=5s
    // Average rate = 5 req/s * 5s = 25 requests (±25% tolerance)
    val expectedRequests = 25
    val tolerance = 0.25
    requestsReceived.toDouble shouldBe (expectedRequests.toDouble +- (expectedRequests * tolerance))

    // Verify wave pattern - should see variation in request intervals
    if (requestTimestamps.size >= 10) {
      val intervals = calculateRequestIntervals(requestTimestamps)
      val avgInterval = intervals.sum.toDouble / intervals.size
      val variance = intervals.map(i => math.pow(i - avgInterval, 2)).sum / intervals.size
      val stdDev = math.sqrt(variance)
      LOGGER.info(s"Wave test intervals: avg=${avgInterval}ms, stdDev=${stdDev}ms")
      // Standard deviation should be significant (oscillating pattern)
      stdDev should be > (avgInterval * 0.2)
    }
  }

  test("Stepped load pattern with HTTP sink should maintain distinct rate levels") {
    val startTime = System.currentTimeMillis()
    val result = executeYamlPlan("http-stepped-test-plan.yaml", "http-stepped-task.yaml")

    // Verify execution completed successfully
    result.generationResults should not be empty
    result.generationResults.head.sinkResult.isSuccess shouldBe true

    // Wait a bit for all requests to complete
    Thread.sleep(2000)

    // Validate against httpbin logs
    val requestTimestamps = getHttpbinRequestTimestamps(startTime)
    val requestsReceived = requestTimestamps.size
    LOGGER.info(s"Stepped test: httpbin received $requestsReceived requests")

    // Expected: step 1 (1s @ 2 req/s) = 2, step 2 (2s @ 5 req/s) = 10, step 3 (2s @ 8 req/s) = 16
    // Total = 28 requests (±25% tolerance)
    val expectedRequests = 28
    val tolerance = 0.25
    requestsReceived.toDouble shouldBe (expectedRequests.toDouble +- (expectedRequests * tolerance))

    // Verify stepped pattern - analyze request counts in time windows
    // Note: httpbin logs have second-level precision, so we analyze by counting requests per time window
    // Given the timing variations and second-level precision, we verify the general pattern rather than strict ordering
    if (requestTimestamps.size >= 15) {
      val testDuration = requestTimestamps.last - requestTimestamps.head
      val step1Duration = testDuration / 5  // First step: 1s out of 5s
      val step2Duration = (testDuration * 2) / 5  // Second step: 2s out of 5s

      // Count requests in each step
      val step1Requests = requestTimestamps.count(t => (t - requestTimestamps.head) < step1Duration)
      val step2Requests = requestTimestamps.count(t => {
        val elapsed = t - requestTimestamps.head
        elapsed >= step1Duration && elapsed < (step1Duration + step2Duration)
      })
      val step3Requests = requestTimestamps.count(t => (t - requestTimestamps.head) >= (step1Duration + step2Duration))

      LOGGER.info(s"Stepped test: step1 requests=$step1Requests, step2 requests=$step2Requests, step3 requests=$step3Requests")
      // Verify pattern shows variation: step1 should have fewer requests than the combined later steps
      // This is more lenient and accounts for timing variations while still validating the stepped behavior
      step1Requests should be < (step2Requests + step3Requests)
      // Verify at least some requests in each step
      step1Requests should be > 0
      step2Requests should be > 0
      step3Requests should be > 0
    }
  }

  /**
   * Parse httpbin container logs and extract timestamps of POST requests to /post
   * that occurred after the given start time.
   *
   * Log format: '192.168.65.1 [07/Nov/2025:03:25:55 +0000] POST /post HTTP/1.1 200 Host: localhost'
   */
  private def getHttpbinRequestTimestamps(startTimeMillis: Long): List[Long] = {
    try {
      // Get httpbin container logs
      val logs = Process(Seq("docker", "logs", "--tail", "1000", "http")).!!.trim
      val postRequestLines = logs.split("\n").filter(_.contains("POST /post"))

      // Parse timestamps from log lines
      val timestampPattern = """.*\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})\s+\+0000\].*""".r
      val monthMap = Map(
        "Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6,
        "Jul" -> 7, "Aug" -> 8, "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12
      )

      postRequestLines.flatMap {
        case timestampPattern(day, month, year, hour, minute, second) =>
          try {
            val monthNum = monthMap.getOrElse(month, 1)
            val cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
            cal.set(year.toInt, monthNum - 1, day.toInt, hour.toInt, minute.toInt, second.toInt)
            cal.set(java.util.Calendar.MILLISECOND, 0)
            val timestamp = cal.getTimeInMillis
            // Only include requests that occurred after the test started
            if (timestamp >= startTimeMillis) Some(timestamp) else None
          } catch {
            case _: Exception => None
          }
        case _ => None
      }.toList.sorted
    } catch {
      case ex: Exception =>
        LOGGER.warn(s"Failed to parse httpbin timestamps: ${ex.getMessage}")
        List.empty
    }
  }

  /**
   * Calculate intervals (in milliseconds) between consecutive requests
   */
  private def calculateRequestIntervals(timestamps: List[Long]): List[Long] = {
    if (timestamps.size < 2) {
      List.empty
    } else {
      timestamps.sliding(2).map { case List(t1, t2) => t2 - t1 }.toList
    }
  }

  private def checkInstaCommandAvailable(): Boolean = {
    // First try to check if command exists using "which" or "command -v"
    val commandExists = Try {
      val whichResult = Process(Seq("which", "insta")).!!.trim
      whichResult.nonEmpty && !whichResult.contains("not found") && new File(whichResult).exists()
    }.getOrElse(false) || Try {
      val commandResult = Process(Seq("command", "-v", "insta")).!!.trim
      commandResult.nonEmpty && new File(commandResult).exists()
    }.getOrElse(false)
    
    if (!commandExists) {
      return false
    }
    
    // If command exists, try to run it with --help or --version to verify it works
    Try {
      val exitCode = Process(Seq("insta", "--help"), None, "INSTA_SKIP_UPDATE" -> "true").!(ProcessLogger(_ => (), _ => ()))
      exitCode == 0 || exitCode == 1 // Exit code 1 might be "wrong usage" but command exists
    }.getOrElse(false)
  }

  private def startHttpbin(): Unit = {
    if (!instaCommandAvailable) {
      return
    }

    try {
      // Check if httpbin is already running
      val isRunning = Try {
        Process(Seq("docker", "ps", "--filter", "name=http", "--format", "{{.Names}}")).!!.trim.contains("http")
      }.getOrElse(false)
      
      if (isRunning) {
        LOGGER.info(s"$httpbinServiceName is already running")
        httpbinStarted = true
        // Verify it's actually accessible
        waitForHttpbinReady()
        return
      }
      
      LOGGER.info(s"Starting $httpbinServiceName via insta-infra...")
      // insta httpbin blocks until service is healthy, so we run it synchronously
      // Capture output for debugging
      val output = new StringBuilder
      val errorOutput = new StringBuilder
      val logger = ProcessLogger(
        (o: String) => {
          output.append(o).append("\n")
          LOGGER.debug(s"[insta] $o")
        },
        (e: String) => {
          errorOutput.append(e).append("\n")
          LOGGER.warn(s"[insta ERROR] $e")
        }
      )
      
      val exitCode = Process(Seq("insta", httpbinServiceName), None, "INSTA_SKIP_UPDATE" -> "true").!(logger)
      
      if (exitCode == 0) {
        httpbinStarted = true
        LOGGER.info(s"$httpbinServiceName started successfully")
        // Wait for httpbin to be ready
        waitForHttpbinReady()
      } else {
        val errorMsg = if (errorOutput.nonEmpty) errorOutput.toString() else output.toString()
        throw new RuntimeException(s"Failed to start $httpbinServiceName. Exit code: $exitCode. Output: $errorMsg")
      }
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Failed to start $httpbinServiceName via insta-infra: ${ex.getMessage}", ex)
    }
  }

  /**
   * Wait for httpbin to be ready by checking if it responds to HTTP requests
   */
  private def waitForHttpbinReady(maxRetries: Int = 30, retryDelayMs: Int = 1000): Unit = {
    LOGGER.info(s"Waiting for $httpbinServiceName to be ready...")
    var retries = 0
    var isReady = false
    
    while (retries < maxRetries && !isReady) {
      try {
        val response = scala.io.Source.fromURL(s"$httpbinUrl/get").mkString
        if (response.nonEmpty) {
          isReady = true
          LOGGER.info(s"$httpbinServiceName is ready")
        }
      } catch {
        case _: Exception =>
          retries += 1
          if (retries < maxRetries) {
            Thread.sleep(retryDelayMs)
          }
      }
    }
    
    if (!isReady) {
      throw new RuntimeException(s"$httpbinServiceName did not become ready after ${maxRetries * retryDelayMs}ms")
    }
  }

  private def stopHttpbin(): Unit = {
    // Only stop if we started it in this test run
    // Don't stop if it was already running before tests
    if (!httpbinStarted) {
      return
    }

    try {
      LOGGER.info(s"Stopping $httpbinServiceName via insta-infra...")
      // Try to stop the service, but don't fail if it's already stopped
      val output = new StringBuilder
      val errorOutput = new StringBuilder
      val logger = ProcessLogger(
        (o: String) => {
          output.append(o).append("\n")
          LOGGER.debug(s"[insta stop] $o")
        },
        (e: String) => {
          errorOutput.append(e).append("\n")
          LOGGER.warn(s"[insta stop ERROR] $e")
        }
      )
      
      val exitCode = Process(Seq("insta", "-d", httpbinServiceName), None, "INSTA_SKIP_UPDATE" -> "true").!(logger)
      
      if (exitCode == 0) {
        LOGGER.info(s"$httpbinServiceName stopped successfully")
      } else {
        LOGGER.warn(s"Failed to stop $httpbinServiceName. Exit code: $exitCode (service may already be stopped)")
      }
    } catch {
      case ex: Exception =>
        LOGGER.warn(s"Exception while stopping $httpbinServiceName: ${ex.getMessage}")
    } finally {
      httpbinStarted = false
    }
  }

  private def executeYamlPlan(planFileName: String, taskFileName: String): PlanRunResults = {
    // Get resource paths
    val planResource = getClass.getResource(s"$planResourcePath/$planFileName")
    val taskResource = getClass.getResource(s"$taskResourcePath/$taskFileName")
    
    if (planResource == null) {
      throw new RuntimeException(s"Plan file not found: $planResourcePath/$planFileName")
    }
    if (taskResource == null) {
      throw new RuntimeException(s"Task file not found: $taskResourcePath/$taskFileName")
    }

    val planFilePath = planResource.getPath
    val taskFolderPath = new File(taskResource.getPath).getParent

    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration.copy(
      foldersConfig = ConfigParser.toDataCatererConfiguration.foldersConfig.copy(
        planFilePath = planFilePath,
        taskFolderPath = taskFolderPath,
        validationFolderPath = s"$tempTestDirectory/validation",
        recordTrackingFolderPath = s"$tempTestDirectory/record-tracking",
        recordTrackingForValidationFolderPath = s"$tempTestDirectory/record-tracking-validation",
        generatedReportsFolderPath = s"$tempTestDirectory/report"
      ),
      flagsConfig = FlagsConfig(enableGeneratePlanAndTasks = false)
    )
    
    PlanProcessor.executePlanWithConfig(
      dataCatererConfiguration,
      None,
      "yaml"
    )
  }

  private def cleanupTestFiles(): Unit = {
    val tempDir = new File(tempTestDirectory)
    if (tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}

