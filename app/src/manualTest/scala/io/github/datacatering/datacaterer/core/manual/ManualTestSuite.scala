package io.github.datacatering.datacaterer.core.manual

import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Canceled}

import java.io.File
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

/**
 * Base trait for manual integration tests that require external dependencies.
 *
 * These tests are NOT run as part of regular test suites. They must be run explicitly:
 * {{{
 * ./gradlew :app:manualTest --tests "io.github.datacatering.datacaterer.core.manual.KafkaStreamingManualTest"
 * }}}
 *
 * For running arbitrary YAML files:
 * {{{
 * YAML_FILE=/path/to/config.yaml ./gradlew :app:manualTest --tests "*YamlFileManualTest"
 * }}}
 */
trait ManualTestSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {

  protected val LOGGER: Logger = Logger.getLogger(getClass.getName)

  protected val tempTestDirectory: String = s"/tmp/data-caterer-manual-test-${java.util.UUID.randomUUID().toString.take(8)}"

  implicit lazy val sparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("manual-integration-tests")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession
    createTempDirectories()
  }

  override protected def afterAll(): Unit = {
    cleanupTempDirectories()
    sparkSession.close()
    super.afterAll()
  }

  override protected def afterEach(): Unit = {
    sparkSession.catalog.clearCache()
    super.afterEach()
  }

  protected def createTempDirectories(): Unit = {
    new File(tempTestDirectory).mkdirs()
    new File(s"$tempTestDirectory/record-tracking").mkdirs()
    new File(s"$tempTestDirectory/record-tracking-validation").mkdirs()
    new File(s"$tempTestDirectory/report").mkdirs()
    new File(s"$tempTestDirectory/validation").mkdirs()
    new File(s"$tempTestDirectory/output").mkdirs()
  }

  protected def cleanupTempDirectories(): Unit = {
    val tempDir = new File(tempTestDirectory)
    if (tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  /**
   * Execute a unified YAML configuration file.
   *
   * @param yamlFilePath Absolute path to the YAML file
   * @return PlanRunResults from the execution
   */
  protected def executeUnifiedYaml(yamlFilePath: String): PlanRunResults = {
    LOGGER.info(s"Executing unified YAML: $yamlFilePath")
    PlanProcessor.executeFromUnifiedYaml(yamlFilePath)
  }

  /**
   * Execute a unified YAML from classpath resources.
   *
   * @param resourcePath Resource path (e.g., "/kafka-streaming.yaml")
   * @return PlanRunResults from the execution
   */
  protected def executeUnifiedYamlResource(resourcePath: String): PlanRunResults = {
    val resource = getClass.getResource(resourcePath)
    if (resource == null) {
      throw new RuntimeException(s"Resource not found: $resourcePath")
    }
    executeUnifiedYaml(resource.getPath)
  }

  /**
   * Check if insta-infra CLI is available for managing external services.
   */
  protected def checkInstaAvailable(): Boolean = {
    Try {
      val whichResult = Process(Seq("which", "insta")).!!.trim
      whichResult.nonEmpty && !whichResult.contains("not found") && new File(whichResult).exists()
    }.getOrElse(false)
  }

  /**
   * Start a service using insta-infra.
   *
   * @param serviceName Name of the service (e.g., "kafka", "postgres", "httpbin")
   * @return true if service started successfully
   */
  protected def startService(serviceName: String): Boolean = {
    if (!checkInstaAvailable()) {
      LOGGER.warn(s"insta-infra not available, cannot start $serviceName")
      return false
    }

    try {
      // Check if already running
      val isRunning = Try {
        Process(Seq("docker", "ps", "--filter", s"name=$serviceName", "--format", "{{.Names}}")).!!.trim.contains(serviceName)
      }.getOrElse(false)

      if (isRunning) {
        LOGGER.info(s"$serviceName is already running")
        return true
      }

      LOGGER.info(s"Starting $serviceName via insta-infra...")
      val output = new StringBuilder
      val logger = ProcessLogger(
        (o: String) => { output.append(o).append("\n"); LOGGER.debug(s"[insta] $o") },
        (e: String) => { output.append(e).append("\n"); LOGGER.warn(s"[insta ERROR] $e") }
      )

      val exitCode = Process(Seq("insta", serviceName), None, "INSTA_SKIP_UPDATE" -> "true").!(logger)
      if (exitCode == 0) {
        LOGGER.info(s"$serviceName started successfully")
        true
      } else {
        LOGGER.error(s"Failed to start $serviceName. Exit code: $exitCode. Output: ${output.toString()}")
        false
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Exception starting $serviceName: ${ex.getMessage}")
        false
    }
  }

  /**
   * Stop a service using insta-infra.
   */
  protected def stopService(serviceName: String): Unit = {
    if (!checkInstaAvailable()) return

    try {
      LOGGER.info(s"Stopping $serviceName via insta-infra...")
      val logger = ProcessLogger(_ => (), _ => ())
      Process(Seq("insta", "-d", serviceName), None, "INSTA_SKIP_UPDATE" -> "true").!(logger)
      LOGGER.info(s"$serviceName stopped")
    } catch {
      case ex: Exception =>
        LOGGER.warn(s"Exception stopping $serviceName: ${ex.getMessage}")
    }
  }

  /**
   * Skip test if a required service is not available.
   */
  protected def requireService(serviceName: String): Unit = {
    if (!startService(serviceName)) {
      cancel(s"Required service '$serviceName' is not available. Install insta-infra or start $serviceName manually.")
    }
  }

  /**
   * Skip test if insta-infra is not available.
   */
  protected def requireInstaInfra(): Unit = {
    if (!checkInstaAvailable()) {
      cancel("insta-infra is not installed. Install from https://github.com/data-catering/insta-infra")
    }
  }

  /**
   * Wait for a service to be ready by checking HTTP endpoint.
   */
  protected def waitForHttpService(url: String, maxRetries: Int = 30, retryDelayMs: Int = 1000): Boolean = {
    LOGGER.info(s"Waiting for HTTP service at $url...")
    var retries = 0
    while (retries < maxRetries) {
      try {
        val response = scala.io.Source.fromURL(url).mkString
        if (response.nonEmpty) {
          LOGGER.info(s"Service at $url is ready")
          return true
        }
      } catch {
        case _: Exception =>
          retries += 1
          if (retries < maxRetries) Thread.sleep(retryDelayMs)
      }
    }
    LOGGER.error(s"Service at $url did not become ready after ${maxRetries * retryDelayMs}ms")
    false
  }

  /**
   * Wait for a TCP port to be available.
   */
  protected def waitForPort(host: String, port: Int, maxRetries: Int = 30, retryDelayMs: Int = 1000): Boolean = {
    LOGGER.info(s"Waiting for $host:$port to be available...")
    var retries = 0
    while (retries < maxRetries) {
      try {
        val socket = new java.net.Socket(host, port)
        socket.close()
        LOGGER.info(s"$host:$port is available")
        return true
      } catch {
        case _: Exception =>
          retries += 1
          if (retries < maxRetries) Thread.sleep(retryDelayMs)
      }
    }
    LOGGER.error(s"$host:$port did not become available after ${maxRetries * retryDelayMs}ms")
    false
  }
}
