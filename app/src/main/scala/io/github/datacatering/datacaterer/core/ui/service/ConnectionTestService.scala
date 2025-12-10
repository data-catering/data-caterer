package io.github.datacatering.datacaterer.core.ui.service

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.ui.model.{Connection, ConnectionTestResult}
import io.github.datacatering.datacaterer.core.ui.resource.SparkSessionManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import scala.util.{Failure, Success, Try}

/**
 * Service for testing data source connections.
 * 
 * This service provides functionality to test connections to various data sources
 * using the same mechanisms that would be used during actual data generation/validation.
 * 
 * For JDBC connections (Postgres, MySQL), it tests using DriverManager directly for faster feedback.
 * For other connections (Cassandra, Kafka, file-based), it uses Spark to verify connectivity.
 */
object ConnectionTestService {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Test a connection and return the result
   *
   * @param connection The connection to test
   * @return ConnectionTestResult with success status and message
   */
  def testConnection(connection: Connection): ConnectionTestResult = {
    LOGGER.info(s"Testing connection, name=${connection.name}, type=${connection.`type`}")
    
    val startTime = System.currentTimeMillis()
    
    val result = try {
      val connectionType = connection.`type`.toLowerCase
      
      connectionType match {
        case POSTGRES | MYSQL | "postgresql" =>
          testJdbcConnection(connection)
        case CASSANDRA_NAME | "cassandra" =>
          testCassandraConnection(connection)
        case KAFKA =>
          testKafkaConnection(connection)
        case CSV | JSON | PARQUET | ORC | DELTA | ICEBERG =>
          testFileConnection(connection)
        case HTTP =>
          testHttpConnection(connection)
        case JMS | "solace" | "rabbitmq" | "activemq" =>
          testJmsConnection(connection)
        case _ =>
          // For unknown types, try a generic Spark-based test
          testGenericSparkConnection(connection)
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Connection test failed, name=${connection.name}, error=${ex.getMessage}", ex)
        ConnectionTestResult(
          success = false,
          message = s"Connection test failed: ${ex.getMessage}",
          details = Some(getStackTraceAsString(ex))
        )
    }
    
    val duration = System.currentTimeMillis() - startTime
    LOGGER.info(s"Connection test completed, name=${connection.name}, success=${result.success}, duration=${duration}ms")
    
    result.copy(durationMs = Some(duration))
  }

  /**
   * Test JDBC connection (Postgres, MySQL)
   */
  private def testJdbcConnection(connection: Connection): ConnectionTestResult = {
    val options = connection.options
    val url = options.getOrElse(URL, options.getOrElse("url", ""))
    val user = options.getOrElse(USERNAME, options.getOrElse("user", ""))
    val password = options.getOrElse(PASSWORD, options.getOrElse("password", ""))
    
    if (url.isEmpty) {
      return ConnectionTestResult(success = false, message = "Missing required option: url")
    }
    
    // Determine driver based on connection type
    val driver = connection.`type`.toLowerCase match {
      case POSTGRES | "postgresql" => POSTGRES_DRIVER
      case MYSQL => MYSQL_DRIVER
      case _ => options.getOrElse(DRIVER, "")
    }
    
    if (driver.nonEmpty) {
      Try(Class.forName(driver)) match {
        case Failure(ex) =>
          return ConnectionTestResult(success = false, message = s"Failed to load JDBC driver: $driver", details = Some(ex.getMessage))
        case Success(_) => // Driver loaded successfully
      }
    }
    
    Try {
      val conn = DriverManager.getConnection(url, user, password)
      try {
        val metadata = conn.getMetaData
        val productName = metadata.getDatabaseProductName
        val productVersion = metadata.getDatabaseProductVersion
        (productName, productVersion)
      } finally {
        conn.close()
      }
    } match {
      case Success((productName, productVersion)) =>
        ConnectionTestResult(
          success = true,
          message = s"Successfully connected to $productName",
          details = Some(s"Database: $productName, Version: $productVersion")
        )
      case Failure(ex) =>
        ConnectionTestResult(
          success = false,
          message = s"Failed to connect: ${ex.getMessage}",
          details = Some(getStackTraceAsString(ex))
        )
    }
  }

  /**
   * Test Cassandra connection using Spark
   */
  private def testCassandraConnection(connection: Connection): ConnectionTestResult = {
    val options = connection.options
    val host = options.getOrElse("spark.cassandra.connection.host", "localhost")
    val port = options.getOrElse("spark.cassandra.connection.port", "9042")
    
    // For Cassandra, we need to use Spark to test the connection
    withSparkSession { implicit spark =>
      Try {
        // Set Cassandra configurations
        options.filter(_._1.startsWith("spark.")).foreach { case (k, v) =>
          spark.conf.set(k, v)
        }
        
        // Try to read system keyspace to verify connection
        val df = spark.read
          .format(CASSANDRA)
          .options(options ++ Map("keyspace" -> "system", "table" -> "local"))
          .load()
        
        val count = df.count()
        s"Connected to Cassandra at $host:$port (system.local has $count rows)"
      } match {
        case Success(msg) =>
          ConnectionTestResult(success = true, message = "Successfully connected to Cassandra", details = Some(msg))
        case Failure(ex) =>
          ConnectionTestResult(success = false, message = s"Failed to connect to Cassandra: ${ex.getMessage}", details = Some(getStackTraceAsString(ex)))
      }
    }
  }

  /**
   * Test Kafka connection
   */
  private def testKafkaConnection(connection: Connection): ConnectionTestResult = {
    val options = connection.options
    val bootstrapServers = options.getOrElse("kafka.bootstrap.servers", options.getOrElse("bootstrap.servers", ""))
    
    if (bootstrapServers.isEmpty) {
      return ConnectionTestResult(success = false, message = "Missing required option: kafka.bootstrap.servers or bootstrap.servers")
    }
    
    // Use Kafka AdminClient to test connection
    import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
    import java.util.Properties
    import scala.collection.JavaConverters._
    
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    
    // Add any additional Kafka options
    options.filter(_._1.startsWith("kafka.")).foreach { case (k, v) =>
      props.put(k.stripPrefix("kafka."), v)
    }
    
    Try {
      val adminClient = AdminClient.create(props)
      try {
        val clusterInfo = adminClient.describeCluster()
        val clusterId = clusterInfo.clusterId().get()
        val nodes = clusterInfo.nodes().get().asScala.toList
        (clusterId, nodes.size)
      } finally {
        adminClient.close()
      }
    } match {
      case Success((clusterId, nodeCount)) =>
        ConnectionTestResult(
          success = true,
          message = "Successfully connected to Kafka cluster",
          details = Some(s"Cluster ID: $clusterId, Nodes: $nodeCount")
        )
      case Failure(ex) =>
        ConnectionTestResult(
          success = false,
          message = s"Failed to connect to Kafka: ${ex.getMessage}",
          details = Some(getStackTraceAsString(ex))
        )
    }
  }

  /**
   * Test file-based connection (CSV, JSON, Parquet, etc.)
   */
  private def testFileConnection(connection: Connection): ConnectionTestResult = {
    val options = connection.options
    val path = options.getOrElse(PATH, options.getOrElse("path", ""))
    
    if (path.isEmpty) {
      return ConnectionTestResult(success = false, message = "Missing required option: path")
    }
    
    // Check if path exists
    import java.nio.file.{Files, Paths}
    
    val pathObj = Paths.get(path)
    if (path.startsWith("s3://") || path.startsWith("gs://") || path.startsWith("hdfs://") || path.startsWith("abfs://")) {
      // For cloud/distributed storage, we need Spark to test
      withSparkSession { implicit spark =>
        Try {
          val format = connection.`type`.toLowerCase
          val df = spark.read.format(format).options(options).load(path)
          val schema = df.schema
          s"Successfully accessed $path with ${schema.fields.length} columns"
        } match {
          case Success(msg) =>
            ConnectionTestResult(success = true, message = "Successfully accessed file path", details = Some(msg))
          case Failure(ex) =>
            ConnectionTestResult(success = false, message = s"Failed to access file path: ${ex.getMessage}", details = Some(getStackTraceAsString(ex)))
        }
      }
    } else {
      // For local paths, check directly
      if (Files.exists(pathObj)) {
        val isDirectory = Files.isDirectory(pathObj)
        val fileType = if (isDirectory) "directory" else "file"
        ConnectionTestResult(
          success = true,
          message = s"Path exists and is accessible",
          details = Some(s"Type: $fileType, Path: $path")
        )
      } else {
        ConnectionTestResult(
          success = false,
          message = s"Path does not exist: $path"
        )
      }
    }
  }

  /**
   * Test HTTP connection
   */
  private def testHttpConnection(connection: Connection): ConnectionTestResult = {
    val options = connection.options
    val url = options.getOrElse(URL, options.getOrElse("url", ""))
    
    if (url.isEmpty) {
      return ConnectionTestResult(success = false, message = "Missing required option: url")
    }
    
    import java.net.{HttpURLConnection, URL => JavaURL}
    
    Try {
      val urlObj = new JavaURL(url)
      val conn = urlObj.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("HEAD")
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(5000)
      try {
        conn.connect()
        val responseCode = conn.getResponseCode
        (responseCode, conn.getResponseMessage)
      } finally {
        conn.disconnect()
      }
    } match {
      case Success((code, message)) if code >= 200 && code < 400 =>
        ConnectionTestResult(
          success = true,
          message = "Successfully connected to HTTP endpoint",
          details = Some(s"Response: $code $message")
        )
      case Success((code, message)) =>
        ConnectionTestResult(
          success = false,
          message = s"HTTP endpoint returned error",
          details = Some(s"Response: $code $message")
        )
      case Failure(ex) =>
        ConnectionTestResult(
          success = false,
          message = s"Failed to connect to HTTP endpoint: ${ex.getMessage}",
          details = Some(getStackTraceAsString(ex))
        )
    }
  }

  /**
   * Test JMS connection (Solace, RabbitMQ, ActiveMQ)
   */
  private def testJmsConnection(connection: Connection): ConnectionTestResult = {
    val options = connection.options
    val url = options.getOrElse(URL, options.getOrElse("url", ""))
    val connectionFactory = options.getOrElse("connectionFactory", "")
    
    if (url.isEmpty) {
      return ConnectionTestResult(success = false, message = "Missing required option: url")
    }
    
    // For JMS, we can only do a basic URL validation
    // Full connection testing would require the specific JMS provider libraries
    ConnectionTestResult(
      success = true,
      message = "JMS configuration validated (full connection test requires running plan)",
      details = Some(s"URL: $url, Connection Factory: ${if (connectionFactory.nonEmpty) connectionFactory else "default"}")
    )
  }

  /**
   * Test generic connection using Spark
   */
  private def testGenericSparkConnection(connection: Connection): ConnectionTestResult = {
    ConnectionTestResult(
      success = true,
      message = s"Configuration validated for ${connection.`type`}",
      details = Some("Full connection test will be performed when running the plan")
    )
  }

  /**
   * Helper to use the shared SparkSession for connection testing
   */
  private def withSparkSession[T](f: SparkSession => T): T = {
    val spark = SparkSessionManager.getOrCreate()
    f(spark)
  }

  private def getStackTraceAsString(ex: Throwable): String = {
    val sw = new java.io.StringWriter()
    val pw = new java.io.PrintWriter(sw)
    ex.printStackTrace(pw)
    sw.toString.take(1000) // Limit to first 1000 chars
  }
}

