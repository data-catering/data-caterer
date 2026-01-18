package io.github.datacatering.datacaterer.core.manual

import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.Try

/**
 * Manual integration test for PostgreSQL with Data Caterer.
 *
 * Prerequisites:
 * 1. Install insta-infra: https://github.com/data-catering/insta-infra
 * 2. Or start PostgreSQL manually
 *
 * Usage:
 * {{{
 * # Run with insta-infra (will auto-start PostgreSQL)
 * ./gradlew :app:manualTest --tests "*PostgresManualTest"
 *
 * # Run with custom PostgreSQL connection
 * POSTGRES_URL=jdbc:postgresql://myhost:5432/mydb POSTGRES_USER=user POSTGRES_PASSWORD=pass \
 *   ./gradlew :app:manualTest --tests "*PostgresManualTest"
 * }}}
 */
class PostgresManualTest extends ManualTestSuite with BeforeAndAfterAll {

  private val postgresUrl = Option(System.getenv("POSTGRES_URL")).getOrElse("jdbc:postgresql://localhost:5432/docker")
  private val postgresUser = Option(System.getenv("POSTGRES_USER")).getOrElse("docker")
  private val postgresPassword = Option(System.getenv("POSTGRES_PASSWORD")).getOrElse("docker")
  private val postgresServiceName = "postgres"
  private var postgresStartedByTest = false

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (!isPostgresRunning) {
      if (checkInstaAvailable()) {
        postgresStartedByTest = startService(postgresServiceName)
        if (!postgresStartedByTest) {
          cancel("Failed to start PostgreSQL via insta-infra")
        }
        // Wait for PostgreSQL to be ready
        if (!waitForPort("localhost", 5432, maxRetries = 30, retryDelayMs = 1000)) {
          cancel("PostgreSQL did not become ready in time")
        }
        // Additional wait for PostgreSQL to accept connections
        Thread.sleep(2000)
      } else {
        cancel("PostgreSQL is not running and insta-infra is not available")
      }
    }
  }

  override def afterAll(): Unit = {
    if (postgresStartedByTest) {
      stopService(postgresServiceName)
    }
    super.afterAll()
  }

  test("PostgreSQL generates data to tables") {
    val testYaml = createTestYaml()
    val yamlFile = Path.of(s"$tempTestDirectory/postgres-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val results = executeUnifiedYaml(yamlFile.toString)

    results should not be null
    results.generationResults should not be empty
    results.generationResults.head.sinkResult.isSuccess shouldBe true
    results.generationResults.head.sinkResult.count should be > 0L

    // Verify data was written to PostgreSQL
    val df = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.test_accounts")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", "org.postgresql.Driver")
      .load()

    df.count() shouldBe 50
  }

  test("PostgreSQL with foreign keys maintains referential integrity") {
    val testYaml = createForeignKeyTestYaml()
    val yamlFile = Path.of(s"$tempTestDirectory/postgres-fk-test.yaml")
    Files.writeString(yamlFile, testYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val results = executeUnifiedYaml(yamlFile.toString)

    results should not be null
    results.generationResults should have size 2

    // Verify parent table
    val parents = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.test_customers")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", "org.postgresql.Driver")
      .load()

    // Verify child table
    val children = sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("dbtable", "public.test_orders")
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", "org.postgresql.Driver")
      .load()

    parents.count() should be > 0L
    children.count() should be > 0L

    // Verify referential integrity - all child customer_ids should exist in parents
    val parentIds = parents.select("customer_id").collect().map(_.getString(0)).toSet
    val childFks = children.select("customer_id").collect().map(_.getString(0)).toSet

    childFks.foreach { fk =>
      parentIds should contain(fk)
    }
  }

  private def isPostgresRunning: Boolean = {
    Try {
      val socket = new java.net.Socket()
      socket.connect(new java.net.InetSocketAddress("localhost", 5432), 1000)
      socket.close()
      true
    }.getOrElse(false)
  }

  private def createTestYaml(): String = {
    s"""version: "1.0"
       |name: "postgres_test"
       |description: "Test PostgreSQL data generation"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |
       |dataSources:
       |  - name: "test_postgres"
       |    connection:
       |      type: "postgres"
       |      options:
       |        url: "$postgresUrl"
       |        user: "$postgresUser"
       |        password: "$postgresPassword"
       |    steps:
       |      - name: "test_accounts"
       |        options:
       |          dbtable: "public.test_accounts"
       |        count:
       |          records: 50
       |        fields:
       |          - name: "account_id"
       |            options:
       |              regex: "ACC[0-9]{8}"
       |          - name: "name"
       |            options:
       |              expression: "#{Name.fullName}"
       |          - name: "balance"
       |            type: "double"
       |            options:
       |              min: 0.0
       |              max: 10000.0
       |          - name: "created_at"
       |            type: "timestamp"
       |""".stripMargin
  }

  private def createForeignKeyTestYaml(): String = {
    s"""version: "1.0"
       |name: "postgres_fk_test"
       |description: "Test PostgreSQL with foreign keys"
       |
       |config:
       |  flags:
       |    enableGenerateData: true
       |    enableSaveReports: false
       |
       |dataSources:
       |  - name: "test_postgres"
       |    connection:
       |      type: "postgres"
       |      options:
       |        url: "$postgresUrl"
       |        user: "$postgresUser"
       |        password: "$postgresPassword"
       |    steps:
       |      - name: "test_customers"
       |        options:
       |          dbtable: "public.test_customers"
       |        count:
       |          records: 20
       |        fields:
       |          - name: "customer_id"
       |            options:
       |              regex: "CUST[0-9]{6}"
       |          - name: "name"
       |            options:
       |              expression: "#{Name.fullName}"
       |      - name: "test_orders"
       |        options:
       |          dbtable: "public.test_orders"
       |        count:
       |          records: 100
       |        fields:
       |          - name: "order_id"
       |            options:
       |              regex: "ORD[0-9]{8}"
       |          - name: "customer_id"
       |          - name: "amount"
       |            type: "double"
       |            options:
       |              min: 10.0
       |              max: 500.0
       |
       |foreignKeys:
       |  - source:
       |      dataSource: "test_postgres"
       |      step: "test_customers"
       |      fields: ["customer_id"]
       |    generate:
       |      - dataSource: "test_postgres"
       |        step: "test_orders"
       |        fields: ["customer_id"]
       |        cardinality:
       |          min: 1
       |          max: 10
       |""".stripMargin
  }
}
