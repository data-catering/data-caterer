package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{Connection, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite

class ConnectionDeserializerTest extends AnyFunSuite {

  private val objectMapper = ObjectMapperUtil.yamlObjectMapper

  test("Can deserialize Connection object directly") {
    val yamlContent =
      """type: postgres
        |options:
        |  url: "jdbc:postgresql://localhost:5432/db"
        |  user: "postgres"
        |  password: "secret"
        |""".stripMargin

    val connection = objectMapper.readValue(yamlContent, classOf[Connection])

    assert(connection.`type` == "postgres")
    assert(connection.options("url") == "jdbc:postgresql://localhost:5432/db")
    assert(connection.options("user") == "postgres")
    assert(connection.options("password") == "secret")
  }

  test("Can deserialize Connection with name") {
    val yamlContent =
      """name: my_db
        |type: postgres
        |options:
        |  url: "jdbc:postgresql://localhost:5432/db"
        |""".stripMargin

    val connection = objectMapper.readValue(yamlContent, classOf[Connection])

    assert(connection.name.contains("my_db"))
    assert(connection.`type` == "postgres")
  }

  test("Can deserialize TaskSummary with connection as string") {
    val yamlContent =
      """name: "test_task"
        |connection: "my_database"
        |""".stripMargin

    val taskSummary = objectMapper.readValue(yamlContent, classOf[TaskSummary])

    assert(taskSummary.name == "test_task")
    assert(taskSummary.connection.isDefined)

    taskSummary.connection.get match {
      case Left(connName) =>
        assert(connName == "my_database")
      case Right(conn) =>
        fail(s"Expected Left (string reference), got Right (Connection object): ${conn.`type`}")
    }
  }

  test("Can deserialize TaskSummary with inline connection object") {
    val yamlContent =
      """name: "test_task"
        |connection:
        |  type: csv
        |  options:
        |    path: "/tmp/test"
        |    header: "true"
        |""".stripMargin

    try {
      val taskSummary = objectMapper.readValue(yamlContent, classOf[TaskSummary])

      assert(taskSummary.name == "test_task")
      assert(taskSummary.connection.isDefined)

      taskSummary.connection.get match {
        case Right(conn) =>
          assert(conn.`type` == "csv")
          assert(conn.options("path") == "/tmp/test")
          assert(conn.options("header") == "true")
        case Left(connName) =>
          fail(s"Expected Right (Connection object), got Left (string reference): $connName")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  test("Can deserialize TaskSummary with no connection field") {
    val yamlContent =
      """name: "test_task"
        |""".stripMargin

    val taskSummary = objectMapper.readValue(yamlContent, classOf[TaskSummary])

    assert(taskSummary.name == "test_task")
    assert(taskSummary.connection.isEmpty)
  }

  test("Connection with empty type string") {
    val yamlContent =
      """name: test_conn
        |type: ""
        |options:
        |  key: "value"
        |""".stripMargin

    val connection = objectMapper.readValue(yamlContent, classOf[Connection])

    assert(connection.name.contains("test_conn"))
    assert(connection.`type` == "")
  }

  test("Connection with minimal fields") {
    val yamlContent =
      """type: csv
        |""".stripMargin

    val connection = objectMapper.readValue(yamlContent, classOf[Connection])

    assert(connection.`type` == "csv")
    assert(connection.name.isEmpty)
    assert(connection.options.isEmpty)
  }

  test("Debug: Parse actual failing YAML from integration test") {
    val yamlContent =
      """name: "task1"
        |
        |connection:
        |  type: csv
        |  options:
        |    path: "/tmp/test"
        |    header: "true"
        |
        |steps:
        |  - name: users
        |    type: csv
        |    count:
        |      records: 100
        |    fields:
        |      - name: user_id
        |        type: string
        |        options:
        |          regex: "USER[0-9]{6}"
        |""".stripMargin

    try {
      val taskSummary = objectMapper.readValue(yamlContent, classOf[TaskSummary])
      assert(taskSummary.connection.isDefined)

      taskSummary.connection.get match {
        case Right(conn) =>
          assert(conn.`type` == "csv")
          assert(conn.options("path") == "/tmp/test")
          assert(conn.options("header") == "true")
        case Left(_) =>
          fail(s"Expected Right (Connection object), got Left (string reference)")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }
}
