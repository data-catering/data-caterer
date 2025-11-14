package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{Connection, Plan, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite

class ConnectionResolverTest extends AnyFunSuite {

  test("buildConnectionMap from plan with connections") {
    val connections = List(
      Connection(Some("db1"), "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db1")),
      Connection(Some("db2"), "mysql", Map("url" -> "jdbc:mysql://localhost:3306/db2"))
    )
    val plan = Plan(connections = Some(connections))

    val connectionMap = ConnectionResolver.buildConnectionMap(plan)

    assert(connectionMap.size == 2)
    assert(connectionMap.contains("db1"))
    assert(connectionMap.contains("db2"))
    assert(connectionMap("db1").`type` == "postgres")
    assert(connectionMap("db2").`type` == "mysql")
  }

  test("buildConnectionMap from plan without connections") {
    val plan = Plan(connections = None)

    val connectionMap = ConnectionResolver.buildConnectionMap(plan)

    assert(connectionMap.isEmpty)
  }

  test("buildConnectionMap ignores connections without names") {
    val connections = List(
      Connection(Some("db1"), "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db1")),
      Connection(None, "mysql", Map("url" -> "jdbc:mysql://localhost:3306/db2")) // No name!
    )
    val plan = Plan(connections = Some(connections))

    val connectionMap = ConnectionResolver.buildConnectionMap(plan)

    assert(connectionMap.size == 1)
    assert(connectionMap.contains("db1"))
    assert(!connectionMap.contains(""))
  }

  test("resolveTaskConnection with inline connection") {
    val inlineConnection = Connection(None, "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db"))
    val taskSummary = TaskSummary(name = "test_task", connection = Some(Right(inlineConnection)))

    val result = ConnectionResolver.resolveTaskConnection(
      taskSummary,
      Map.empty,
      Map.empty
    )

    assert(result.isDefined)
    val (connectionMap, isFromAppConf) = result.get
    assert(connectionMap("format") == "postgres")
    assert(connectionMap("url") == "jdbc:postgresql://localhost:5432/db")
    assert(!isFromAppConf)
  }

  test("resolveTaskConnection with connection reference from plan") {
    val planConnections = Map(
      "my_db" -> Connection(Some("my_db"), "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db"))
    )
    val taskSummary = TaskSummary(name = "test_task", connection = Some(Left("my_db")))

    val result = ConnectionResolver.resolveTaskConnection(
      taskSummary,
      planConnections,
      Map.empty
    )

    assert(result.isDefined)
    val (connectionMap, isFromAppConf) = result.get
    assert(connectionMap("format") == "postgres")
    assert(!isFromAppConf)
  }

  test("resolveTaskConnection fallback to application.conf") {
    val appConfConnections = Map(
      "legacy_db" -> Map("format" -> "postgres", "url" -> "jdbc:postgresql://localhost:5432/legacy")
    )
    val taskSummary = TaskSummary(name = "test_task", connection = Some(Left("legacy_db")))

    val result = ConnectionResolver.resolveTaskConnection(
      taskSummary,
      Map.empty,
      appConfConnections
    )

    assert(result.isDefined)
    val (connectionMap, isFromAppConf) = result.get
    assert(connectionMap("format") == "postgres")
    assert(isFromAppConf) // Should indicate fallback
  }

  test("resolveTaskConnection with dataSourceName when task has no connection") {
    val appConfConnections = Map(
      "my_source" -> Map("format" -> "csv", "path" -> "/tmp/data")
    )
    val taskSummary = TaskSummary(name = "test_task", dataSourceName = "my_source", connection = None)

    val result = ConnectionResolver.resolveTaskConnection(
      taskSummary,
      Map.empty,
      appConfConnections
    )

    assert(result.isDefined)
    val (connectionMap, isFromAppConf) = result.get
    assert(connectionMap("format") == "csv")
    assert(isFromAppConf)
  }

  test("resolveTaskConnection returns None when connection not found") {
    val taskSummary = TaskSummary(name = "test_task", connection = Some(Left("nonexistent")))

    val result = ConnectionResolver.resolveTaskConnection(
      taskSummary,
      Map.empty,
      Map.empty
    )

    assert(result.isEmpty)
  }

  test("interpolateConnection with environment variables") {
    // Set up a connection with env var placeholders
    val connection = Connection(
      Some("db"),
      "postgres",
      Map(
        "url" -> "jdbc:postgresql://${DB_HOST:-localhost}:5432/db",
        "user" -> "${DB_USER:-postgres}",
        "password" -> "${DB_PASSWORD:-secret}"
      )
    )

    val interpolated = ConnectionResolver.interpolateConnection(connection)

    assert(interpolated.options("url") == "jdbc:postgresql://localhost:5432/db")
    assert(interpolated.options("user") == "postgres")
    assert(interpolated.options("password") == "secret")
  }

  test("connectionToMap converts Connection to Map") {
    val connection = Connection(
      Some("db"),
      "postgres",
      Map(
        "url" -> "jdbc:postgresql://localhost:5432/db",
        "user" -> "postgres",
        "password" -> "secret"
      )
    )

    val connectionMap = ConnectionResolver.connectionToMap(connection)

    assert(connectionMap("format") == "postgres")
    assert(connectionMap("url") == "jdbc:postgresql://localhost:5432/db")
    assert(connectionMap("user") == "postgres")
    assert(connectionMap("password") == "secret")
  }

  test("connectionToMap without URL") {
    val connection = Connection(
      Some("csv_data"),
      "csv",
      Map("path" -> "/tmp/data", "header" -> "true")
    )

    val connectionMap = ConnectionResolver.connectionToMap(connection)

    assert(connectionMap("format") == "csv")
    assert(!connectionMap.contains("url"))
    assert(connectionMap("path") == "/tmp/data")
    assert(connectionMap("header") == "true")
  }

  test("validateConnections with all valid connections") {
    val planConnections = List(
      Connection(Some("db1"), "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db1"))
    )
    val plan = Plan(
      tasks = List(
        TaskSummary(name = "task1", connection = Some(Left("db1"))),
        TaskSummary(name = "task2", connection = Some(Right(Connection(None, "csv", Map.empty))))
      ),
      connections = Some(planConnections)
    )

    val errors = ConnectionResolver.validateConnections(plan, Map.empty)

    assert(errors.isEmpty)
  }

  test("validateConnections with invalid connection reference") {
    val plan = Plan(
      tasks = List(TaskSummary(name = "task1", connection = Some(Left("nonexistent_db")))),
      connections = None
    )

    val errors = ConnectionResolver.validateConnections(plan, Map.empty)

    assert(errors.nonEmpty)
    assert(errors.head.contains("task1"))
    assert(errors.head.contains("nonexistent_db"))
  }

  test("validateConnections allows fallback to application.conf") {
    val plan = Plan(
      tasks = List(TaskSummary(name = "task1", connection = Some(Left("legacy_db")))),
      connections = None
    )
    val appConfConnections = Map("legacy_db" -> Map("format" -> "postgres"))

    val errors = ConnectionResolver.validateConnections(plan, appConfConnections)

    assert(errors.isEmpty) // Should be valid via application.conf
  }

  test("getConnectionSources shows plan and app.conf sources") {
    val planConnections = List(
      Connection(Some("db1"), "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db1"))
    )
    val plan = Plan(connections = Some(planConnections))
    val appConfConnections = Map("db2" -> Map("format" -> "mysql"))

    val sources = ConnectionResolver.getConnectionSources(plan, appConfConnections)

    assert(sources("db1") == "plan")
    assert(sources("db2") == "application.conf")
  }

  test("getConnectionSources with plan overriding application.conf") {
    val planConnections = List(
      Connection(Some("db1"), "postgres", Map("url" -> "jdbc:postgresql://localhost:5432/db1"))
    )
    val plan = Plan(connections = Some(planConnections))
    val appConfConnections = Map("db1" -> Map("format" -> "mysql")) // Same name!

    val sources = ConnectionResolver.getConnectionSources(plan, appConfConnections)

    assert(sources("db1") == "plan") // Plan takes precedence
    assert(sources.size == 1)
  }

  test("resolveTaskConnection priority: inline > plan > application.conf") {
    val inlineConnection = Connection(None, "inline_type", Map("url" -> "inline_url"))
    val planConnections = Map(
      "conn1" -> Connection(Some("conn1"), "plan_type", Map("url" -> "plan_url"))
    )
    val appConfConnections = Map(
      "conn1" -> Map("format" -> "appconf_type", "url" -> "appconf_url")
    )

    // Test 1: Inline connection should be used
    val taskSummary1 = TaskSummary(name = "task1", connection = Some(Right(inlineConnection)))
    val result1 = ConnectionResolver.resolveTaskConnection(taskSummary1, planConnections, appConfConnections)
    assert(result1.isDefined)
    assert(result1.get._1("format") == "inline_type")
    assert(!result1.get._2) // Not from app.conf

    // Test 2: Plan connection should be used over app.conf
    val taskSummary2 = TaskSummary(name = "task2", connection = Some(Left("conn1")))
    val result2 = ConnectionResolver.resolveTaskConnection(taskSummary2, planConnections, appConfConnections)
    assert(result2.isDefined)
    assert(result2.get._1("format") == "plan_type")
    assert(!result2.get._2) // Not from app.conf

    // Test 3: Application.conf should be used as last resort
    val taskSummary3 = TaskSummary(name = "task3", dataSourceName = "conn1", connection = None)
    val result3 = ConnectionResolver.resolveTaskConnection(taskSummary3, Map.empty, appConfConnections)
    assert(result3.isDefined)
    assert(result3.get._1("format") == "appconf_type")
    assert(result3.get._2) // From app.conf
  }
}
