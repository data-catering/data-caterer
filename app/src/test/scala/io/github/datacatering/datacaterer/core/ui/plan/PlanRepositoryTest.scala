package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.{Plan, Step, Task}
import io.github.datacatering.datacaterer.core.ui.model.{Connection, PlanRunRequest}
import io.github.datacatering.datacaterer.core.plan.YamlPlanRun
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.PrivateMethodTester
import java.nio.file.{Files, Path}
import java.io.File
import scala.reflect.io.Directory

class PlanRepositoryTest extends AnyFunSuite with Matchers with PrivateMethodTester {

  test("should map Postgres connection details correctly including dbtable from schema and table") {
    // Setup temp directory for connections
    val tempDir = Files.createTempDirectory("datacaterer-test")
    System.setProperty("data-caterer-install-dir", tempDir.toString)
    
    try {
      val connectionDir = tempDir.resolve("connection")
      Files.createDirectories(connectionDir)
      
      val dataSourceName = "test-postgres"
      val connection = Connection(dataSourceName, "postgres", Some("datasource"), Map("url" -> "jdbc:postgresql://localhost:5432/test", "user" -> "user", "password" -> "pass"))
      Files.writeString(connectionDir.resolve(s"$dataSourceName.csv"), connection.toString)
      
      val planName = "test-plan"
      val taskName = "test-task"
      // UI sends schema and table separately, not dbtable
      val step = Step(name = taskName, `type` = "jdbc", options = Map("schema" -> "public", "table" -> "users"))
      
      val plan = Plan(planName, "desc", List(io.github.datacatering.datacaterer.api.model.TaskSummary(taskName, dataSourceName)))
      val request = PlanRunRequest("run-1", plan, List(step))
      
      // Invoke private method getPlanAsYaml
      val getPlanAsYaml = PrivateMethod[YamlPlanRun]('getPlanAsYaml)
      val yamlPlanRun = PlanRepository invokePrivate getPlanAsYaml(request, tempDir.toString)
      
      // Check if the task has the correct options
      val tasks = yamlPlanRun._tasks
      tasks should have size 1
      val task = tasks.head
      task.steps should have size 1
      val stepOptions = task.steps.head.options
      
      // Verify that schema and table were converted to dbtable
      stepOptions should contain key "dbtable"
      stepOptions("dbtable") should be ("public.users")
      stepOptions.contains("schema") should be (false)
      stepOptions.contains("table") should be (false)
      stepOptions should contain key "url"
      stepOptions should contain key "user"
      stepOptions should contain key "password"
      stepOptions should contain key "driver"
      stepOptions("driver") should be ("org.postgresql.Driver")
      
    } finally {
      // Cleanup
      val directory = new Directory(new File(tempDir.toString))
      directory.deleteRecursively()
      System.clearProperty("data-caterer-install-dir")
    }
  }
  
  test("should fail if dbtable is missing for Postgres connection") {
     // Setup temp directory for connections
    val tempDir = Files.createTempDirectory("datacaterer-test-fail")
    System.setProperty("data-caterer-install-dir", tempDir.toString)
    
    try {
      val connectionDir = tempDir.resolve("connection")
      Files.createDirectories(connectionDir)
      
      val dataSourceName = "test-postgres-fail"
      val connection = Connection(dataSourceName, "postgres", Some("datasource"), Map("url" -> "jdbc:postgresql://localhost:5432/test", "user" -> "user", "password" -> "pass"))
      Files.writeString(connectionDir.resolve(s"$dataSourceName.csv"), connection.toString)
      
      val planName = "test-plan-fail"
      val taskName = "test-task-fail"
      // Missing dbtable in options
      val step = Step(name = taskName, `type` = "jdbc", options = Map())
      
      val plan = Plan(planName, "desc", List(io.github.datacatering.datacaterer.api.model.TaskSummary(taskName, dataSourceName)))
      val request = PlanRunRequest("run-1", plan, List(step))
      
      // Invoke private method getPlanAsYaml
      val getPlanAsYaml = PrivateMethod[YamlPlanRun]('getPlanAsYaml)
      val yamlPlanRun = PlanRepository invokePrivate getPlanAsYaml(request, tempDir.toString)
      
      // Check options
      val tasks = yamlPlanRun._tasks
      val stepOptions = tasks.head.steps.head.options
      
      // This assertion confirms that dbtable is indeed missing, which causes the Spark error later
      stepOptions.contains("dbtable") should be (false)
      stepOptions.contains("query") should be (false)
      
    } finally {
      val directory = new Directory(new File(tempDir.toString))
      directory.deleteRecursively()
      System.clearProperty("data-caterer-install-dir")
    }
  }
}
