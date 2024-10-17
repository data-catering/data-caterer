package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.{Count, Field, FoldersConfig, Generator, Schema, Step, Task}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import scala.reflect.io.Directory

@RunWith(classOf[JUnitRunner])
class PlanGeneratorTest extends SparkSuite {

  test("Write plan and tasks to file system") {
    val folderPath = "src/test/resources/sample/plan-gen"
    val task = Task("basic_account", List(
      Step("account_json", "json", Count(), Map(),
        Schema(Some(List(
          Field("id", Some("string"), Some(Generator("random", Map("unique" -> "true")))),
          Field("name", Some("string"), Some(Generator("random", Map("expression" -> "#{Name.name}")))),
          Field("amount", Some("double"), Some(Generator("random", Map("min" -> "10.0")))),
        )))
      )
    ))
    val foreignKeys = List(("json.account_json.id", List("postgres.account.id"), List()))

    PlanGenerator.writeToFiles(None, List(("account_json", task)), foreignKeys, List(), FoldersConfig(generatedPlanAndTaskFolderPath = folderPath))

    val planFolder = new File(folderPath + "/plan")
    assert(planFolder.exists())
    assert(planFolder.list().length == 1)
    val taskFolder = new File(folderPath + "/task/")
    assert(taskFolder.exists())
    assert(taskFolder.list().length == 1)
    assert(taskFolder.list().head == "basic_account_task.yaml")
    new Directory(planFolder).deleteRecursively()
    new Directory(taskFolder).deleteRecursively()
  }
}
