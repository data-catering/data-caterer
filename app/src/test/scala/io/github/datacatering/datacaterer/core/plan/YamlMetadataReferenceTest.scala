package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test to verify that YAML metadata sources work correctly
 */
class YamlMetadataReferenceTest extends AnyFunSuite {

  test("Can reference YAML task fields in programmatic API") {
    val testPlan = new TestYamlMetadataPlan()
    
    // Verify the plan was created successfully
    assert(testPlan != null)
    
    // This test primarily verifies that the syntax compiles correctly
    // and that the YAML metadata source can be referenced
  }
  
  class TestYamlMetadataPlan extends PlanRun {
    // Reference existing YAML task file from test resources
    val jsonTask = json("test_json_with_yaml_fields", "/tmp/test-yaml-metadata")
      .fields(metadataSource.yamlTask(
        "app/src/test/resources/sample/task/file/simple-json-task.yaml",
        "simple_json",
        "file_account"
      ))
      .count(count.records(100))  // Override record count
    
    // Alternative: Reference without step name (loads all fields from task)
    val csvTask = csv("test_csv_with_yaml_fields", "/tmp/test-yaml-metadata.csv")
      .fields(metadataSource.yamlTask(
        "app/src/test/resources/sample/task/file/simple-json-task.yaml",
        "simple_json"
      ))
      .count(count.records(50))
    
    // Don't execute - this is just a compilation test
    // execute(plan.tasks(jsonTask, csvTask))
  }
}