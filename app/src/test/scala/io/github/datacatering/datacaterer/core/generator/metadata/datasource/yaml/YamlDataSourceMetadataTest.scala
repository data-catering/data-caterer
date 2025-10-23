package io.github.datacatering.datacaterer.core.generator.metadata.datasource.yaml

import io.github.datacatering.datacaterer.api.model.Constants.{METADATA_IDENTIFIER, YAML_PLAN_FILE, YAML_STEP_NAME, YAML_TASK_FILE, YAML_TASK_NAME}
import io.github.datacatering.datacaterer.core.util.SparkSuite

class YamlDataSourceMetadataTest extends SparkSuite {

  test("can extract metadata from simple YAML plan file") {
    val planPath = getClass.getResource("/sample/yaml/simple-plan.yaml").getPath
    val connectionConfig = Map(YAML_PLAN_FILE -> planPath)
    
    val metadata = YamlDataSourceMetadata("test-plan", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 2)
    
    // Check that we have the expected tasks
    val identifiers = subDataSources.map(_.readOptions(METADATA_IDENTIFIER)).toSet
    assert(identifiers.contains("my_postgres.user-task"))
    assert(identifiers.contains("my_kafka.order-task"))
    
    // Plan metadata doesn't include field metadata (TaskSummary doesn't have steps)
    assert(subDataSources.forall(_.optFieldMetadata.isEmpty))
  }

  test("can extract metadata from simple YAML task file") {
    val taskPath = getClass.getResource("/sample/yaml/simple-task.yaml").getPath
    val connectionConfig = Map(YAML_TASK_FILE -> taskPath)
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    val subDataSource = subDataSources.head
    assert(subDataSource.readOptions(METADATA_IDENTIFIER) == "user-task.users")
    assert(subDataSource.optFieldMetadata.isDefined)
    
    val fieldMetadata = subDataSource.optFieldMetadata.get.collect()
    assert(fieldMetadata.length == 5) // user_id, username, email, age, profile
    
    // Check basic field metadata
    val fieldNames = fieldMetadata.map(_.field).toSet
    assert(fieldNames.contains("user_id"))
    assert(fieldNames.contains("username"))
    assert(fieldNames.contains("email"))
    assert(fieldNames.contains("age"))
    assert(fieldNames.contains("profile"))
    
    // Verify field types and options
    val userIdField = fieldMetadata.find(_.field == "user_id").get
    assert(userIdField.metadata("type") == "string")
    assert(userIdField.metadata("nullable") == "false")
    assert(userIdField.metadata.contains("regex"))
    
    val emailField = fieldMetadata.find(_.field == "email").get
    assert(emailField.metadata("type") == "string")
    assert(emailField.metadata("nullable") == "true")
    assert(emailField.metadata.contains("expression"))
  }

  test("can handle nested fields in YAML task file") {
    val taskPath = getClass.getResource("/sample/yaml/simple-task.yaml").getPath
    val connectionConfig = Map(YAML_TASK_FILE -> taskPath)
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    val fieldMetadata = subDataSources.head.optFieldMetadata.get.collect()
    val profileField = fieldMetadata.find(_.field == "profile").get
    
    // Check nested fields
    assert(profileField.nestedFields.length == 3) // first_name, last_name, address
    
    val nestedFieldNames = profileField.nestedFields.map(_.field).toSet
    assert(nestedFieldNames.contains("first_name"))
    assert(nestedFieldNames.contains("last_name"))
    assert(nestedFieldNames.contains("address"))
    
    // Check deeply nested fields
    val addressField = profileField.nestedFields.find(_.field == "address").get
    assert(addressField.nestedFields.length == 2) // street, city
    
    val deepNestedFieldNames = addressField.nestedFields.map(_.field).toSet
    assert(deepNestedFieldNames.contains("street"))
    assert(deepNestedFieldNames.contains("city"))
  }

  test("can filter by task name") {
    val taskPath = getClass.getResource("/sample/yaml/multi-task.yaml").getPath
    val connectionConfig = Map(
      YAML_TASK_FILE -> taskPath,
      YAML_TASK_NAME -> "multi-step-task"
    )
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 2) // users and orders steps
    
    val identifiers = subDataSources.map(_.readOptions(METADATA_IDENTIFIER)).toSet
    assert(identifiers.contains("multi-step-task.users"))
    assert(identifiers.contains("multi-step-task.orders"))
  }

  test("can filter by step name") {
    val taskPath = getClass.getResource("/sample/yaml/multi-task.yaml").getPath
    val connectionConfig = Map(
      YAML_TASK_FILE -> taskPath,
      YAML_TASK_NAME -> "multi-step-task",
      YAML_STEP_NAME -> "users"
    )
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    val subDataSource = subDataSources.head
    assert(subDataSource.readOptions(METADATA_IDENTIFIER) == "multi-step-task.users")
    
    val fieldMetadata = subDataSource.optFieldMetadata.get.collect()
    assert(fieldMetadata.length == 2) // id, name
    
    val fieldNames = fieldMetadata.map(_.field).toSet
    assert(fieldNames.contains("id"))
    assert(fieldNames.contains("name"))
  }

  test("handles empty field list gracefully") {
    val taskPath = getClass.getResource("/sample/yaml/simple-task.yaml").getPath
    val connectionConfig = Map(YAML_TASK_FILE -> taskPath)
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    
    // Test with empty fields
    val emptyFieldsDataset = metadata.convertStepToFieldMetadata(List.empty, "test.step")
    assert(emptyFieldsDataset.count() == 0)
  }

  test("handles invalid file paths gracefully") {
    val connectionConfig = Map(YAML_TASK_FILE -> "/nonexistent/path.yaml")
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.isEmpty)
  }

  test("prefers task file when both plan and task files are specified") {
    val planPath = getClass.getResource("/sample/yaml/simple-plan.yaml").getPath
    val taskPath = getClass.getResource("/sample/yaml/simple-task.yaml").getPath
    val connectionConfig = Map(
      YAML_PLAN_FILE -> planPath,
      YAML_TASK_FILE -> taskPath
    )
    
    val metadata = YamlDataSourceMetadata("test", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    // Should use task file (1 sub data source) instead of plan file (2 sub data sources)
    assert(subDataSources.length == 1)
    assert(subDataSources.head.readOptions(METADATA_IDENTIFIER) == "user-task.users")
  }

  test("validates hasSourceData is false") {
    val taskPath = getClass.getResource("/sample/yaml/simple-task.yaml").getPath
    val connectionConfig = Map(YAML_TASK_FILE -> taskPath)
    
    val metadata = YamlDataSourceMetadata("test", "yaml", connectionConfig)
    assert(!metadata.hasSourceData)
  }

  test("static field values are preserved in metadata") {
    // Create a simple field with static value to test this functionality
    import io.github.datacatering.datacaterer.api.model.Field
    
    val staticField = Field(
      name = "status",
      `type` = Some("string"),
      static = Some("ACTIVE"),
      nullable = false
    )
    
    val taskPath = getClass.getResource("/sample/yaml/simple-task.yaml").getPath
    val connectionConfig = Map(YAML_TASK_FILE -> taskPath)
    val metadata = YamlDataSourceMetadata("test", "yaml", connectionConfig)

    // Test the conversion method directly
    val readOptions = Map(METADATA_IDENTIFIER -> "test.step")
    val fieldMetadata = metadata.convertFieldToFieldMetadata(staticField, readOptions)

    assert(fieldMetadata.field == "status")
    assert(fieldMetadata.metadata("type") == "string")
    assert(fieldMetadata.metadata("nullable") == "false")
    assert(fieldMetadata.metadata("static") == "ACTIVE")
    assert(fieldMetadata.dataSourceReadOptions(METADATA_IDENTIFIER) == "test.step")
  }

  test("includes YAML context in readOptions for task metadata") {
    val taskPath = getClass.getResource("/sample/yaml/multi-task.yaml").getPath
    val connectionConfig = Map(
      YAML_TASK_FILE -> taskPath,
      YAML_TASK_NAME -> "multi-step-task",
      YAML_STEP_NAME -> "users"
    )
    
    val metadata = YamlDataSourceMetadata("test-task", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    val subDataSource = subDataSources.head
    val readOptions = subDataSource.readOptions
    
    // Verify YAML context is included in readOptions
    assert(readOptions(METADATA_IDENTIFIER) == "multi-step-task.users")
    assert(readOptions(YAML_TASK_FILE) == taskPath)
    assert(readOptions(YAML_TASK_NAME) == "multi-step-task") 
    assert(readOptions(YAML_STEP_NAME) == "users")
  }

  test("includes YAML context in readOptions for plan metadata") {
    val planPath = getClass.getResource("/sample/yaml/simple-plan.yaml").getPath
    val connectionConfig = Map(YAML_PLAN_FILE -> planPath)
    
    val metadata = YamlDataSourceMetadata("test-plan", "yaml", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 2)
    
    val subDataSource = subDataSources.head
    val readOptions = subDataSource.readOptions
    
    // Verify YAML context is included in readOptions
    assert(readOptions.contains(METADATA_IDENTIFIER))
    assert(readOptions(YAML_PLAN_FILE) == planPath)
    assert(!readOptions.contains(YAML_TASK_FILE))
    assert(!readOptions.contains(YAML_TASK_NAME))
    assert(!readOptions.contains(YAML_STEP_NAME))
  }
}