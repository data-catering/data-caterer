package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model.Constants.{JSON_SCHEMA_FILE, METADATA_IDENTIFIER}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.FieldMetadata
import io.github.datacatering.datacaterer.core.util.SparkSuite

class JsonSchemaDataSourceMetadataTest extends SparkSuite {

  test("can extract metadata from simple JSON schema") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val connectionConfig = Map(JSON_SCHEMA_FILE -> schemaPath)
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    val fieldMetadata = subDataSources.head.optFieldMetadata.get.collect()
    assert(fieldMetadata.length > 0)
    
    // Check that we have the expected fields
    val fieldNames = fieldMetadata.map(_.field).toSet
    assert(fieldNames.contains("name"))
    assert(fieldNames.contains("age"))
    assert(fieldNames.contains("email"))
    
    // Verify field metadata structure
    val nameField = fieldMetadata.find(_.field == "name").get
    assert(nameField.metadata.contains("type"))
    assert(nameField.metadata.contains("nullable"))
  }

  test("can extract metadata from array schema") {
    val schemaPath = getClass.getResource("/sample/schema/array-schema.json").getPath
    val connectionConfig = Map(JSON_SCHEMA_FILE -> schemaPath)
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    val fieldMetadata = subDataSources.head.optFieldMetadata.get.collect()
    assert(fieldMetadata.length > 0)
    
    // Check that we have array fields - the actual field names from the schema
    val fieldNames = fieldMetadata.map(_.field).toSet
    println(s"Actual field names: $fieldNames") // Debug output
    
    // The array schema creates a root_array field with nested elements
    assert(fieldNames.contains("root_array"))
    
    // Check array field metadata
    val arrayField = fieldMetadata.find(_.field == "root_array").get
    assert(arrayField.metadata("type").contains("array"))
  }

  test("handles missing schema file gracefully") {
    val connectionConfig = Map(JSON_SCHEMA_FILE -> "/nonexistent/schema.json")
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    
    assertThrows[Exception] {
      metadata.getSubDataSourcesMetadata
    }
  }

  test("validates required configuration") {
    val connectionConfig = Map("someOtherConfig" -> "value")
    
    assertThrows[IllegalArgumentException] {
      JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    }
  }

  test("field metadata contains all expected properties") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val connectionConfig = Map(JSON_SCHEMA_FILE -> schemaPath)
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    val fieldMetadata = subDataSources.head.optFieldMetadata.get.collect()
    val nameField = fieldMetadata.find(_.field == "name").get
    
    // Verify all expected metadata properties are present
    assert(nameField.metadata.contains("type"))
    assert(nameField.metadata.contains("nullable"))
    
    // Check that field options from JSON schema are included
    // (these would come from constraints like minLength, maxLength, etc.)
    assert(nameField.metadata.nonEmpty)
  }

  test("correctly handles nested object fields from complex schema") {
    val schemaPath = getClass.getResource("/sample/schema/complex-user-schema.json").getPath
    val connectionConfig = Map(JSON_SCHEMA_FILE -> schemaPath)
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    val fieldMetadata = subDataSources.head.optFieldMetadata.get.collect()
    assert(fieldMetadata.length > 0)
    
    // Verify top level fields
    val fieldNames = fieldMetadata.map(_.field).toSet
    assert(fieldNames.contains("id"))
    assert(fieldNames.contains("profile"))
    assert(fieldNames.contains("addresses"))

    // Verify nested fields
    val profileField = fieldMetadata.find(_.field == "profile").get
    val nestedFields = profileField.nestedFields
    assert(nestedFields.length == 4)
    assert(nestedFields.map(_.field).toSet.contains("name"))
    assert(nestedFields.map(_.field).toSet.contains("email"))
    assert(nestedFields.map(_.field).toSet.contains("age"))
    assert(nestedFields.map(_.field).toSet.contains("website"))

    // Verify nested fields metadata
    val nameField = nestedFields.find(_.field == "name").get
    assert(nameField.metadata.contains("type"))
    assert(nameField.metadata.contains("minLen"))

    // Verify array nested fields
    val addressesField = fieldMetadata.find(_.field == "addresses").get
    val nestedAddressesFields = addressesField.nestedFields
    assert(nestedAddressesFields.length == 5)
    assert(nestedAddressesFields.map(_.field).toSet.contains("type"))
    assert(nestedAddressesFields.map(_.field).toSet.contains("street"))
    assert(nestedAddressesFields.map(_.field).toSet.contains("city"))
    assert(nestedAddressesFields.map(_.field).toSet.contains("zipCode"))
    assert(nestedAddressesFields.map(_.field).toSet.contains("coordinates"))

    // Verify nested coordinates fields
    val coordinatesField = nestedAddressesFields.find(_.field == "coordinates").get
    val nestedCoordinatesFields = coordinatesField.nestedFields
    assert(nestedCoordinatesFields.length == 2)
    assert(nestedCoordinatesFields.map(_.field).toSet.contains("latitude"))
    assert(nestedCoordinatesFields.map(_.field).toSet.contains("longitude"))

    // Verify nested coordinates fields metadata
    val latitudeField = nestedCoordinatesFields.find(_.field == "latitude").get
    assert(latitudeField.metadata.contains("type"))
    assert(latitudeField.metadata.contains("min"))
  }

  test("handles validation generation errors gracefully") {
    val connectionConfig = Map(JSON_SCHEMA_FILE -> "/non/existent/schema.json")
    val dataSourceReadOptions = Map(JSON_SCHEMA_FILE -> "/non/existent/schema.json")
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    val validations = metadata.getDataSourceValidations(dataSourceReadOptions)
    
    // Should return empty list when schema file doesn't exist
    assert(validations.isEmpty)
  }

  test("metadata identifier includes schema file name for task matching") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val connectionConfig = Map(JSON_SCHEMA_FILE -> schemaPath)
    
    val metadata = JsonSchemaDataSourceMetadata("test", "json", connectionConfig)
    val subDataSources = metadata.getSubDataSourcesMetadata
    
    assert(subDataSources.length == 1)
    
    // The metadata identifier should include the schema file name
    val metadataIdentifier = subDataSources.head.readOptions(METADATA_IDENTIFIER)
    assert(metadataIdentifier.contains("simple-user-schema_json"), 
      s"Metadata identifier should contain schema file name, actual: $metadataIdentifier")
    
    // Verify field metadata also has the same identifier
    val fieldMetadata = subDataSources.head.optFieldMetadata.get.collect()
    assert(fieldMetadata.length > 0)
    
    val firstField = fieldMetadata.head
    val fieldMetadataIdentifier = firstField.dataSourceReadOptions(METADATA_IDENTIFIER)
    assert(fieldMetadataIdentifier == metadataIdentifier,
      s"Field metadata identifier should match sub data source identifier, field: $fieldMetadataIdentifier, sub: $metadataIdentifier")
  }
} 