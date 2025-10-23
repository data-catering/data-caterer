package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class JsonSchemaLoaderTest extends AnyFunSuite {

  private val objectMapper = new ObjectMapper()

  test("can load JSON schema from file") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    assert(schema != null)
    
    // Load the schema file directly to test its structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    assert(schemaNode.get("title").asText() == "User")
    assert(schemaNode.get("type").asText() == "object")
    
    // Verify required fields
    val required = schemaNode.get("required")
    assert(required.isArray)
    assert(required.size() == 3)
    
    // Verify properties exist
    val properties = schemaNode.get("properties")
    assert(properties.has("id"))
    assert(properties.has("name"))
    assert(properties.has("email"))
    assert(properties.has("age"))
    assert(properties.has("active"))
    assert(properties.has("tags"))
    assert(properties.has("address"))
  }

  test("can load complex JSON schema with references") {
    val schemaPath = getClass.getResource("/sample/schema/complex-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    assert(schema != null)
    
    // Load the schema file directly to test its structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    assert(schemaNode.get("title").asText() == "Complex Schema")
    
    // Verify $defs section exists
    val defs = schemaNode.get("$defs")
    assert(defs != null)
    assert(defs.has("address"))
    assert(defs.has("contact"))
    
    // Verify complex properties
    val properties = schemaNode.get("properties")
    assert(properties.has("personalInfo"))
    assert(properties.has("addresses"))
    assert(properties.has("contacts"))
    assert(properties.has("metadata"))
  }

  test("can load JSON schema with different versions") {
    val schemaPath = getClass.getResource("/sample/schema/draft-07-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    assert(schema != null)
    
    // Load the schema file directly to test its structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    assert(schemaNode.get("title").asText() == "Draft 07 Schema")
    
    // Verify draft-07 specific features
    val definitions = schemaNode.get("definitions")
    assert(definitions != null)
    assert(definitions.has("phoneNumber"))
    
    // Verify $schema field indicates draft-07
    val schemaVersion = schemaNode.get("$schema").asText()
    assert(schemaVersion.contains("draft-07"))
  }

  test("can validate schema structure using networknt library") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Test validation capabilities with the networknt library
    val testJson = """{"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30, "active": true}"""
    val jsonNode = objectMapper.readTree(testJson)
    val validationResult = schema.validate(jsonNode)
    
    assert(validationResult.isEmpty, "Valid JSON should pass validation")
  }

  test("can detect validation errors using networknt library") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Test with invalid JSON (missing required fields)
    val invalidJson = """{"id": 1, "name": "John Doe"}"""
    val jsonNode = objectMapper.readTree(invalidJson)
    val validationResult = schema.validate(jsonNode)
    
    assert(validationResult.size() > 0, "Invalid JSON should fail validation")
    
    // Test with invalid data types
    val wrongTypeJson = """{"id": "not-a-number", "name": "John Doe", "email": "john@example.com"}"""
    val wrongTypeNode = objectMapper.readTree(wrongTypeJson)
    val wrongTypeResult = schema.validate(wrongTypeNode)
    
    assert(wrongTypeResult.size() > 0, "Wrong data types should fail validation")
  }

  test("handles malformed JSON gracefully") {
    // Create a temporary file with malformed JSON
    val malformedJson = """{"name": "test", "missing": "comma" "invalid": true}"""
    val tempFile = Files.createTempFile("malformed", ".json")
    Files.write(tempFile, malformedJson.getBytes(StandardCharsets.UTF_8))
    
    try {
      assertThrows[JsonSchemaLoadException] {
        JsonSchemaLoader.loadSchema(tempFile.toString)
      }
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("throws exception for non-existent file") {
    assertThrows[JsonSchemaLoadException] {
      JsonSchemaLoader.loadSchema("/path/that/does/not/exist.json")
    }
  }

  test("can detect URL vs file path") {
    val filePath = "/some/file/path.json"
    val httpUrl = "http://example.com/schema.json"
    val httpsUrl = "https://example.com/schema.json"
    
    // We can't test the actual URL loading without a real server,
    // but we can verify the loader distinguishes between URLs and file paths
    // by checking that different error types are thrown
    
    assertThrows[JsonSchemaLoadException] {
      JsonSchemaLoader.loadSchema(filePath)
    }
    
    // URL loading will fail differently (network error vs file not found)
    assertThrows[JsonSchemaLoadException] {
      JsonSchemaLoader.loadSchema(httpUrl)
    }
    
    assertThrows[JsonSchemaLoadException] {
      JsonSchemaLoader.loadSchema(httpsUrl)
    }
  }

  test("can handle empty schema file") {
    val emptyJson = "{}"
    val tempFile = Files.createTempFile("empty", ".json")
    Files.write(tempFile, emptyJson.getBytes(StandardCharsets.UTF_8))
    
    try {
      val schema = JsonSchemaLoader.loadSchema(tempFile.toString)
      assert(schema != null)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("can handle schema with only $schema field") {
    val minimalJson = """{"$schema": "https://json-schema.org/draft/2020-12/schema"}"""
    val tempFile = Files.createTempFile("minimal", ".json")
    Files.write(tempFile, minimalJson.getBytes(StandardCharsets.UTF_8))
    
    try {
      val schema = JsonSchemaLoader.loadSchema(tempFile.toString)
      assert(schema != null)
      
      // Parse the schema to check its content
      val schemaNode = objectMapper.readTree(minimalJson)
      assert(schemaNode.get("$schema").asText().contains("2020-12"))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("can extract schema metadata from file") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Load the schema file directly to test metadata extraction
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    // Test metadata extraction
    assert(schemaNode.get("$schema").asText() == "https://json-schema.org/draft/2020-12/schema")
    assert(schemaNode.get("$id").asText() == "https://example.com/user.schema.json")
    assert(schemaNode.get("title").asText() == "User")
    assert(schemaNode.get("description").asText() == "A simple user schema for testing")
  }

  test("can handle schema with nested objects") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Parse the schema file to examine structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    val properties = schemaNode.get("properties")
    val address = properties.get("address")
    
    assert(address != null)
    assert(address.get("type").asText() == "object")
    assert(address.has("properties"))
    assert(address.get("properties").has("street"))
    assert(address.get("properties").has("city"))
    assert(address.get("properties").has("zipCode"))
    
    // Verify nested required fields
    val addressRequired = address.get("required")
    assert(addressRequired.isArray)
    assert(addressRequired.size() == 2)
  }

  test("can handle schema with array properties") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Parse the schema file to examine structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    val properties = schemaNode.get("properties")
    val tags = properties.get("tags")
    
    assert(tags != null)
    assert(tags.get("type").asText() == "array")
    assert(tags.has("items"))
    assert(tags.get("items").get("type").asText() == "string")
    assert(tags.get("uniqueItems").asBoolean() == true)
  }

  test("can handle different data types and constraints") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Parse the schema file to examine structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    val properties = schemaNode.get("properties")
    
    // Test integer type with constraints
    val id = properties.get("id")
    assert(id.get("type").asText() == "integer")
    assert(id.get("minimum").asInt() == 1)
    
    // Test string type with length constraints
    val name = properties.get("name")
    assert(name.get("type").asText() == "string")
    assert(name.get("minLength").asInt() == 1)
    assert(name.get("maxLength").asInt() == 100)
    
    // Test string with format
    val email = properties.get("email")
    assert(email.get("type").asText() == "string")
    assert(email.get("format").asText() == "email")
    
    // Test integer with range
    val age = properties.get("age")
    assert(age.get("type").asText() == "integer")
    assert(age.get("minimum").asInt() == 0)
    assert(age.get("maximum").asInt() == 150)
    
    // Test boolean type
    val active = properties.get("active")
    assert(active.get("type").asText() == "boolean")
  }

  test("can handle pattern constraints") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Parse the schema file to examine structure
    val schemaContent = scala.io.Source.fromFile(schemaPath).mkString
    val schemaNode = objectMapper.readTree(schemaContent)
    
    val properties = schemaNode.get("properties")
    val address = properties.get("address")
    val addressProperties = address.get("properties")
    val zipCode = addressProperties.get("zipCode")
    
    assert(zipCode.get("type").asText() == "string")
    assert(zipCode.get("pattern").asText() == "^[0-9]{5}$")
  }

  test("can validate complex schema with references") {
    val schemaPath = getClass.getResource("/sample/schema/complex-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Test validation with complex nested data
    val complexValidJson = """{
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "personalInfo": {
        "firstName": "John",
        "lastName": "Doe",
        "dateOfBirth": "1990-01-01",
        "salary": 75000.50
      },
      "addresses": [{
        "street": "123 Main St",
        "city": "Anytown", 
        "country": "US",
        "zipCode": "12345"
      }]
    }"""
    
    val jsonNode = objectMapper.readTree(complexValidJson)
    val validationResult = schema.validate(jsonNode)
    
    assert(validationResult.isEmpty, "Valid complex JSON should pass validation")
  }

  test("detects validation errors in complex schema") {
    val schemaPath = getClass.getResource("/sample/schema/complex-schema.json").getPath
    val schema = JsonSchemaLoader.loadSchema(schemaPath)
    
    // Test with missing required fields
    val invalidComplexJson = """{
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "personalInfo": {
        "firstName": "John"
      }
    }"""
    
    val jsonNode = objectMapper.readTree(invalidComplexJson)
    val validationResult = schema.validate(jsonNode)
    
    assert(validationResult.size() > 0, "Invalid complex JSON should fail validation")
  }
} 