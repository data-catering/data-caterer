package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.scalatest.funsuite.AnyFunSuite

class JsonSchemaParserTest extends AnyFunSuite {

  test("can parse simple JSON schema from string") {
    val simpleSchema =
      """
        |{
        |  "$schema": "https://json-schema.org/draft/2020-12/schema",
        |  "type": "object",
        |  "properties": {
        |    "name": {
        |      "type": "string",
        |      "minLength": 1,
        |      "maxLength": 100
        |    },
        |    "age": {
        |      "type": "integer",
        |      "minimum": 0,
        |      "maximum": 150
        |    }
        |  },
        |  "required": ["name"]
        |}
        |""".stripMargin

    val schema = JsonSchemaParser.parseSchemaFromString(simpleSchema)
    
    assert(schema != null)
    assert(schema.isObjectType)
    assert(schema.getSchemaVersion == JsonSchemaVersion.Draft202012)
    assert(schema.properties.isDefined)
    assert(schema.properties.get.contains("name"))
    assert(schema.properties.get.contains("age"))
  }

  test("can parse JSON schema from test file") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaParser.parseSchema(schemaPath)
    
    assert(schema != null)
    assert(schema.isObjectType)
  }

  test("can detect schema version") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val version = JsonSchemaParser.detectSchemaVersion(schemaPath)
    
    assert(version != null)
  }

  test("JsonSchemaDefinition helper methods work correctly") {
    val schema = JsonSchemaDefinition(
      `type` = Some("string"),
      format = Some("email"),
      minLength = Some(5),
      maxLength = Some(100)
    )
    
    assert(schema.isStringType)
    assert(!schema.isArrayType)
    assert(!schema.isObjectType)
    assert(!schema.hasReference)
    assert(!schema.hasComposition)
  }

  test("JsonSchemaConstraints helper methods work correctly") {
    val constraints = JsonSchemaConstraints(
      pattern = Some("[a-z]+"),
      minimum = Some("0.0"),
      maximum = Some("100.0"),
      minItems = Some(1),
      `enum` = Some(List("red", "green", "blue"))
    )
    
    assert(constraints.hasStringConstraints)
    assert(constraints.hasNumericConstraints) 
    assert(constraints.hasArrayConstraints)
    assert(constraints.hasValueConstraints)
    assert(!constraints.isEmpty)
  }

  test("JsonSchemaVersion.fromString works correctly") {
    assert(JsonSchemaVersion.fromString("https://json-schema.org/draft/2020-12/schema") == JsonSchemaVersion.Draft202012)
    assert(JsonSchemaVersion.fromString("http://json-schema.org/draft-07/schema#") == JsonSchemaVersion.Draft7)
    assert(JsonSchemaVersion.fromString("http://json-schema.org/draft-04/schema#") == JsonSchemaVersion.Draft4)
    assert(JsonSchemaVersion.fromString("unknown") == JsonSchemaVersion.Draft202012) // Default
  }

  test("JsonSchemaType validation works") {
    assert(JsonSchemaType.isValidType("string"))
    assert(JsonSchemaType.isValidType("object"))
    assert(JsonSchemaType.isValidType("array"))
    assert(!JsonSchemaType.isValidType("invalid"))
  }

  test("JsonSchemaFormat validation works") {
    assert(JsonSchemaFormat.isValidFormat("email"))
    assert(JsonSchemaFormat.isValidFormat("date-time"))
    assert(JsonSchemaFormat.isValidFormat("uuid"))
    assert(!JsonSchemaFormat.isValidFormat("invalid"))
  }

  test("can parse complex schema with validation constraints") {
    val complexSchema = """
    {
      "type": "object",
      "properties": {
        "score": {
          "type": "number",
          "minimum": 0.0,
          "maximum": 100.0
        }
      }
    }
    """

    val schema = JsonSchemaParser.parseSchemaFromString(complexSchema)
    
    assert(schema.`type`.contains("object"))
    assert(schema.properties.isDefined)
    
    val scoreProperty = schema.properties.get("score")
    assert(scoreProperty.`type`.contains("number"))
    assert(scoreProperty.minimum.contains("0.0"))
    assert(scoreProperty.maximum.contains("100.0"))
  }
} 