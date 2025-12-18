package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{ConfluentSchemaRegistrySource, DataContractCliSource, GreatExpectationsSource, JsonSchemaSource, MarquezMetadataSource, OpenAPISource, OpenDataContractStandardSource, OpenMetadataSource}
import org.scalatest.funsuite.AnyFunSuite

class MetadataSourceBuilderTest extends AnyFunSuite {

  test("Can create Marquez metadata source") {
    val result = MetadataSourceBuilder().marquez("localhost:8080", "food_delivery").metadataSource

    assert(result.isInstanceOf[MarquezMetadataSource])
    assert(result.asInstanceOf[MarquezMetadataSource].connectionOptions ==
      Map(METADATA_SOURCE_URL -> "localhost:8080", OPEN_LINEAGE_NAMESPACE -> "food_delivery"))
  }

  test("Can create Marquez metadata source with dataset") {
    val result = MetadataSourceBuilder().marquez("localhost:8080", "food_delivery", "public.delivery").metadataSource

    assert(result.isInstanceOf[MarquezMetadataSource])
    assert(result.asInstanceOf[MarquezMetadataSource].connectionOptions ==
      Map(METADATA_SOURCE_URL -> "localhost:8080", OPEN_LINEAGE_NAMESPACE -> "food_delivery", OPEN_LINEAGE_DATASET -> "public.delivery"))
  }

  test("Can create OpenMetadata metadata source") {
    val result = MetadataSourceBuilder().openMetadataWithToken("localhost:8080", "my_token").metadataSource

    assert(result.isInstanceOf[OpenMetadataSource])
    assert(result.asInstanceOf[OpenMetadataSource].connectionOptions ==
      Map(OPEN_METADATA_HOST -> "localhost:8080", OPEN_METADATA_API_VERSION -> OPEN_METADATA_DEFAULT_API_VERSION,
        OPEN_METADATA_AUTH_TYPE -> OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_JWT_TOKEN -> "my_token"))
  }

  test("Can create OpenMetadata metadata source with basic auth") {
    val result = MetadataSourceBuilder().openMetadata("localhost:8080", OPEN_METADATA_AUTH_TYPE_BASIC,
      Map(OPEN_METADATA_BASIC_AUTH_USERNAME -> "username", OPEN_METADATA_BASIC_AUTH_PASSWORD -> "password")).metadataSource

    assert(result.isInstanceOf[OpenMetadataSource])
    assert(result.asInstanceOf[OpenMetadataSource].connectionOptions ==
      Map(OPEN_METADATA_HOST -> "localhost:8080", OPEN_METADATA_API_VERSION -> OPEN_METADATA_DEFAULT_API_VERSION,
        OPEN_METADATA_AUTH_TYPE -> OPEN_METADATA_AUTH_TYPE_BASIC, OPEN_METADATA_BASIC_AUTH_USERNAME -> "username",
        OPEN_METADATA_BASIC_AUTH_PASSWORD -> "password"))
  }

  test("Can create OpenAPI metadata source") {
    val result = MetadataSourceBuilder().openApi("localhost:8080").metadataSource

    assert(result.isInstanceOf[OpenAPISource])
    assertResult(Map(SCHEMA_LOCATION -> "localhost:8080"))(result.asInstanceOf[OpenAPISource].connectionOptions)
  }

  test("Can create Great Expectations metadata source") {
    val result = MetadataSourceBuilder().greatExpectations("/tmp/expectations").metadataSource

    assert(result.isInstanceOf[GreatExpectationsSource])
    assertResult(Map(GREAT_EXPECTATIONS_FILE -> "/tmp/expectations"))(result.asInstanceOf[GreatExpectationsSource].connectionOptions)
  }

  test("Can create Open Data Contract Standard metadata source") {
    val result = MetadataSourceBuilder().openDataContractStandard("/tmp/odcs").metadataSource

    assert(result.isInstanceOf[OpenDataContractStandardSource])
    assertResult(Map(DATA_CONTRACT_FILE -> "/tmp/odcs"))(result.asInstanceOf[OpenDataContractStandardSource].connectionOptions)
  }

  test("Can create Open Data Contract Standard metadata source with schema name") {
    val result = MetadataSourceBuilder().openDataContractStandard("/tmp/odcs", "accounts").metadataSource

    assert(result.isInstanceOf[OpenDataContractStandardSource])
    assert(result.asInstanceOf[OpenDataContractStandardSource].connectionOptions == Map(
      DATA_CONTRACT_FILE -> "/tmp/odcs",
      DATA_CONTRACT_SCHEMA -> "accounts"
    ))
  }

  test("Can create Data Contract CLI metadata source") {
    val result = MetadataSourceBuilder().dataContractCli("/tmp/datacli").metadataSource

    assert(result.isInstanceOf[DataContractCliSource])
    assertResult(Map(DATA_CONTRACT_FILE -> "/tmp/datacli"))(result.asInstanceOf[DataContractCliSource].connectionOptions)
  }

  test("Can create Data Contract CLI metadata source with schema name") {
    val result = MetadataSourceBuilder().dataContractCli("/tmp/datacli", "accounts").metadataSource

    assert(result.isInstanceOf[DataContractCliSource])
    assert(result.asInstanceOf[DataContractCliSource].connectionOptions == Map(
      DATA_CONTRACT_FILE -> "/tmp/datacli",
      DATA_CONTRACT_SCHEMA -> "accounts"
    ))
  }

  test("Can create Data Contract CLI metadata source with multiple schema names") {
    val result = MetadataSourceBuilder().dataContractCli("/tmp/datacli", List("accounts", "balances")).metadataSource

    assert(result.isInstanceOf[DataContractCliSource])
    assert(result.asInstanceOf[DataContractCliSource].connectionOptions == Map(
      DATA_CONTRACT_FILE -> "/tmp/datacli",
      DATA_CONTRACT_SCHEMA -> "accounts,balances"
    ))
  }

  test("Can create Confluent Schema Registry metadata source with schema ID") {
    val result = MetadataSourceBuilder().confluentSchemaRegistry("localhost:8081", 1).metadataSource

    assert(result.isInstanceOf[ConfluentSchemaRegistrySource])
    assert(result.asInstanceOf[ConfluentSchemaRegistrySource].connectionOptions == Map(
      METADATA_SOURCE_URL -> "localhost:8081",
      CONFLUENT_SCHEMA_REGISTRY_ID -> "1"
    ))
  }

  test("Can create Confluent Schema Registry metadata source with schema subject") {
    val result = MetadataSourceBuilder().confluentSchemaRegistry("localhost:8081", "my-proto").metadataSource

    assert(result.isInstanceOf[ConfluentSchemaRegistrySource])
    assert(result.asInstanceOf[ConfluentSchemaRegistrySource].connectionOptions == Map(
      METADATA_SOURCE_URL -> "localhost:8081",
      CONFLUENT_SCHEMA_REGISTRY_SUBJECT -> "my-proto"
    ))
  }

  test("Can create Confluent Schema Registry metadata source with schema subject and version") {
    val result = MetadataSourceBuilder().confluentSchemaRegistry("localhost:8081", "my-proto", 2).metadataSource

    assert(result.isInstanceOf[ConfluentSchemaRegistrySource])
    assert(result.asInstanceOf[ConfluentSchemaRegistrySource].connectionOptions == Map(
      METADATA_SOURCE_URL -> "localhost:8081",
      CONFLUENT_SCHEMA_REGISTRY_SUBJECT -> "my-proto",
      CONFLUENT_SCHEMA_REGISTRY_VERSION -> "2"
    ))
  }

  test("Can create JSON Schema metadata source") {
    val result = MetadataSourceBuilder().jsonSchema("/tmp/user-schema.json").metadataSource

    assert(result.isInstanceOf[JsonSchemaSource])
    assert(result.asInstanceOf[JsonSchemaSource].connectionOptions == Map(
      JSON_SCHEMA_FILE -> "/tmp/user-schema.json"
    ))
  }

  test("Can create JSON Schema metadata source with options") {
    val result = MetadataSourceBuilder().jsonSchema("/tmp/user-schema.json", Map("strictValidation" -> "true")).metadataSource

    assert(result.isInstanceOf[JsonSchemaSource])
    assert(result.asInstanceOf[JsonSchemaSource].connectionOptions == Map(
      JSON_SCHEMA_FILE -> "/tmp/user-schema.json",
      "strictValidation" -> "true"
    ))
  }

}
