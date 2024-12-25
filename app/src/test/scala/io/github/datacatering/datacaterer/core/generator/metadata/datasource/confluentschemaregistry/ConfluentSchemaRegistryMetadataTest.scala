package io.github.datacatering.datacaterer.core.generator.metadata.datasource.confluentschemaregistry

import io.github.datacatering.datacaterer.api.model.Constants.{CONFLUENT_SCHEMA_REGISTRY_ID, CONFLUENT_SCHEMA_REGISTRY_SUBJECT, CONFLUENT_SCHEMA_REGISTRY_VERSION, FIELD_DATA_TYPE, METADATA_IDENTIFIER, METADATA_SOURCE_URL}
import io.github.datacatering.datacaterer.core.exception.InvalidConfluentSchemaRegistrySchemaRequestException
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.SubDataSourceMetadata
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.asynchttpclient.{AsyncHttpClient, BoundRequestBuilder, ListenableFuture, Response}
import org.asynchttpclient.netty.NettyResponse
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class ConfluentSchemaRegistryMetadataTest extends SparkSuite with MockFactory {

  test("Can get protobuf schema from Confluent Schema Registry by schema id") {
    val mockHttp = mock[AsyncHttpClient]
    val confluentSchemaRegistryMetadata = ConfluentSchemaRegistryMetadata(
      "my-kafka",
      "protobuf",
      Map(METADATA_SOURCE_URL -> "localhost:8081", CONFLUENT_SCHEMA_REGISTRY_ID -> "1"),
      mockHttp
    )
    setupMock(mockHttp, "src/test/resources/sample/metadata/confluentschemaregistry/get-my-import.json")

    val result = confluentSchemaRegistryMetadata.getSubDataSourcesMetadata
    confluentSchemaRegistryMetadata.close()

    validateMyImport(result)
  }

  test("Can get protobuf schema from Confluent Schema Registry by schema subject and version") {
    val mockHttp = mock[AsyncHttpClient]
    val confluentSchemaRegistryMetadata = ConfluentSchemaRegistryMetadata(
      "my-kafka",
      "protobuf",
      Map(METADATA_SOURCE_URL -> "localhost:8081", CONFLUENT_SCHEMA_REGISTRY_SUBJECT -> "my-import", CONFLUENT_SCHEMA_REGISTRY_VERSION -> "1"),
      mockHttp
    )
    setupMock(mockHttp, "src/test/resources/sample/metadata/confluentschemaregistry/get-my-import-by-subject.json")

    val result = confluentSchemaRegistryMetadata.getSubDataSourcesMetadata
    confluentSchemaRegistryMetadata.close()

    validateMyImport(result)
  }

  test("Can get protobuf schema from Confluent Schema Registry by schema subject") {
    val mockHttp = mock[AsyncHttpClient]
    val confluentSchemaRegistryMetadata = ConfluentSchemaRegistryMetadata(
      "my-kafka",
      "protobuf",
      Map(METADATA_SOURCE_URL -> "localhost:8081", CONFLUENT_SCHEMA_REGISTRY_SUBJECT -> "my-import"),
      mockHttp
    )
    setupMock(mockHttp, "src/test/resources/sample/metadata/confluentschemaregistry/get-my-import-by-subject.json")

    val result = confluentSchemaRegistryMetadata.getSubDataSourcesMetadata
    confluentSchemaRegistryMetadata.close()

    validateMyImport(result)
  }

  test("Throw exception when only schema version is provided for Confluent Schema Registry") {
    val mockHttp = mock[AsyncHttpClient]
    val confluentSchemaRegistryMetadata = ConfluentSchemaRegistryMetadata(
      "my-kafka",
      "protobuf",
      Map(METADATA_SOURCE_URL -> "localhost:8081", CONFLUENT_SCHEMA_REGISTRY_VERSION -> "1"),
      mockHttp
    )

    assertThrows[InvalidConfluentSchemaRegistrySchemaRequestException](confluentSchemaRegistryMetadata.getSubDataSourcesMetadata)
  }

  test("Throw exception when no schema information is provided for Confluent Schema Registry") {
    val mockHttp = mock[AsyncHttpClient]
    val confluentSchemaRegistryMetadata = ConfluentSchemaRegistryMetadata(
      "my-kafka",
      "protobuf",
      Map(METADATA_SOURCE_URL -> "localhost:8081"),
      mockHttp
    )

    assertThrows[InvalidConfluentSchemaRegistrySchemaRequestException](confluentSchemaRegistryMetadata.getSubDataSourcesMetadata)
  }

  private def validateMyImport(result: Array[SubDataSourceMetadata]) = {
    assertResult(1)(result.length)
    val headMetadata = result.head
    assert(headMetadata.optFieldMetadata.isDefined)
    val fieldMetadata = headMetadata.optFieldMetadata.get.collect()
    assertResult(1)(fieldMetadata.length)
    assertResult("import")(fieldMetadata.head.field)
    assert(fieldMetadata.head.dataSourceReadOptions.contains(METADATA_IDENTIFIER))
    assertResult(Map(FIELD_DATA_TYPE -> "string"))(fieldMetadata.head.metadata)
    assert(fieldMetadata.head.nestedFields.isEmpty)
  }

  private def setupMock(mockHttp: AsyncHttpClient, responseFile: String) = {
    val mockBoundRequest = mock[BoundRequestBuilder]
    val mockListenableResponse = mock[ListenableFuture[Response]]
    val mockResponse = mock[Response]
    val responseBody = Files.readString(Path.of(responseFile))
    (mockHttp.prepareGet(_: String)).expects(*).once().returns(mockBoundRequest)
    (() => mockBoundRequest.execute()).expects().once().returns(mockListenableResponse)
    (() => mockListenableResponse.get()).expects().once().returns(mockResponse)
    (() => mockResponse.getStatusCode).expects().once().returns(200)
    (() => mockHttp.close()).expects().once()
    (() => mockResponse.getResponseBody).expects().once().returns(responseBody)
  }
}
