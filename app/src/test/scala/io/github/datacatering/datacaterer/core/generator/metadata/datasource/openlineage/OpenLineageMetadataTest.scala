package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openlineage

import io.github.datacatering.datacaterer.api.model.Constants.{DATA_SOURCE_NAME, FIELD_DATA_TYPE, METADATA_IDENTIFIER, METADATA_SOURCE_URL, OPEN_LINEAGE_NAMESPACE, URI}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.asynchttpclient.netty.NettyResponse
import org.asynchttpclient.{AsyncHttpClient, BoundRequestBuilder, ListenableFuture, Response}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class OpenLineageMetadataTest extends SparkSuite with MockFactory {

  test("Can get list of datasets from Marquez") {
    val mockHttp = mock[AsyncHttpClient]
    val openLineageMetadata = OpenLineageMetadata("my_postgres", "jdbc", Map(METADATA_SOURCE_URL -> "localhost:1001", OPEN_LINEAGE_NAMESPACE -> "food_delivery"), mockHttp)
    val mockBoundRequest = mock[BoundRequestBuilder]
    val mockListenableResponse = mock[ListenableFuture[Response]]
    val mockResponse = mock[NettyResponse]
    val responseBody = Files.readString(Path.of("src/test/resources/sample/metadata/marquez/list_datasets_api_response.json"))
    (mockHttp.prepareGet(_: String)).expects(*).once().returns(mockBoundRequest)
    (() => mockBoundRequest.execute()).expects().once().returns(mockListenableResponse)
    (() => mockListenableResponse.get()).expects().once().returns(mockResponse)
    (() => mockHttp.close()).expects().once()
    (() => mockResponse.getResponseBody).expects().once().returns(responseBody)

    val result = openLineageMetadata.listDatasets("food_delivery")
    openLineageMetadata.close()

    assert(result.totalCount == 1)
    assert(result.datasets.size == 1)
    assert(result.datasets.head.`type` == "DB_TABLE")
  }

  test("Can get dataset from Marquez") {
    val mockHttp = mock[AsyncHttpClient]
    val openLineageMetadata = OpenLineageMetadata("my_postgres", "jdbc", Map(METADATA_SOURCE_URL -> "localhost:1001", OPEN_LINEAGE_NAMESPACE -> "food_delivery"), mockHttp)
    val mockBoundRequest = mock[BoundRequestBuilder]
    val mockListenableResponse = mock[ListenableFuture[Response]]
    val mockResponse = mock[NettyResponse]
    val responseBody = Files.readString(Path.of("src/test/resources/sample/metadata/marquez/get_dataset_api_response.json"))
    (mockHttp.prepareGet(_: String)).expects(*).once().returns(mockBoundRequest)
    (() => mockBoundRequest.execute()).expects().once().returns(mockListenableResponse)
    (() => mockListenableResponse.get()).expects().once().returns(mockResponse)
    (() => mockHttp.close()).expects().once()
    (() => mockResponse.getResponseBody).expects().once().returns(responseBody)

    val result = openLineageMetadata.getDataset("food_delivery", "my_dataset")
    openLineageMetadata.close()

    assert(result.`type` == "DB_TABLE")
    assert(result.fields.size == 4)
  }

  test("Can get additional column metadata from Marquez") {
    val mockHttp = mock[AsyncHttpClient]
    val openLineageMetadata = OpenLineageMetadata("my_postgres", "jdbc", Map(METADATA_SOURCE_URL -> "localhost:1001", OPEN_LINEAGE_NAMESPACE -> "food_delivery"), mockHttp)
    val mockBoundRequest = mock[BoundRequestBuilder]
    val mockListenableResponse = mock[ListenableFuture[Response]]
    val mockResponse = mock[NettyResponse]
    val responseBody = Files.readString(Path.of("src/test/resources/sample/metadata/marquez/list_datasets_api_response.json"))
    (mockHttp.prepareGet(_: String)).expects(*).once().returns(mockBoundRequest)
    (() => mockBoundRequest.execute()).expects().once().returns(mockListenableResponse)
    (() => mockListenableResponse.get()).expects().once().returns(mockResponse)
    (() => mockHttp.close()).expects().once()
    (() => mockResponse.getResponseBody).expects().once().returns(responseBody)

    val result = openLineageMetadata.getSubDataSourcesMetadata.head.optFieldMetadata.get.collect()
    openLineageMetadata.close()

    assert(result.length == 4)
    assert(result.head.dataSourceReadOptions(METADATA_IDENTIFIER) == "food_delivery_public.categories")
    assert(result.head.metadata(DATA_SOURCE_NAME) == "food_delivery_db")
    assert(result.head.metadata(URI) == "postgres://food_delivery:food_delivery@postgres:5432/food_delivery")
    assert(
      List(("id", "integer"), ("name", "string"), ("menu_id", "integer"), ("description", "string"))
        .forall(colName => result.exists(c => c.field == colName._1 && c.metadata(FIELD_DATA_TYPE) == colName._2))
    )
  }

  test("Will throw exception if metadata configurations not defined") {
    assertThrows[IllegalArgumentException](OpenLineageMetadata("my_postgres", "jdbc", Map()))
    assertThrows[IllegalArgumentException](OpenLineageMetadata("my_postgres", "jdbc", Map(METADATA_SOURCE_URL -> "localhost:1001")))
    assertThrows[IllegalArgumentException](OpenLineageMetadata("my_postgres", "jdbc", Map(OPEN_LINEAGE_NAMESPACE -> "food_delivery")))
  }
}
