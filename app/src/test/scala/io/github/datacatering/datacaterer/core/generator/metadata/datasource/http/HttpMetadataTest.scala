package io.github.datacatering.datacaterer.core.generator.metadata.datasource.http

import io.github.datacatering.datacaterer.api.model.Constants.SCHEMA_LOCATION
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpMetadataTest extends SparkSuite {

  test("Can get all endpoints and schemas from OpenApi spec") {
    val result = HttpMetadata("my_http", "http", Map(SCHEMA_LOCATION -> "src/test/resources/sample/http/openapi/petstore.json"))
      .getSubDataSourcesMetadata

    assertResult(3)(result.length)
  }

}
