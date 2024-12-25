package io.github.datacatering.datacaterer.core.generator.metadata.datasource.datacontractcli

import io.github.datacatering.datacaterer.api.model.Constants.{DATA_CONTRACT_FILE, FIELD_DATA_TYPE, METADATA_IDENTIFIER}
import io.github.datacatering.datacaterer.core.exception.{InvalidDataContractFileFormatException, MissingDataContractFilePathException}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class DataContractCliDataSourceMetadataTest extends SparkSuite {

  test("throw MissingDataContractFilePathException when DATA_CONTRACT_FILE is not defined") {
    val metadata = DataContractCliDataSourceMetadata("test", "test", Map())

    assertThrows[MissingDataContractFilePathException](metadata.getSubDataSourcesMetadata(sparkSession))
  }

  test("throw InvalidDataContractFileFormatException when DATA_CONTRACT_FILE is invalid") {
    val metadata = DataContractCliDataSourceMetadata("test", "test", Map(DATA_CONTRACT_FILE -> "invalid_path.yaml"))

    val exception = intercept[InvalidDataContractFileFormatException] {
      metadata.getSubDataSourcesMetadata(sparkSession)
    }
    assert(exception.getMessage.startsWith("Failed to parse data contract file"))
  }

  test("return RuntimeException when empty contract is given") {
    val tempFile = File.createTempFile("empty_contract", ".yaml")
    tempFile.deleteOnExit()
    val metadata = DataContractCliDataSourceMetadata("test", "test", Map(DATA_CONTRACT_FILE -> tempFile.getAbsolutePath))

    assertThrows[RuntimeException](metadata.getSubDataSourcesMetadata(sparkSession))
  }

  test("return SubDataSourceMetadata for each model in the contract") {
    val tempFile = File.createTempFile("test_contract", ".yaml")
    tempFile.deleteOnExit()
    val yamlContent =
      """dataContractSpecification: 0.9.3
        |id: covid_cases
        |models:
        |  model1:
        |    fields:
        |      field1:
        |        type: string
        |  model2:
        |    fields:
        |      field2:
        |        type: integer
        |""".stripMargin
    java.nio.file.Files.write(tempFile.toPath, yamlContent.getBytes)

    val metadata = DataContractCliDataSourceMetadata("test", "test", Map(DATA_CONTRACT_FILE -> tempFile.getAbsolutePath))

    val result = metadata.getSubDataSourcesMetadata(sparkSession)
    result.length shouldBe 2
    result.map(_.readOptions(METADATA_IDENTIFIER)).toSet shouldBe Set("model1", "model2")
  }

  test("Can convert data contract CLI file to field metadata") {
    val metadata = DataContractCliDataSourceMetadata("test", "test", Map(DATA_CONTRACT_FILE -> "src/test/resources/sample/metadata/datacontractcli/datacontract.yaml"))

    val result = metadata.getSubDataSourcesMetadata(sparkSession)
    assertResult(1)(result.length)
    assert(result.head.optFieldMetadata.isDefined)
    val fields = result.head.optFieldMetadata.get.collect()
    assertResult(9)(fields.length)

    val fipsField = fields.filter(f => f.field == "fips").head
    val expectedFipsMetadata = Map(FIELD_DATA_TYPE -> "string")
    assertResult(expectedFipsMetadata)(fipsField.metadata)

    val lastUpdateField = fields.filter(f => f.field == "last_update").head
    val expectedLastUpdateMetadata = Map(FIELD_DATA_TYPE -> "timestamp")
    assertResult(expectedLastUpdateMetadata)(lastUpdateField.metadata)

    val latitudeField = fields.filter(f => f.field == "latitude").head
    val expectedLatitudeMetadata = Map(FIELD_DATA_TYPE -> "double")
    assertResult(expectedLatitudeMetadata)(latitudeField.metadata)

    val confirmedField = fields.filter(f => f.field == "confirmed").head
    val expectedConfirmedMetadata = Map(FIELD_DATA_TYPE -> "integer")
    assertResult(expectedConfirmedMetadata)(confirmedField.metadata)
  }
}
