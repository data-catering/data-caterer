package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.model.Constants.{CLUSTERING_POSITION, DATA_CONTRACT_FILE, ENABLED_NULL, FIELD_DATA_TYPE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, METADATA_IDENTIFIER, PRIMARY_KEY_POSITION}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.SubDataSourceMetadata
import io.github.datacatering.datacaterer.core.util.SparkSuite

class OpenDataContractStandardDataSourceMetadataTest extends SparkSuite {

  test("Can convert ODCS file to field metadata") {
    val connectionConfig = Map(DATA_CONTRACT_FILE -> "src/test/resources/sample/metadata/odcs/full-example.odcs.yaml")
    val odcsMetadata = OpenDataContractStandardDataSourceMetadata("odcs", "parquet", connectionConfig)
    val result = odcsMetadata.getSubDataSourcesMetadata

    validateResult(connectionConfig, result)
  }

  test("Can convert ODCS v3.0.0 file to field metadata") {
    val connectionConfig = Map(DATA_CONTRACT_FILE -> "src/test/resources/sample/metadata/odcs/full-example-v3.odcs.yaml")
    val odcsMetadata = OpenDataContractStandardDataSourceMetadata("odcs", "parquet", connectionConfig)
    val result = odcsMetadata.getSubDataSourcesMetadata

    validateResult(connectionConfig, result, false)
  }

  private def validateResult(
                              connectionConfig: Map[String, String],
                              result: Array[SubDataSourceMetadata],
                              isVersion2: Boolean = true
                            ) = {
    assertResult(1)(result.length)
    connectionConfig.foreach(kv => assert(result.head.readOptions(kv._1) == kv._2))
    assertResult(true)(result.head.readOptions.contains(METADATA_IDENTIFIER))
    assertResult(true)(result.head.optFieldMetadata.isDefined)
    val resultCols = result.head.optFieldMetadata.get.collect()
    assertResult(3)(resultCols.length)

    assertResult(true)(resultCols.exists(_.field == "txn_ref_dt"))
    val txnDateCol = resultCols.filter(_.field == "txn_ref_dt").head
    val txnCluster = if (isVersion2) Map(CLUSTERING_POSITION -> "-1") else Map()
    // v2 uses isNullable directly; v3 inverts required field
    val isNullableValue = if (isVersion2) "false" else "true"
    val expectedTxnDateMetadata = Map(
      IS_PRIMARY_KEY -> "false",
      IS_NULLABLE -> isNullableValue,
      ENABLED_NULL -> isNullableValue,
      IS_UNIQUE -> "false",
      PRIMARY_KEY_POSITION -> "-1",
      FIELD_DATA_TYPE -> "date"
    ) ++ txnCluster
    // In v3, examples and classification are extracted
    val v3AdditionalMetadata = if (!isVersion2) {
      Map(
        "odcsExamples" -> "2022-10-03,2020-01-28",
        "odcsClassification" -> "public"
      )
    } else Map()
    assertResult(expectedTxnDateMetadata ++ v3AdditionalMetadata)(txnDateCol.metadata)

    assertResult(true)(resultCols.exists(_.field == "rcvr_id"))
    val rcvrIdCol = resultCols.filter(_.field == "rcvr_id").head
    val rcvrIdCluster = if (isVersion2) Map(CLUSTERING_POSITION -> "1") else Map()
    // v2 uses isNullable directly; v3 inverts required field
    val expectedRcvrIdMetadata = Map(
      IS_PRIMARY_KEY -> "true",
      IS_NULLABLE -> isNullableValue,
      ENABLED_NULL -> isNullableValue,
      IS_UNIQUE -> "false",
      PRIMARY_KEY_POSITION -> "1",
      FIELD_DATA_TYPE -> "string"
    ) ++ rcvrIdCluster
    // In v3, classification is extracted for rcvr_id
    val v3RcvrIdMetadata = if (!isVersion2) {
      Map("odcsClassification" -> "restricted")
    } else Map()
    assertResult(expectedRcvrIdMetadata ++ v3RcvrIdMetadata)(rcvrIdCol.metadata)

    assertResult(true)(resultCols.exists(_.field == "rcvr_cntry_code"))
    val countryCodeCol = resultCols.filter(_.field == "rcvr_cntry_code").head
    val countryCodeCluster = if (isVersion2) Map(CLUSTERING_POSITION -> "-1") else Map()
    // v2 uses isNullable directly; v3 inverts required field
    val expectedCountryCodeMetadata = Map(
      IS_PRIMARY_KEY -> "false",
      IS_NULLABLE -> isNullableValue,
      ENABLED_NULL -> isNullableValue,
      IS_UNIQUE -> "false",
      PRIMARY_KEY_POSITION -> "-1",
      FIELD_DATA_TYPE -> "string"
    ) ++ countryCodeCluster
    // In v3, classification is extracted for rcvr_cntry_code
    val v3CountryCodeMetadata = if (!isVersion2) {
      Map("odcsClassification" -> "public")
    } else Map()
    assertResult(expectedCountryCodeMetadata ++ v3CountryCodeMetadata)(countryCodeCol.metadata)
  }

  test("Can correctly map required field to nullable metadata") {
    // Test that required=true means NOT nullable, and required=false means nullable
    val connectionConfig = Map(DATA_CONTRACT_FILE -> "src/test/resources/sample/metadata/odcs/nullable-test-v3.odcs.yaml")
    val odcsMetadata = OpenDataContractStandardDataSourceMetadata("odcs", "parquet", connectionConfig)
    val result = odcsMetadata.getSubDataSourcesMetadata

    assertResult(1)(result.length)
    val resultCols = result.head.optFieldMetadata.get.collect()
    assertResult(2)(resultCols.length)

    // Field with required=true should have IS_NULLABLE=false
    val requiredField = resultCols.filter(_.field == "required_field").head
    assertResult("false")(requiredField.metadata(IS_NULLABLE))
    assertResult("false")(requiredField.metadata(ENABLED_NULL))

    // Field with required=false should have IS_NULLABLE=true
    val optionalField = resultCols.filter(_.field == "optional_field").head
    assertResult("true")(optionalField.metadata(IS_NULLABLE))
    assertResult("true")(optionalField.metadata(ENABLED_NULL))
  }
}
