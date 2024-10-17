package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.api.model.Constants.{CLUSTERING_POSITION, DATA_CONTRACT_FILE, ENABLED_NULL, FIELD_DATA_TYPE, FORMAT, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, METADATA_IDENTIFIER, PASSWORD, PRIMARY_KEY_POSITION, URL, USERNAME}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OpenDataContractStandardDataSourceMetadataTest extends SparkSuite {

  test("Can convert ODCS file to column metadata") {
    val connectionConfig = Map(DATA_CONTRACT_FILE -> "src/test/resources/sample/metadata/odcs/full-example.odcs.yaml")
    val odcsMetadata = OpenDataContractStandardDataSourceMetadata("odcs", "parquet", connectionConfig)
    val result = odcsMetadata.getSubDataSourcesMetadata

    assertResult(1)(result.length)
    val expectedReadOptions = Map(
      URL -> "localhost:5432",
      USERNAME -> "${env.username}",
      PASSWORD -> "${env.password}",
      FORMAT -> "csv",
    )
    connectionConfig.foreach(kv => assert(result.head.readOptions(kv._1) == kv._2))
    assertResult(true)(result.head.readOptions.contains(METADATA_IDENTIFIER))
    assertResult(true)(result.head.optFieldMetadata.isDefined)
    val resultCols = result.head.optFieldMetadata.get.collect()
    assertResult(3)(resultCols.length)

    assertResult(true)(resultCols.exists(_.field == "txn_ref_dt"))
    val txnDateCol = resultCols.filter(_.field == "txn_ref_dt").head
    val expectedTxnDateMetadata = Map(
      IS_PRIMARY_KEY -> "false",
      IS_NULLABLE -> "false",
      ENABLED_NULL -> "false",
      IS_UNIQUE -> "false",
      PRIMARY_KEY_POSITION -> "-1",
      CLUSTERING_POSITION -> "-1",
      FIELD_DATA_TYPE -> "date"
    )
    assertResult(expectedTxnDateMetadata)(txnDateCol.metadata)

    assertResult(true)(resultCols.exists(_.field == "rcvr_id"))
    val rcvrIdCol = resultCols.filter(_.field == "rcvr_id").head
    val expectedRcvrIdMetadata = Map(
      IS_PRIMARY_KEY -> "true",
      IS_NULLABLE -> "false",
      ENABLED_NULL -> "false",
      IS_UNIQUE -> "false",
      PRIMARY_KEY_POSITION -> "1",
      CLUSTERING_POSITION -> "1",
      FIELD_DATA_TYPE -> "string"
    )
    assertResult(expectedRcvrIdMetadata)(rcvrIdCol.metadata)

    assertResult(true)(resultCols.exists(_.field == "rcvr_cntry_code"))
    val countryCodeCol = resultCols.filter(_.field == "rcvr_cntry_code").head
    val expectedCountryCodeMetadata = Map(
      IS_PRIMARY_KEY -> "false",
      IS_NULLABLE -> "false",
      ENABLED_NULL -> "false",
      IS_UNIQUE -> "false",
      PRIMARY_KEY_POSITION -> "-1",
      CLUSTERING_POSITION -> "-1",
      FIELD_DATA_TYPE -> "string"
    )
    assertResult(expectedCountryCodeMetadata)(countryCodeCol.metadata)
  }

}
