package io.github.datacatering.datacaterer.core.generator.metadata.datasource.file

import io.github.datacatering.datacaterer.core.util.SparkSuite

class FileMetadataTest extends SparkSuite {
  private val baseFolder = getClass.getResource("/sample/files").getPath

  test("Can get all distinct folder pathways for csv file type") {
    val fileMetadata = FileMetadata("csv_data", "csv", Map("path" -> s"$baseFolder/csv"))

    val result = fileMetadata.getSubDataSourcesMetadata

    assertResult(2)(result.length)
    assert(result.forall(m => m.readOptions("format") == "csv"))
    assert(result.forall(m => m.readOptions("path").contains(s"$baseFolder/csv/account")
      || m.readOptions("path").contains(s"$baseFolder/csv/transactions")))
  }

  test("Can get all distinct folder pathways for parquet file type") {
    val fileMetadata = FileMetadata("parquet_data", "parquet", Map("path" -> s"$baseFolder/parquet"))

    val result = fileMetadata.getSubDataSourcesMetadata

    assertResult(3)(result.length)
    assert(result.forall(m => m.readOptions("format") == "parquet"))
    assert(result.forall(m =>
      m.readOptions("path").contains(s"$baseFolder/parquet/account") ||
        m.readOptions("path").contains(s"$baseFolder/parquet/customer") ||
        m.readOptions("path").contains(s"$baseFolder/parquet/transactions")))
  }

  test("Can get all distinct folder pathways for json file type") {
    val fileMetadata = FileMetadata("json_data", "json", Map("path" -> s"$baseFolder/json"))

    val result = fileMetadata.getSubDataSourcesMetadata

    assertResult(1)(result.length)
    assert(result.forall(m => m.readOptions("format") == "json"))
    assert(result.forall(m => m.readOptions("path").contains(s"$baseFolder/json")))
  }

  test("Can get json files from nested csv/json folder when searching specifically there") {
    val fileMetadata = FileMetadata("json_data", "json", Map("path" -> s"$baseFolder/csv/json"))

    val result = fileMetadata.getSubDataSourcesMetadata

    assertResult(1)(result.length)
    assert(result.forall(m => m.readOptions("format") == "json"))
    assert(result.forall(m => m.readOptions("path").contains(s"$baseFolder/csv/json")))
  }

}
