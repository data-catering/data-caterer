package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.model.Constants.{DELTA, DELTA_LAKE_SPARK_CONF, FORMAT, ICEBERG, ICEBERG_SPARK_CONF, PATH, TABLE}
import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, FoldersConfig, ValidationConfig, ValidationConfigResult, ValidationConfiguration}
import io.github.datacatering.datacaterer.api.{PreFilterBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.util.{SparkSuite, Transaction}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.sql.Date
import scala.reflect.io.Directory

@RunWith(classOf[JUnitRunner])
class ValidationProcessorTest extends SparkSuite {

  private val sampleData = Seq(
    Transaction("acc123", "peter", "txn1", Date.valueOf("2020-01-01"), 10.0),
    Transaction("acc123", "peter", "txn2", Date.valueOf("2020-01-01"), 50.0),
    Transaction("acc123", "peter", "txn3", Date.valueOf("2020-01-02"), 200.0),
    Transaction("acc123", "peter", "txn4", Date.valueOf("2020-01-03"), 500.0)
  )
  private val df = sparkSession.createDataFrame(sampleData)

  test("Can pre-filter data before running validation") {
    val validation = ValidationBuilder()
      .preFilter(PreFilterBuilder().filter(ValidationBuilder().field("transaction_id").in("txn3", "txn4")))
      .field("amount").greaterThan(100)
    val validationProcessor = new ValidationProcessor(Map(), None, ValidationConfig(), FoldersConfig())
    val result = validationProcessor.tryValidate(df, validation)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assertResult(2)(result.head.total)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can pre-filter data with multiple conditions before running validation") {
    val validation = ValidationBuilder()
      .preFilter(PreFilterBuilder()
        .filter(ValidationBuilder().field("transaction_id").in("txn3", "txn4"))
        .and(ValidationBuilder().field("account_id").isEqual("acc123"))
        .and(ValidationBuilder().field("name").isEqual("peter"))
      )
      .field("amount").greaterThan(100)
    val validationProcessor = new ValidationProcessor(Map(), None, ValidationConfig(), FoldersConfig())
    val result = validationProcessor.tryValidate(df, validation)

    assertResult(1)(result.size)
    assert(result.head.isSuccess)
    assertResult(2)(result.head.total)
    assert(result.head.sampleErrorValues.isEmpty)
  }

  test("Can read Iceberg data for validation") {
    ICEBERG_SPARK_CONF.foreach(conf => df.sqlContext.setConf(conf._1, conf._2))
    df.writeTo("tmp.transactions").using("iceberg").createOrReplace()
    val validationProcessor = setupValidationProcessor(Map(FORMAT -> ICEBERG, TABLE -> "local.tmp.transactions"))
    val result = validationProcessor.executeValidations
    validateResult(result)
  }

  test("Can read Delta Lake data for validation") {
    val path = "/tmp/delta-validation-test"
    new Directory(new File(path)).deleteRecursively()
    DELTA_LAKE_SPARK_CONF.foreach(conf => df.sqlContext.setConf(conf._1, conf._2))
    df.write.format("delta").mode("overwrite").save(path)
    val validationProcessor = setupValidationProcessor(Map(FORMAT -> DELTA, PATH -> path))
    val result = validationProcessor.executeValidations
    validateResult(result)
  }

  test("Can read validations from YAML file") {
    val path = "/tmp/yaml-validation-json-test"
    new Directory(new File(path)).deleteRecursively()
    df.write.format("json").mode("overwrite").save(path)
    val validationProcessor = new ValidationProcessor(
      Map("json" -> Map(FORMAT -> "json")),
      None,
      ValidationConfig(),
      FoldersConfig(validationFolderPath = "src/test/resources/sample/validation/json")
    )
    validationProcessor.executeValidations
  }

  private def setupValidationProcessor(connectingConfig: Map[String, String]): ValidationProcessor = {
    val connectionConfig = Map("test_connection" -> connectingConfig)
    val validationConfig = ValidationConfiguration(dataSources =
      Map("test_connection" ->
        List(DataSourceValidation(
          options = connectionConfig.head._2,
          validations = List(ValidationBuilder().field("transaction_id").startsWith("txn"))
        ))
      )
    )
    new ValidationProcessor(connectionConfig, Some(List(validationConfig)), ValidationConfig(), FoldersConfig())
  }

  private def validateResult(result: List[ValidationConfigResult]): Unit = {
    assertResult(1)(result.size)
    assertResult(1)(result.head.dataSourceValidationResults.size)
    assertResult(1)(result.head.dataSourceValidationResults.head.validationResults.size)
    val resultValidation = result.head.dataSourceValidationResults.head.validationResults.head
    assert(resultValidation.isSuccess)
    assert(resultValidation.sampleErrorValues.isEmpty)
    assertResult(4)(resultValidation.total)
  }
}


