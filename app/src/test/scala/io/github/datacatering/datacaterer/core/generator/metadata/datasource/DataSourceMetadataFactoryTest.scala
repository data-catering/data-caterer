package io.github.datacatering.datacaterer.core.generator.metadata.datasource

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.core.util.SparkSuite

class DataSourceMetadataFactoryTest extends SparkSuite {

  class ManualGenerationPlan extends PlanRun {
    val myJson = json("my_json", "/tmp/my_json")
      .fields(
        field.name("account_id"),
        field.name("name"),
      )
      .validations(metadataSource.greatExpectations("src/test/resources/sample/validation/great-expectations/taxi-expectations.json"))
      .validations(validation.field("account_id").isNull(true))

    val conf = configuration
      .enableGenerateValidations(true)
      .generatedReportsFolderPath("/tmp/report")

    execute(conf, myJson)
  }

  class ODCSPlan extends PlanRun {
    val myJson = json("my_json", "/tmp/my_json", Map("saveMode" -> "overwrite"))
      .fields(metadataSource.openDataContractStandard("src/test/resources/sample/metadata/odcs/full-example.odcs.yaml"))
      .fields(field.name("rcvr_cntry_code").oneOf("AUS", "FRA"))
      .validations(
        validation.count().isEqual(100),
        validation.field("rcvr_cntry_code").in("AUS", "FRA")
      )
      .count(count.records(100))

    val conf = configuration.enableGeneratePlanAndTasks(true)
      .enableGenerateValidations(true)
      .generatedReportsFolderPath("/tmp/report")

    execute(conf, myJson)
  }

  test("Can merge manual and auto plan generation") {
    // manual data generation with auto validation
    val plan = new ManualGenerationPlan()
    val result = new DataSourceMetadataFactory(plan.conf.build).extractAllDataSourceMetadata(plan)

    assert(result.nonEmpty)
    assertResult(1)(result.get._2.size)
    assertResult(1)(result.get._2.head.steps.size)
    assert(result.get._2.head.steps.head.fields.nonEmpty)
    assertResult(2)(result.get._2.head.steps.head.fields.size)
    assertResult(1)(result.get._3.size)
    assertResult(1)(result.get._3.head.dataSources.size)
    assertResult(1)(result.get._3.head.dataSources.head._2.size)
    assertResult(12)(result.get._3.head.dataSources.head._2.head.validations.size)
  }

  test("Can merge manual and auto plan generation when using ODCS") {
    val plan = new ODCSPlan()
    val result = new DataSourceMetadataFactory(plan.conf.build).extractAllDataSourceMetadata(plan)

    assert(result.nonEmpty)
    assertResult(1)(result.get._2.size)
    assertResult(1)(result.get._2.head.steps.size)
    assert(result.get._2.head.steps.head.fields.nonEmpty)
    assertResult(3)(result.get._2.head.steps.head.fields.size)
    assertResult(1)(result.get._3.size)
    assertResult(1)(result.get._3.head.dataSources.size)
    assertResult(1)(result.get._3.head.dataSources.head._2.size)
    assertResult(4)(result.get._3.head.dataSources.head._2.head.validations.size)
  }
}
