package io.github.datacatering.datacaterer.core.generator.track

import io.github.datacatering.datacaterer.api.model.Constants.{IS_PRIMARY_KEY, PRIMARY_KEY_POSITION}
import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.StepOps
import io.github.datacatering.datacaterer.core.util.SparkSuite

class RecordTrackingProcessorTest extends SparkSuite {

  test("Can get all primary keys in order") {
    val schema = List(
      Field("name", options = Map(IS_PRIMARY_KEY -> "true", PRIMARY_KEY_POSITION -> "2")),
      Field("account_id", options = Map(IS_PRIMARY_KEY -> "true", PRIMARY_KEY_POSITION -> "1")),
      Field("balance", options = Map(IS_PRIMARY_KEY -> "false"))
    )
    val step = Step("create accounts", "jdbc", Count(), Map(), schema)
    val primaryKeys = step.gatherPrimaryKeys
    assertResult(List("account_id", "name"))(primaryKeys)
  }
}
