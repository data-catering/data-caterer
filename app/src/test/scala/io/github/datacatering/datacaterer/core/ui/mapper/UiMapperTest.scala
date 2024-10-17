package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.ui.model.{ConfigurationRequest, DataSourceRequest, PlanRunRequest}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UiMapperTest extends AnyFunSuite {

  test("Can convert UI plan run request to plan run") {
    val planRunRequest = PlanRunRequest(
      "plan-name",
      "some-id",
      List(DataSourceRequest("csv", "my-csv", Some(CSV), Some(Map(PATH -> "/tmp/csv")))),
      List(),
      Some(ConfigurationRequest())
    )
    val res = UiMapper.mapToPlanRun(planRunRequest, "/tmp/my-install")._plan
    assertResult("plan-name")(res.name)
    assertResult(Some("some-id"))(res.runId)
    assertResult(1)(res.tasks.size)
    assertResult(true)(res.sinkOptions.isEmpty)
    assertResult(0)(res.validations.size)
  }
}
