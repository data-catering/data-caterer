package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, ExpressionValidation, PauseWaitCondition, ValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ValidationBuilder, WaitConditionBuilder}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidationConfigurationHelperTest extends AnyFunSuite {

  test("Can merge validation config from user and generated validation config") {
    val userValidations = List(
      ValidationConfiguration(
        "my_validation", "my_valid_description",
        Map("my_postgres" -> List(
          DataSourceValidation(
            Map("dbtable" -> "public.categories"),
            WaitConditionBuilder().pause(5).waitCondition,
            List(
              ValidationBuilder().expr("age > 0"),
              ValidationBuilder().unique("account_id"),
            ))),
          "my_csv" -> List(
          DataSourceValidation(
            Map("path" -> "/tmp/csv"),
            WaitConditionBuilder().file("/tmp/csv").waitCondition,
            List(
              ValidationBuilder().expr("amount > 0"),
            )))
        )
      )
    )
    val generatedValidations = ValidationConfiguration(
      "gen_valid", "gen_desc",
      Map("my_postgres" -> List(
        DataSourceValidation(
          Map("dbtable" -> "public.categories"),
          validations =
            List(
              ValidationBuilder().expr("DATE(open_date) <= DATE(close_date)")
            )
        ),
        DataSourceValidation(
          Map("dbtable" -> "public.orders"),
          validations =
            List(
              ValidationBuilder().expr("TIMESTAMP(order_ts) <= TIMESTAMP(deliver_ts)")
            )
        )
      ),
      "my_json" -> List(
          DataSourceValidation(
            Map("path" -> "/tmp/json"),
            validations = List(
              ValidationBuilder().expr("isNotNull(id)")
            )
          )
        ))
    )

    val result = ValidationConfigurationHelper.merge(userValidations, generatedValidations)

    assertResult("my_validation")(result.name)
    assertResult("my_valid_description")(result.description)
    assertResult(3)(result.dataSources.size)
    assert(result.dataSources.contains("my_postgres"))
    assert(result.dataSources.contains("my_csv"))
    assert(result.dataSources.contains("my_json"))
    val dsValidations = result.dataSources("my_postgres")
    assertResult(2)(dsValidations.size)
    val publicCatValid = dsValidations.find(_.options.get("dbtable").contains("public.categories")).get
    assert(publicCatValid.waitCondition.isInstanceOf[PauseWaitCondition])
    assertResult(5)(publicCatValid.waitCondition.asInstanceOf[PauseWaitCondition].pauseInSeconds)
    assertResult(3)(publicCatValid.validations.size)
    val expectedPublicCatValid = List("age > 0", "DATE(open_date) <= DATE(close_date)")
    publicCatValid.validations.map(_.validation)
      .filter(_.isInstanceOf[ExpressionValidation])
      .forall(valid => expectedPublicCatValid.contains(valid.asInstanceOf[ExpressionValidation].expr))
    val publicOrdValid = dsValidations.find(_.options.get("dbtable").contains("public.orders")).get
    assertResult("TIMESTAMP(order_ts) <= TIMESTAMP(deliver_ts)")(publicOrdValid.validations.head.validation.asInstanceOf[ExpressionValidation].expr)
  }

}
