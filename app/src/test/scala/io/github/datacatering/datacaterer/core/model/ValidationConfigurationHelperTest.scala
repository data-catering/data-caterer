package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, ExpressionValidation, PauseWaitCondition, ValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ValidationBuilder, WaitConditionBuilder}
import org.scalatest.funsuite.AnyFunSuite

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

    assert(result.name == "my_validation")
    assert(result.description == "my_valid_description")
    assert(result.dataSources.size == 3)
    assert(result.dataSources.contains("my_postgres"))
    assert(result.dataSources.contains("my_csv"))
    assert(result.dataSources.contains("my_json"))
    val dsValidations = result.dataSources("my_postgres")
    assert(dsValidations.size == 2)
    val publicCatValid = dsValidations.find(_.options.get("dbtable").contains("public.categories")).get
    assert(publicCatValid.waitCondition.isInstanceOf[PauseWaitCondition])
    assert(publicCatValid.waitCondition.asInstanceOf[PauseWaitCondition].pauseInSeconds == 5)
    assert(publicCatValid.validations.size == 3)
    val expectedPublicCatValid = List("age > 0", "DATE(open_date) <= DATE(close_date)")
    publicCatValid.validations.map(_.validation)
      .filter(_.isInstanceOf[ExpressionValidation])
      .forall(valid => expectedPublicCatValid.contains(valid.asInstanceOf[ExpressionValidation].expr))
    val publicOrdValid = dsValidations.find(_.options.get("dbtable").contains("public.orders")).get
    assert(publicOrdValid.validations.head.validation.asInstanceOf[ExpressionValidation].expr == "TIMESTAMP(order_ts) <= TIMESTAMP(deliver_ts)")
  }

}
