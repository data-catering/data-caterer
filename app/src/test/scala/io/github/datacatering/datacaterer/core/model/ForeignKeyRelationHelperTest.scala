package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.{FOREIGN_KEY_DELIMITER, FOREIGN_KEY_PLAN_FILE_DELIMITER}
import io.github.datacatering.datacaterer.core.util.ForeignKeyRelationHelper
import org.scalatest.funsuite.AnyFunSuite

class ForeignKeyRelationHelperTest extends AnyFunSuite {

  test("Can parse foreign key relation from string") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id")

    assertResult("my_postgres")(result.dataSource)
    assertResult("public.categories")(result.step)
    assertResult(List("id"))(result.fields)
  }

  test("Can parse foreign key relation from string with multiple fields") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id,amount,description")

    assertResult("my_postgres")(result.dataSource)
    assertResult("public.categories")(result.step)
    assertResult(List("id", "amount", "description"))(result.fields)
  }

  test("Can parse foreign key relation from plan YAML file") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_PLAN_FILE_DELIMITER}account_postgres${FOREIGN_KEY_PLAN_FILE_DELIMITER}id,amount,description")

    assertResult("my_postgres")(result.dataSource)
    assertResult("account_postgres")(result.step)
    assertResult(List("id", "amount", "description"))(result.fields)
  }

  test("Throw exception when unable to parse foreign key relation") {
    assertThrows[RuntimeException](ForeignKeyRelationHelper.fromString(s"my_postgres|account_postgres"))
  }

}
