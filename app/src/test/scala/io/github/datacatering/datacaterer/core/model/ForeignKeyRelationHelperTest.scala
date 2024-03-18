package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.{FOREIGN_KEY_DELIMITER, FOREIGN_KEY_PLAN_FILE_DELIMITER}
import io.github.datacatering.datacaterer.core.util.ForeignKeyRelationHelper
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ForeignKeyRelationHelperTest extends AnyFunSuite {

  test("Can parse foreign key relation from string") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id")

    assert(result.dataSource == "my_postgres")
    assert(result.step == "public.categories")
    assert(result.columns == List("id"))
  }

  test("Can parse foreign key relation from string with multiple columns") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id,amount,description")

    assert(result.dataSource == "my_postgres")
    assert(result.step == "public.categories")
    assert(result.columns == List("id", "amount", "description"))
  }

  test("Can parse foreign key relation from plan YAML file") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_PLAN_FILE_DELIMITER}account_postgres${FOREIGN_KEY_PLAN_FILE_DELIMITER}id,amount,description")

    assert(result.dataSource == "my_postgres")
    assert(result.step == "account_postgres")
    assert(result.columns == List("id", "amount", "description"))
  }

  test("Throw exception when unable to parse foreign key relation") {
    assertThrows[RuntimeException](ForeignKeyRelationHelper.fromString(s"my_postgres|account_postgres"))
  }

}
