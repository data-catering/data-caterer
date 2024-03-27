package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SinkOptionsBuilderTest extends AnyFunSuite {

  test("Can create sink options with random seed, locale and foreign keys") {
    val result = SinkOptionsBuilder()
      .seed(10)
      .locale("id")
      .foreignKey(new ForeignKeyRelation("my_postgres", "account", "account_id"),
        new ForeignKeyRelation("my_json", "account", "account_id"))
      .foreignKey(new ForeignKeyRelation("my_postgres", "account", "customer_number"),
        new ForeignKeyRelation("my_json", "account", "customer_number"),
        new ForeignKeyRelation("my_parquet", "transaction", "cust_num"))
      .sinkOptions

    assert(result.seed.contains("10"))
    assert(result.locale.contains("id"))
    assert(result.foreignKeys.size == 2)
    assert(result.foreignKeys.contains((s"my_postgres${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}account_id",
      List(s"my_json${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}account_id"), List())))
    assert(result.foreignKeys.contains((s"my_postgres${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}customer_number",
      List(s"my_json${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}customer_number", s"my_parquet${FOREIGN_KEY_DELIMITER}transaction${FOREIGN_KEY_DELIMITER}cust_num"), List())))
  }

}
