package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.{ForeignKey, ForeignKeyRelation}
import org.scalatest.funsuite.AnyFunSuite

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
    assertResult(2)(result.foreignKeys.size)
    assert(result.foreignKeys.contains(ForeignKey(ForeignKeyRelation(s"my_postgres", "account", List("account_id")),
      List(ForeignKeyRelation("my_json", "account", List("account_id"))), List())))
    assert(result.foreignKeys.contains(ForeignKey(ForeignKeyRelation("my_postgres", "account", List("customer_number")),
      List(ForeignKeyRelation("my_json", "account", List("customer_number")), ForeignKeyRelation("my_parquet", "transaction", List("cust_num"))), List())))
  }

}
