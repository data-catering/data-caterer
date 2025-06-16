package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import io.github.datacatering.datacaterer.api.model.{ForeignKey, ForeignKeyRelation, SinkOptions}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.SinkOptionsOps
import org.scalatest.funsuite.AnyFunSuite

class PlanImplicitsTest extends AnyFunSuite {

  test("Can map foreign key relations to relationships without field names") {
    val sinkOptions = SinkOptions(foreignKeys =
      List(
        ForeignKey(ForeignKeyRelation("my_postgres", "public.categories", List("id")),
          List(ForeignKeyRelation("my_csv", "account", List("account_id"))), List()
        )
      )
    )
    val result = sinkOptions.foreignKeysWithoutFieldNames

    assertResult(List(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories" -> List(s"my_csv${FOREIGN_KEY_DELIMITER}account")))(result)
  }

}
