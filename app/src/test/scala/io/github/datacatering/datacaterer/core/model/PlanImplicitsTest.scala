package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import io.github.datacatering.datacaterer.api.model.SinkOptions
import io.github.datacatering.datacaterer.core.util.PlanImplicits.SinkOptionsOps
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanImplicitsTest extends AnyFunSuite {

  test("Can map foreign key relations to relationships without column names") {
    val sinkOptions = SinkOptions(foreignKeys =
      List(
        s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id" ->
          List(s"my_csv${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}account_id")
      )
    )
    val result = sinkOptions.foreignKeysWithoutColumnNames

    assert(result == List(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories" -> List(s"my_csv${FOREIGN_KEY_DELIMITER}account")))
  }

}
