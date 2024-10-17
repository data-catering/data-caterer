package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.{FieldBuilder, PlanBuilder}
import io.github.datacatering.datacaterer.api.connection.FileBuilder
import io.github.datacatering.datacaterer.core.ui.model.{ForeignKeyRequest, ForeignKeyRequestItem}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ForeignKeyMapperTest extends AnyFunSuite {

  test("Can convert UI foreign key config") {
    val foreignKeyRequests = List(ForeignKeyRequest(Some(ForeignKeyRequestItem("task-1", "account_id,year")), List(ForeignKeyRequestItem("task-2", "account_number,year"))))
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_number"), FieldBuilder().name("year"))
    )
    val planBuilder = PlanBuilder()
    val res = ForeignKeyMapper.foreignKeyMapping(foreignKeyRequests, connections, planBuilder)
    assertResult(1)(res.plan.sinkOptions.get.foreignKeys.size)
    assert(res.plan.sinkOptions.get.foreignKeys.head._1.startsWith("json"))
    assert(res.plan.sinkOptions.get.foreignKeys.head._1.endsWith("account_id,year"))
    assert(res.plan.sinkOptions.get.foreignKeys.head._2.size == 1)
    assert(res.plan.sinkOptions.get.foreignKeys.head._2.head.startsWith("json"))
    assert(res.plan.sinkOptions.get.foreignKeys.head._2.head.endsWith("account_number,year"))
  }

}
