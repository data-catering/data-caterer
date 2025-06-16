package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.model.Constants.IS_PRIMARY_KEY
import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, GroupByValidation}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.PostgresMetadata
import org.apache.spark.sql.types.{DateType, MetadataBuilder, StringType, StructField}
import org.scalatest.funsuite.AnyFunSuite

class ValidationPredictorTest extends AnyFunSuite {

  test("Can suggest validations from struct fields") {
    val fields = Array(
      StructField("id", StringType, false, new MetadataBuilder().putString(IS_PRIMARY_KEY, "true").build()),
      StructField("open_date", DateType),
      StructField("close_date", DateType)
    )
    val result = ValidationPredictor.suggestValidations(PostgresMetadata("my_postgres", Map()), Map(), fields).map(_.validation)

    assertResult(3)(result.size)
    val expectedExprValidations = List("isNotNull(id)", "DATE(open_date) <= DATE(close_date)")
    result.filter(_.isInstanceOf[ExpressionValidation]).forall(v => expectedExprValidations.contains(v.asInstanceOf[ExpressionValidation].expr))
    result.filter(_.isInstanceOf[GroupByValidation]).foreach(v => {
      val grp = v.asInstanceOf[GroupByValidation]
      assertResult(Seq("id"))(grp.groupByFields)
      assertResult("unique")(grp.aggField)
      assertResult("count")(grp.aggType)
      assertResult("count == 1")(grp.aggExpr)
    })
  }
}