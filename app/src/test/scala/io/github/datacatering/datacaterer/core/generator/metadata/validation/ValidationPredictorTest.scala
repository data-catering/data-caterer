package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.model.Constants.IS_PRIMARY_KEY
import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, GroupByValidation}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.database.PostgresMetadata
import org.apache.spark.sql.types.{DateType, MetadataBuilder, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidationPredictorTest extends AnyFunSuite {

  test("Can suggest validations from struct fields") {
    val fields = Array(
      StructField("id", StringType, false, new MetadataBuilder().putString(IS_PRIMARY_KEY, "true").build()),
      StructField("open_date", DateType),
      StructField("close_date", DateType)
    )
    val result = ValidationPredictor.suggestValidations(PostgresMetadata("my_postgres", Map()), Map(), fields).map(_.validation)

    assert(result.size == 3)
    val expectedExprValidations = List("isNotNull(id)", "DATE(open_date) <= DATE(close_date)")
    result.filter(_.isInstanceOf[ExpressionValidation]).forall(v => expectedExprValidations.contains(v.asInstanceOf[ExpressionValidation].expr))
    result.filter(_.isInstanceOf[GroupByValidation]).foreach(v => {
      val grp = v.asInstanceOf[GroupByValidation]
      assert(grp.groupByCols == Seq("id"))
      assert(grp.aggCol == "unique")
      assert(grp.aggType == "count")
      assert(grp.aggExpr == "count == 1")
    })
  }

}
