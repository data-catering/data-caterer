package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.{MINIMUM, ONE_OF_GENERATOR, OPEN_METADATA_API_VERSION, OPEN_METADATA_HOST}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceDetail
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.OpenMetadataDataSourceMetadata
import io.github.datacatering.datacaterer.core.util.TaskHelper
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class TaskHelperTest extends AnyFunSuite {

  private val structType = StructType(Array(
    StructField("name", StringType, false, new MetadataBuilder().putString("key", "value").build()),
    StructField("age", IntegerType, false, new MetadataBuilder().putString("key1", "value1").build()),
    StructField("category", StringType, false, new MetadataBuilder().putString("oneOf", "person,dog").build()),
    StructField("customers", StructType(Array(
      StructField("name", StringType),
      StructField("sex", StringType),
    ))),
  ))
  private val openMetadataConf = Map(OPEN_METADATA_HOST -> "localhost:8585", OPEN_METADATA_API_VERSION -> "v1")

  test("Can create task from metadata generated values") {
    val dataSourceMetadata = OpenMetadataDataSourceMetadata("my_json", "json", openMetadataConf)

    val result = TaskHelper.fromMetadata(None, "task_name", "json", List(DataSourceDetail(dataSourceMetadata, Map(), structType, List())))

    assert(result._1.name == "task_name")
    assert(result._1.steps.size == 1)
    assert(result._1.steps.head.`type` == "json")
    assert(result._1.steps.head.schema.fields.isDefined)
    val resFields = result._1.steps.head.schema.fields.get
    assert(resFields.size == 4)
    assert(resFields.exists(_.name == "name"))
    assert(resFields.exists(_.name == "age"))
    assert(resFields.exists(_.name == "category"))
    assert(resFields.exists(_.name == "customers"))
    assert(resFields.find(_.name == "name").get.generator.isDefined)
    assert(resFields.find(_.name == "age").get.generator.isDefined)
    assert(resFields.find(_.name == "category").get.generator.isDefined)
    assert(resFields.find(_.name == "name").get.generator.get.options.exists(x => x._1 == "key" && x._2.toString == "value"))
    assert(resFields.find(_.name == "age").get.generator.get.options.exists(x => x._1 == "key1" && x._2.toString == "value1"))
    assert(resFields.find(_.name == "category").get.generator.get.`type` == ONE_OF_GENERATOR)
    assert(resFields.find(_.name == "category").get.generator.get.options.contains(ONE_OF_GENERATOR))
    assert(resFields.find(_.name == "category").get.generator.get.options.get(ONE_OF_GENERATOR).contains("person,dog"))
    assert(result._2.isEmpty)
  }

  test("Can merge in user defined values with metadata generated values") {
    val userDefinedPlan = new JsonPlanRun
    val dataSourceMetadata = OpenMetadataDataSourceMetadata("my_json", "json", openMetadataConf)

    val result = TaskHelper.fromMetadata(Some(userDefinedPlan), "my_json", "json", List(DataSourceDetail(dataSourceMetadata, Map(), structType, List())))

    val resFields = result._1.steps.head.schema.fields.get
    assert(resFields.find(_.name == "name").get.generator.get.`type` == ONE_OF_GENERATOR)
    assert(resFields.find(_.name == "name").get.generator.get.options(ONE_OF_GENERATOR).isInstanceOf[mutable.WrappedArray[_]])
    assert(resFields.find(_.name == "name").get.generator.get.options(ONE_OF_GENERATOR).asInstanceOf[mutable.WrappedArray[_]] sameElements Array("peter", "john"))
    assert(resFields.find(_.name == "name").get.generator.get.options.exists(x => x._1 == "key" && x._2.toString == "value"))
    assert(resFields.find(_.name == "age").get.generator.get.options.exists(x => x._1 == "key1" && x._2.toString == "value1"))
    assert(resFields.find(_.name == "age").get.generator.get.options.exists(x => x._1 == MINIMUM && x._2 == "18"))
    assert(resFields.find(_.name == "category").get.generator.get.`type` == ONE_OF_GENERATOR)
    assert(resFields.find(_.name == "category").get.generator.get.options.contains(ONE_OF_GENERATOR))
    assert(resFields.find(_.name == "category").get.generator.get.options.get(ONE_OF_GENERATOR).contains("person,dog"))
    assert(resFields.find(_.name == "customers").get.schema.isDefined)
    assert(resFields.find(_.name == "customers").get.schema.get.fields.isDefined)
    assert(resFields.find(_.name == "customers").get.schema.get.fields.get.exists(_.name == "sex"))
    assert(resFields.find(_.name == "customers").get.schema.get.fields.get.find(_.name == "sex").get.generator.isDefined)
    assert(resFields.find(_.name == "customers").get.schema.get.fields.get.find(_.name == "sex").get.generator.get.`type` == ONE_OF_GENERATOR)
    assert(resFields.find(_.name == "customers").get.schema.get.fields.get.find(_.name == "sex").get.generator.get.options(ONE_OF_GENERATOR).isInstanceOf[mutable.WrappedArray[_]])
    assert(resFields.find(_.name == "customers").get.schema.get.fields.get.find(_.name == "sex").get.generator.get.options(ONE_OF_GENERATOR).asInstanceOf[mutable.WrappedArray[_]] sameElements Array("M", "F"))
  }

  class JsonPlanRun extends PlanRun {
    val jsonTask = json("my_json", "/tmp/data/json")
      .schema(metadataSource.openMetadataWithToken("http://localhost:8585/api", "my_token"))
      .schema(
        field.name("name").oneOf("peter", "john"),
        field.name("age").min(18),
        field.name("customers").schema(field.name("sex").oneOf("M", "F"))
      )

    execute(jsonTask)
  }

}
