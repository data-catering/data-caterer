package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP, JMS, ONE_OF_GENERATOR, REGEX_GENERATOR, SQL_GENERATOR}
import io.github.datacatering.datacaterer.api.model.{Step, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import io.github.datacatering.datacaterer.core.model.Constants.{BATCH, REAL_TIME}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object GeneratorUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getDataGenerator(structField: StructField, faker: Faker): DataGenerator[_] = {
    val hasRegex = structField.metadata.contains(REGEX_GENERATOR)
    val hasOneOf = structField.metadata.contains(ONE_OF_GENERATOR)
    (hasRegex, hasOneOf) match {
      case (true, _) => RegexDataGenerator.getGenerator(structField, faker)
      case (_, true) => OneOfDataGenerator.getGenerator(structField, faker)
      case _ => RandomDataGenerator.getGeneratorForStructField(structField, faker)
    }
  }

  def getDataGenerator(generatorOpts: Map[String, Any], structField: StructField, faker: Faker): DataGenerator[_] = {
    if (generatorOpts.contains(ONE_OF_GENERATOR)) {
      OneOfDataGenerator.getGenerator(structField, faker)
    } else if (generatorOpts.contains(REGEX_GENERATOR)) {
      RegexDataGenerator.getGenerator(structField, faker)
    } else {
      RandomDataGenerator.getGeneratorForStructField(structField, faker)
    }
  }

  def zipWithIndex(df: DataFrame, colName: String): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(ln._1.toSeq ++ Seq(ln._2))
      ),
      StructType(
        df.schema.fields ++ Array(StructField(colName, LongType, false))
      )
    )
  }

  def getDataSourceName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.dataSourceName}.${step.name}"
  }

  def applySqlExpressions(df: DataFrame, foreignKeyFields: List[String] = List(), isIgnoreForeignColExists: Boolean = true): DataFrame = {
    def getSqlExpr(field: StructField): String = {
      field.dataType match {
        case StructType(fields) =>
          val nestedSqlExpr = fields.map(f => s"'${f.name}', ${getSqlExpr(f.copy(name = s"${field.name}.${f.name}"))}").mkString(",")
          s"NAMED_STRUCT($nestedSqlExpr)"
        case _ =>
          if (field.metadata.contains(SQL_GENERATOR) &&
            (isIgnoreForeignColExists || foreignKeyFields.exists(col => field.metadata.getString(SQL_GENERATOR).contains(col)))) {
            field.metadata.getString(SQL_GENERATOR)
          } else {
            // Escape field names that contain dots
            if (field.name.contains(".")) s"${field.name}" else field.name
          }
      }
    }

    val sqlExpressions = df.schema.fields.map(f => {
      val columnAlias = if (f.name.contains(".")) s"`${f.name}`" else f.name
      s"${getSqlExpr(f)} as $columnAlias"
    })
    val res = df.selectExpr(sqlExpressions: _*)
      .selectExpr(sqlExpressions: _*) //fix for nested SQL references but I don't think it would work longer term
    //TODO have to figure out the order of the SQL expressions and execute accordingly
    res
  }

  def determineSaveTiming(dataSourceName: String, format: String, stepName: String): String = {
    format match {
      case HTTP | JMS =>
        LOGGER.debug(s"Given the step type is either HTTP or JMS, data will be generated in real-time mode. " +
          s"It will be based on requests per second defined at plan level, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        REAL_TIME
      case _ =>
        LOGGER.debug(s"Will generate data in batch mode for step, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        BATCH
    }
  }

}
