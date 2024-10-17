package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP, JMS, ONE_OF_GENERATOR, RANDOM_GENERATOR, REGEX_GENERATOR, SQL_GENERATOR}
import io.github.datacatering.datacaterer.api.model.{Generator, Step, TaskSummary}
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataGeneratorType
import io.github.datacatering.datacaterer.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import io.github.datacatering.datacaterer.core.model.Constants.{BATCH, REAL_TIME, RECORD_COUNT_GENERATOR_COL}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{LongType, Metadata, StructField, StructType}
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

  def getDataGenerator(optGenerator: Option[Generator], structField: StructField, faker: Faker): DataGenerator[_] = {
    if (optGenerator.isDefined) {
      optGenerator.get.`type` match {
        //TODO: Slightly abusing random data generator giving back correct data type for sql type generated data
        case RANDOM_GENERATOR | SQL_GENERATOR => RandomDataGenerator.getGeneratorForStructField(structField, faker)
        case ONE_OF_GENERATOR => OneOfDataGenerator.getGenerator(structField, faker)
        case REGEX_GENERATOR => RegexDataGenerator.getGenerator(structField, faker)
        case x => throw UnsupportedDataGeneratorType(x)
      }
    } else {
      LOGGER.debug(s"No generator defined, will get type of generator based on field options, field-name=${structField.name}")
      getDataGenerator(structField, faker)
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

  def applySqlExpressions(df: DataFrame, foreignKeyCols: List[String] = List(), isIgnoreForeignColExists: Boolean = true): DataFrame = {
    def getSqlExpr(field: StructField): String = {
      field.dataType match {
        case StructType(fields) =>
          val nestedSqlExpr = fields.map(f => s"'${f.name}', ${getSqlExpr(f.copy(name = s"${field.name}.${f.name}"))}").mkString(",")
          s"NAMED_STRUCT($nestedSqlExpr)"
        case _ =>
          if (field.metadata.contains(SQL_GENERATOR) &&
            (isIgnoreForeignColExists || foreignKeyCols.exists(col => field.metadata.getString(SQL_GENERATOR).contains(col)))) {
            field.metadata.getString(SQL_GENERATOR)
          } else {
            field.name
          }
      }
    }

    val sqlExpressions = df.schema.fields.map(f => s"${getSqlExpr(f)} as ${f.name}")
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

  private def getGeneratedCount(generator: Generator, faker: Faker): Long = {
    val metadata = Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(generator.options))
    val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, LongType, false, metadata)
    getDataGenerator(Some(generator), countStructField, faker).generate.asInstanceOf[Long]
  }

}
