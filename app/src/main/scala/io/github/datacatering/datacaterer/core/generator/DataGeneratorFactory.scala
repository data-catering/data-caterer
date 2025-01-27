package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{ALL_COMBINATIONS, ONE_OF_GENERATOR, STATIC}
import io.github.datacatering.datacaterer.api.model.{Field, PerFieldCount, Step}
import io.github.datacatering.datacaterer.core.exception.InvalidStepCountGeneratorConfigurationException
import io.github.datacatering.datacaterer.core.generator.provider.DataGenerator
import io.github.datacatering.datacaterer.core.generator.provider.OneOfDataGenerator.RandomOneOfDataGenerator
import io.github.datacatering.datacaterer.core.model.Constants._
import io.github.datacatering.datacaterer.core.sink.SinkProcessor
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.{applySqlExpressions, getDataGenerator}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import io.github.datacatering.datacaterer.core.util.PlanImplicits.FieldOps
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random


case class Holder(__index_inc: Long)

class DataGeneratorFactory(faker: Faker)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  registerSparkFunctions()

  def generateDataForStep(step: Step, dataSourceName: String, startIndex: Long, endIndex: Long): DataFrame = {
    val structFieldsWithDataGenerators = getStructWithGenerators(step.fields)
    val indexedDf = sparkSession.createDataFrame(Seq.range(startIndex, endIndex).map(Holder))
    generateDataViaSql(structFieldsWithDataGenerators, step, indexedDf)
      .alias(s"$dataSourceName.${step.name}")
  }

  private def generateDataViaSql(dataGenerators: List[DataGenerator[_]], step: Step, indexedDf: DataFrame): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    SinkProcessor.validateSchema(step.`type`, structType)

    val allRecordsDf = if (step.options.contains(ALL_COMBINATIONS) && step.options(ALL_COMBINATIONS).equalsIgnoreCase("true")) {
      generateCombinationRecords(dataGenerators, indexedDf)
    } else {
      val genSqlExpression = dataGenerators.map(dg => s"${dg.generateSqlExpressionWrapper} AS `${dg.structField.name}`")
      val df = indexedDf.selectExpr(genSqlExpression: _*)

      if (!df.storageLevel.useMemory) df.cache()
      step.count.perField
        .map(perCol => generateRecordsPerField(dataGenerators, step, perCol, df))
        .getOrElse(df)
    }

    if (!allRecordsDf.storageLevel.useMemory) allRecordsDf.cache()
    val dfWithMetadata = attachMetadata(allRecordsDf, structType)
    val dfAllFields = attachMetadata(applySqlExpressions(dfWithMetadata), structType)
    if (!dfAllFields.storageLevel.useMemory) dfAllFields.cache()
    dfAllFields
  }

  private def generateRecordsPerField(dataGenerators: List[DataGenerator[_]], step: Step,
                                      perFieldCount: PerFieldCount, df: DataFrame): DataFrame = {
    val fieldsToBeGenerated = dataGenerators.filter(x => !perFieldCount.fieldNames.contains(x.structField.name))

    if (fieldsToBeGenerated.isEmpty) {
      LOGGER.warn(s"Per field count defined but no other fields to generate")
      df
    } else {
      val metadata = if (perFieldCount.options.nonEmpty) {
        Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(perFieldCount.options))
      } else if (perFieldCount.count.isDefined) {
        Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(Map(STATIC -> perFieldCount.count.get.toString)))
      } else {
        throw InvalidStepCountGeneratorConfigurationException(step)
      }

      val countStructField = StructField(RECORD_COUNT_GENERATOR_FIELD, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(perFieldCount.options, countStructField, faker).asInstanceOf[DataGenerator[Int]]
      val perFieldRange = generateDataWithSchemaSql(df, generatedCount, fieldsToBeGenerated)

      val explodeCount = perFieldRange.withColumn(PER_FIELD_INDEX_FIELD, explode(col(PER_FIELD_COUNT)))
        .drop(col(PER_FIELD_COUNT))
      explodeCount.select(PER_FIELD_INDEX_FIELD + ".*", perFieldCount.fieldNames: _*)
    }
  }

  private def generateCombinationRecords(dataGenerators: List[DataGenerator[_]], indexedDf: DataFrame) = {
    LOGGER.debug("Attempting to generate all combinations of 'oneOf' fields")
    val oneOfFields = dataGenerators
      .filter(x => x.isInstanceOf[RandomOneOfDataGenerator] || x.options.contains(ONE_OF_GENERATOR))
    val nonOneOfFields = dataGenerators.filter(x => !x.isInstanceOf[RandomOneOfDataGenerator] && !x.options.contains(ONE_OF_GENERATOR))

    val oneOfFieldsSql = oneOfFields.map(field => {
      val fieldValues = field.structField.metadata.getStringArray(ONE_OF_GENERATOR)
      sparkSession.createDataFrame(Seq(1L).map(Holder))
        .selectExpr(explode(typedlit(fieldValues)).as(field.structField.name).expr.sql)
    })
    val nonOneOfFieldsSql = nonOneOfFields.map(dg => s"${dg.generateSqlExpressionWrapper} AS `${dg.structField.name}`")

    if (oneOfFields.nonEmpty) {
      LOGGER.debug("Found fields defined with 'oneOf', attempting to create all combinations of possible values")
      val pairwiseCombinations = oneOfFieldsSql.reduce((a, b) => a.crossJoin(b))
      val selectExpr = pairwiseCombinations.columns.toList ++ nonOneOfFieldsSql
      pairwiseCombinations.selectExpr(selectExpr: _*)
    } else {
      LOGGER.info("No fields defined with 'oneOf', unable to create all possible combinations")
      indexedDf
    }
  }

  private def generateDataWithSchemaSql(df: DataFrame, countGenerator: DataGenerator[_], dataGenerators: List[DataGenerator[_]]): DataFrame = {
    val namedStruct = dataGenerators.map(dg => s"'${dg.structField.name}', CAST(${dg.generateSqlExpressionWrapper} AS ${dg.structField.dataType.sql})").mkString(",")
    val perCountGeneratedExpr = df.columns ++ Array(s"SEQUENCE(1, CAST(${countGenerator.generateSqlExpressionWrapper} AS INT)) AS $PER_FIELD_COUNT_GENERATED")
    val transformPerCountExpr = df.columns ++ Array(s"TRANSFORM($PER_FIELD_COUNT_GENERATED, x -> NAMED_STRUCT($namedStruct)) AS $PER_FIELD_COUNT")

    df.selectExpr(perCountGeneratedExpr: _*)
      .selectExpr(transformPerCountExpr: _*)
      .drop(PER_FIELD_COUNT_GENERATED)
  }

  private def getStructWithGenerators(fields: List[Field]): List[DataGenerator[_]] = {
    fields.map(field => getDataGenerator(field.options, field.toStructField, faker))
  }

  private def registerSparkFunctions(): Unit = {
    sparkSession.udf.register(GENERATE_REGEX_UDF, UDFHelperFunctions.regex(faker))
    sparkSession.udf.register(GENERATE_FAKER_EXPRESSION_UDF, UDFHelperFunctions.expression(faker))
    sparkSession.udf.register(GENERATE_RANDOM_ALPHANUMERIC_STRING_UDF, UDFHelperFunctions.alphaNumeric(faker))
    defineRandomLengthView()
  }

  private def attachMetadata(df: DataFrame, structType: StructType): DataFrame = {
    sparkSession.createDataFrame(df.selectExpr(structType.fieldNames: _*).rdd, structType)
  }

  private def defineRandomLengthView(): Unit = {
    sparkSession.sql(
        s"""WITH lengths AS (
           |  SELECT sequence(1, $DATA_CATERER_RANDOM_LENGTH_MAX_VALUE) AS length_list
           |),
           |
           |-- Explode the sequence into individual length values
           |exploded_lengths AS (
           |  SELECT explode(length_list) AS length
           |  FROM lengths
           |),
           |
           |-- Create the heuristic cumulative distribution dynamically
           |length_distribution AS (
           |  SELECT
           |    length,
           |    CASE
           |      WHEN length <= 5 THEN 0.001 * POWER(2, length - 1)
           |      WHEN length <= 10 THEN 0.01 * POWER(2, length - 6)
           |      ELSE 0.1 * POWER(2, length - 11)
           |    END AS weight,
           |    SUM(CASE
           |          WHEN length <= 5 THEN 0.001 * POWER(2, length - 1)
           |          WHEN length <= 10 THEN 0.01 * POWER(2, length - 6)
           |          ELSE 0.1 * POWER(2, length - 11)
           |        END) OVER (ORDER BY length) AS cumulative_weight,
           |    SUM(CASE
           |          WHEN length <= 5 THEN 0.001 * POWER(2, length - 1)
           |          WHEN length <= 10 THEN 0.01 * POWER(2, length - 6)
           |          ELSE 0.1 * POWER(2, length - 11)
           |        END) OVER () AS total_weight
           |  FROM exploded_lengths
           |),
           |
           |-- Calculate cumulative probabilities
           |length_probabilities AS (
           |  SELECT
           |    length,
           |    cumulative_weight / total_weight AS cumulative_prob
           |  FROM length_distribution
           |),
           |
           |-- Select a single random length based on the heuristic distribution
           |random_length AS (
           |  SELECT
           |    length
           |  FROM length_probabilities
           |  WHERE cumulative_prob >= rand()
           |  ORDER BY cumulative_prob
           |  LIMIT 1
           |)
           |
           |-- Final query to get the single random length
           |SELECT * FROM random_length;""".stripMargin)
      .createOrReplaceTempView(DATA_CATERER_RANDOM_LENGTH)
  }
}

object UDFHelperFunctions extends Serializable {

  private val RANDOM = new Random()

  def regex(faker: Faker): UserDefinedFunction = udf((s: String) => faker.regexify(s)).asNondeterministic()

  def expression(faker: Faker): UserDefinedFunction = udf((s: String) => faker.expression(s)).asNondeterministic()

  def alphaNumeric(faker: Faker): UserDefinedFunction = udf((minLength: Int, maxLength: Int) => {
    val length = RANDOM.nextInt(maxLength + 1) + minLength
    RANDOM.alphanumeric.take(length).mkString("")
  }).asNondeterministic()
}
