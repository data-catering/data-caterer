package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.SQL_GENERATOR
import io.github.datacatering.datacaterer.api.model.{Field, PerColumnCount, Step}
import io.github.datacatering.datacaterer.core.exception.InvalidStepCountGeneratorConfigurationException
import io.github.datacatering.datacaterer.core.generator.provider.DataGenerator
import io.github.datacatering.datacaterer.core.model.Constants._
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.{applySqlExpressions, getDataGenerator}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import io.github.datacatering.datacaterer.core.util.PlanImplicits.FieldOps
import net.datafaker.Faker
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random


case class Holder(__index_inc: Long)

class DataGeneratorFactory(faker: Faker)(implicit val sparkSession: SparkSession) {

  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  registerSparkFunctions()

  def generateDataForStep(step: Step, dataSourceName: String, startIndex: Long, endIndex: Long): DataFrame = {
    val structFieldsWithDataGenerators = step.schema.fields.map(getStructWithGenerators).getOrElse(List())
    val indexedDf = sparkSession.createDataFrame(Seq.range(startIndex, endIndex).map(Holder))
    generateDataViaSql(structFieldsWithDataGenerators, step, indexedDf)
      .alias(s"$dataSourceName.${step.name}")
  }

  private def generateDataViaSql(dataGenerators: List[DataGenerator[_]], step: Step, indexedDf: DataFrame): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    val genSqlExpression = dataGenerators.map(dg => s"${dg.generateSqlExpressionWrapper} AS `${dg.structField.name}`")
    val df = indexedDf.selectExpr(genSqlExpression: _*)

    val perColDf = step.count.perColumn
      .map(perCol => generateRecordsPerColumn(dataGenerators, step, perCol, df))
      .getOrElse(df)
    if (!perColDf.storageLevel.useMemory) perColDf.cache()

    val dfWithMetadata = attachMetadata(perColDf, structType)
    val dfAllFields = attachMetadata(applySqlExpressions(dfWithMetadata), structType)
    if (!dfAllFields.storageLevel.useMemory) dfAllFields.cache()
    dfAllFields
  }

  def generateData(dataGenerators: List[DataGenerator[_]], step: Step): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    val count = step.count

    val generatedData = if (count.generator.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(count.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(count.generator, countStructField, faker).generate.asInstanceOf[Int].toLong
      (1L to generatedCount).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    } else if (count.records.isDefined) {
      (1L to count.records.get.asInstanceOf[Number].longValue()).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    } else {
      throw new InvalidStepCountGeneratorConfigurationException(step)
    }

    val rddGeneratedData = sparkSession.sparkContext.parallelize(generatedData)
    val df = sparkSession.createDataFrame(rddGeneratedData, structType)

    var dfPerCol = count.perColumn
      .map(perCol => generateRecordsPerColumn(dataGenerators, step, perCol, df))
      .getOrElse(df)
    val sqlGeneratedFields = structType.fields.filter(f => f.metadata.contains(SQL_GENERATOR))
    sqlGeneratedFields.foreach(field => {
      val allFields = structType.fields.filter(_ != field).map(_.name) ++ Array(s"${field.metadata.getString(SQL_GENERATOR)} AS `${field.name}`")
      dfPerCol = dfPerCol.selectExpr(allFields: _*)
    })
    dfPerCol
  }

  private def generateRecordsPerColumn(dataGenerators: List[DataGenerator[_]], step: Step,
                                       perColumnCount: PerColumnCount, df: DataFrame): DataFrame = {
    val fieldsToBeGenerated = dataGenerators.filter(x => !perColumnCount.columnNames.contains(x.structField.name))

    val perColumnRange = if (perColumnCount.generator.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(perColumnCount.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(perColumnCount.generator, countStructField, faker).asInstanceOf[DataGenerator[Int]]
      val numList = generateDataWithSchema(fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT_GENERATED, expr(generatedCount.generateSqlExpressionWrapper))
        .withColumn(PER_COLUMN_COUNT, numList(col(PER_COLUMN_COUNT_GENERATED)))
        .drop(PER_COLUMN_COUNT_GENERATED)
    } else if (perColumnCount.count.isDefined) {
      val numList = generateDataWithSchema(perColumnCount.count.get, fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else {
      throw new InvalidStepCountGeneratorConfigurationException(step)
    }

    val explodeCount = perColumnRange.withColumn(PER_COLUMN_INDEX_COL, explode(col(PER_COLUMN_COUNT)))
      .drop(col(PER_COLUMN_COUNT))
    explodeCount.select(PER_COLUMN_INDEX_COL + ".*", perColumnCount.columnNames: _*)
  }

  private def generateDataWithSchema(dataGenerators: List[DataGenerator[_]]): UserDefinedFunction = {
    udf((sqlGen: Int) => {
      (1L to sqlGen)
        .toList
        .map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    }, ArrayType(StructType(dataGenerators.map(_.structField))))
  }

  private def generateDataWithSchema(count: Long, dataGenerators: List[DataGenerator[_]]): UserDefinedFunction = {
    udf(() => {
      (1L to count)
        .toList
        .map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    }, ArrayType(StructType(dataGenerators.map(_.structField))))
  }

  private def getStructWithGenerators(fields: List[Field]): List[DataGenerator[_]] = {
    fields.map(field => getDataGenerator(field.generator, field.toStructField, faker))
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
    sparkSession.sql(s"""WITH lengths AS (
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
