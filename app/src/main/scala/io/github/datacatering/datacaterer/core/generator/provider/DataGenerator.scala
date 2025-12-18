package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.{ARRAY_MAXIMUM_LENGTH, ARRAY_MINIMUM_LENGTH, ENABLED_EDGE_CASE, ENABLED_NULL, IS_UNIQUE, MAP_MAXIMUM_SIZE, MAP_MINIMUM_SIZE, PROBABILITY_OF_EDGE_CASE, PROBABILITY_OF_NULL, RANDOM_SEED, STATIC}
import io.github.datacatering.datacaterer.api.model.generator.BaseGenerator
import io.github.datacatering.datacaterer.core.exception.ExhaustedUniqueValueGenerationException
import io.github.datacatering.datacaterer.core.model.Constants.DATA_CATERER_RANDOM_LENGTH
import net.datafaker.Faker
import org.apache.spark.sql.functions.{expr, lit, monotonically_increasing_id, rand, when, xxhash64}
import org.apache.spark.sql.types.StructField

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.higherKinds
import scala.util.Random

trait DataGenerator[T] extends BaseGenerator[T] with Serializable {

  val structField: StructField
  val faker: Faker

  lazy val optRandomSeed: Option[Long] = if (structField.metadata.contains(RANDOM_SEED)) Some(structField.metadata.getString(RANDOM_SEED).toLong) else None
  lazy val sqlRandom: String = optRandomSeed.map(seed => s"RAND($seed)").getOrElse("RAND()")
  lazy val random: Random = if (structField.metadata.contains(RANDOM_SEED)) new Random(structField.metadata.getString(RANDOM_SEED).toLong) else new Random()
  lazy val enabledNull: Boolean = if (structField.metadata.contains(ENABLED_NULL)) structField.metadata.getString(ENABLED_NULL).toBoolean else false
  lazy val enabledEdgeCases: Boolean = if (structField.metadata.contains(ENABLED_EDGE_CASE)) structField.metadata.getString(ENABLED_EDGE_CASE).toBoolean else false
  lazy val isUnique: Boolean = if (structField.metadata.contains(IS_UNIQUE)) structField.metadata.getString(IS_UNIQUE).toBoolean else false
  lazy val probabilityOfNull: Double = if (structField.metadata.contains(PROBABILITY_OF_NULL)) structField.metadata.getString(PROBABILITY_OF_NULL).toDouble else 0.1
  lazy val probabilityOfEdgeCases: Double = if (structField.metadata.contains(PROBABILITY_OF_EDGE_CASE)) structField.metadata.getString(PROBABILITY_OF_EDGE_CASE).toDouble else 0.5
  lazy val prevGenerated: mutable.Set[T] = mutable.Set[T]()
  lazy val optStatic: Option[String] = if (structField.metadata.contains(STATIC)) Some(structField.metadata.getString(STATIC)) else None

  def generateSqlExpressionWrapper: String = {
    if (optStatic.isDefined) {
      return s"'${optStatic.get}'"
    }
    val baseSqlExpression = replaceLambdaFunction(generateSqlExpression)
    // Use hash-based approach when seed is provided for deterministic behavior across environments
    // (Spark's rand(seed) is partition-dependent and not truly deterministic)
    // Note: Using xxhash64 with monotonically_increasing_id provides row-level determinism
    val caseRandom = optRandomSeed match {
      case Some(s) =>
        // Hash row ID with seed, normalize to [0, 1)
        val hashExpr = xxhash64(monotonically_increasing_id(), lit(s))
        (hashExpr.bitwiseAND(lit(Long.MaxValue))).cast("double") / lit(Long.MaxValue.toDouble)
      case None => rand()
    }
    val expression = (enabledEdgeCases, enabledNull) match {
      case (true, true) =>
        when(caseRandom.leq(probabilityOfEdgeCases), edgeCases(random.nextInt(edgeCases.size)))
          .when(caseRandom.leq(probabilityOfEdgeCases + probabilityOfNull), lit(null).cast(structField.dataType))
          .otherwise(expr(baseSqlExpression))
          .expr.sql
      case (true, false) =>
        when(caseRandom.leq(probabilityOfEdgeCases), edgeCases(random.nextInt(edgeCases.size)))
          .otherwise(expr(baseSqlExpression))
          .expr.sql
      case (false, true) =>
        when(caseRandom.leq(probabilityOfNull), lit(null).cast(structField.dataType))
          .otherwise(expr(baseSqlExpression))
          .expr.sql
      case _ => baseSqlExpression
    }
    val replaceLambda = replaceLambdaFunction(expression)
    val replaceSubScalar = replaceSubScalarFunction(replaceLambda, baseSqlExpression)
    replaceSubScalar
  }

  def generateWrapper(count: Int = 0): T = {
    val randDouble = random.nextDouble()
    val generatedValue = if (enabledEdgeCases && randDouble <= probabilityOfEdgeCases) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
    if (count > 10) {
      //TODO: logic doesn't work when field is auto_increment, need to be aware if data system automatically takes care of it (therefore, field can be omitted from generation)
      throw ExhaustedUniqueValueGenerationException(count, structField.name, structField.metadata, prevGenerated.take(3).map(_.toString).toList)
    } else if (isUnique) {
      if (prevGenerated.contains(generatedValue)) {
        generateWrapper(count + 1)
      } else {
        prevGenerated.add(generatedValue)
        generatedValue
      }
    } else {
      generatedValue
    }
  }

  private def replaceLambdaFunction(sql: String): String = {
    val lambdaRegex = ".*lambdafunction\\((.+?), i\\).*".r.pattern
    val replaceTargetFn: String => String = r => s"lambdafunction($r, i)"
    val replacementFn: String => String = r => s"i -> $r"
    replaceByRegex(sql, lambdaRegex, replaceTargetFn, replacementFn)
  }

  private def replaceSubScalarFunction(sql: String, originalSql: String): String = {
    val lambdaRegex = ".*scalarsubquery\\((.*?)\\).*".r.pattern
    val replaceTargetFn: String => String = r => s"scalarsubquery()"
    val originalRegex = s".*\\(SELECT CAST\\((.+?) $DATA_CATERER_RANDOM_LENGTH\\).*".r.pattern
    val matcher = originalRegex.matcher(originalSql)
    if (matcher.matches()) {
      val replacementFn: String => String = _ => s"(SELECT CAST(${matcher.group(1)} $DATA_CATERER_RANDOM_LENGTH)"
      replaceByRegex(sql, lambdaRegex, replaceTargetFn, replacementFn)
    } else sql
  }

  @tailrec
  private def replaceByRegex(text: String, pattern: Pattern, replaceTargetFn: String => String, replacementFn: String => String): String = {
    val matcher = pattern.matcher(text)
    if (matcher.matches()) {
      val innerFunction = matcher.group(1)
      val replace = text.replace(replaceTargetFn(innerFunction), replacementFn(innerFunction))
      replaceByRegex(replace, pattern, replaceTargetFn, replacementFn)
    } else text
  }
}

trait NullableDataGenerator[T >: Null] extends DataGenerator[T] {

  override def generateWrapper(count: Int = 0): T = {
    val randDouble = random.nextDouble()
    if (enabledNull && structField.nullable && randDouble <= probabilityOfNull) {
      null
    } else if (enabledEdgeCases && edgeCases.nonEmpty &&
      ((structField.nullable && randDouble <= probabilityOfEdgeCases + probabilityOfNull) ||
        (!structField.nullable && randDouble <= probabilityOfEdgeCases))) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}

trait ArrayDataGenerator[T] extends NullableDataGenerator[List[T]] {

  lazy val arrayMaxSize: Int = if (structField.metadata.contains(ARRAY_MAXIMUM_LENGTH)) structField.metadata.getString(ARRAY_MAXIMUM_LENGTH).toInt else 5
  lazy val arrayMinSize: Int = if (structField.metadata.contains(ARRAY_MINIMUM_LENGTH)) structField.metadata.getString(ARRAY_MINIMUM_LENGTH).toInt else 0

  def elementGenerator: DataGenerator[T]

  override def generate: List[T] = {
    val listSize = (random.nextDouble() * (arrayMaxSize - arrayMinSize) + arrayMinSize).toInt
    (1 to listSize)
      .map(_ => elementGenerator.generate)
      .toList
  }
}

trait MapDataGenerator[T, K] extends NullableDataGenerator[Map[T, K]] {

  lazy val mapMaxSize: Int = if (structField.metadata.contains(MAP_MAXIMUM_SIZE)) structField.metadata.getString(MAP_MAXIMUM_SIZE).toInt else 5
  lazy val mapMinSize: Int = if (structField.metadata.contains(MAP_MINIMUM_SIZE)) structField.metadata.getString(MAP_MINIMUM_SIZE).toInt else 0

  def keyGenerator: DataGenerator[T]

  def valueGenerator: DataGenerator[K]

  override def generate: Map[T, K] = {
    val mapSize = (random.nextDouble() * (mapMaxSize - mapMinSize) + mapMinSize).toInt
    (1 to mapSize)
      .map(_ => keyGenerator.generate -> valueGenerator.generate)
      .toMap
  }
}