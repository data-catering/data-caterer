package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, ONE_OF_GENERATOR}
import io.github.datacatering.datacaterer.api.model.Field
import io.github.datacatering.datacaterer.core.exception.InvalidFakerExpressionException
import net.datafaker.Faker
import org.apache.log4j.Logger

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object CombinationCalculator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val FAKER_EXPRESSION_REGEX = "#\\{(.+?)}".r

  def totalCombinationsForSchema(fields: List[Field], faker: Faker): BigInt = {
    fields.map(field => {
      if (field.options.nonEmpty) {
        if (field.options.contains(EXPRESSION)) {
          val expression = field.options(EXPRESSION).toString
          val totalCombinations = getNumberCombinationsForFakerExpression(expression, faker)
          LOGGER.info(s"Total combinations for faker expression, expression=$expression, combinations=$totalCombinations")
          totalCombinations
        } else if (field.options.contains(ONE_OF_GENERATOR)) {
          BigInt(field.options(ONE_OF_GENERATOR).asInstanceOf[List[_]].size)
        } else {
          BigInt(1)
        }
      } else if (field.fields.nonEmpty) {
        totalCombinationsForSchema(field.fields, faker)
      } else {
        BigInt(1)
      }
    }).product
  }

  private def getNumberCombinationsForFakerExpression(expression: String, faker: Faker): BigInt = {
    val allMatches = FAKER_EXPRESSION_REGEX.findAllMatchIn(expression).toList
    LOGGER.info(s"Found faker expression matches, num-matches=${allMatches.size}, matches=$allMatches")
    val totalCombinations: BigInt = allMatches.map(m => fetchNumValues(m.group(1), faker)).product
    totalCombinations
  }

  /*
  different scenarios for faker expressions
  1. #{Name.name} => Map[String, List[String]]
  2. #{first_name} #{last_name} => List[String] that contains #{} pattern
  3. #{male_first_name} => List[String]
   */
  private def fetchNumValues(key: String, faker: Faker, baseMap: Map[String, util.List[String]] = Map()): BigInt = {
    val spt = key.toLowerCase.split("\\.")
    if (baseMap.nonEmpty) {
      val expressionValues = baseMap(key).toList
      if (containsFakerExpression(expressionValues)) {
        expressionValues.map(exp => {
          val allMatches = FAKER_EXPRESSION_REGEX.findAllMatchIn(exp).toList
          allMatches.map(expMatch => {
            val mapMatch = baseMap(expMatch.group(1)).toList
            if (containsFakerExpression(mapMatch)) {
              mapMatch.map(m => {
                val innerMatch = FAKER_EXPRESSION_REGEX.findAllMatchIn(m).toList
                innerMatch.map(i => fetchNumValues(i.group(1), faker, baseMap)).product
              }).sum
            } else {
              LOGGER.debug(s"Inner expression match, expression=$exp, inner-expression=${expMatch.group(1)}, size=${mapMatch.size}")
              BigInt(mapMatch.size)
            }
          }).product
        }).sum
      } else {
        LOGGER.debug(s"Simple list match, expression=$key, size=${expressionValues.size}")
        BigInt(expressionValues.size)
      }
    } else {
      if (spt.length < 2) throw InvalidFakerExpressionException(key)
      val fileObject = faker.fakeValuesService.fetchObject(spt.head, faker.getContext)
      fileObject match {
        case JavaMapStringToList(stringToStrings) =>
          val mapFakerExpressions = stringToStrings.asScala.toMap
          fetchNumValues(spt.last, faker, mapFakerExpressions)
        case _ => throw new RuntimeException(s"Unexpected return type from faker object, key=$key")
      }
    }
  }

  private def containsFakerExpression(expressions: List[String]): Boolean = expressions.exists(FAKER_EXPRESSION_REGEX.pattern.matcher(_).matches())
}

case class JavaMapStringToList(value: util.Map[String, util.List[String]])
