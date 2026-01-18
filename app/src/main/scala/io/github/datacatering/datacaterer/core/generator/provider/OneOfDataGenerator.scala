package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.{ONE_OF_GENERATOR, ONE_OF_GENERATOR_DELIMITER}
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

import scala.util.{Failure, Success, Try}

object OneOfDataGenerator {

  def getGenerator(structField: StructField, faker: Faker = new Faker()): DataGenerator[_] = {
    val oneOfValues = getOneOfList(structField)
    if (isWeightedOneOf(oneOfValues)) {
      new RandomOneOfWeightedDataGenerator(structField, faker, oneOfValues)
    } else {
      new RandomOneOfDataGenerator(structField, faker, oneOfValues)
    }
  }

  class RandomOneOfDataGenerator(val structField: StructField, val faker: Faker = new Faker(), oneOfValues: Array[String]) extends DataGenerator[Any] {
    private lazy val oneOfArrayLength = oneOfValues.length
    validateMetadata(structField)

    override def generate: Any = {
      oneOfValues(random.nextInt(oneOfArrayLength))
    }

    override def generateSqlExpression: String = {
      val oneOfValuesString = oneOfValues.mkString("||")
      s"CAST(SPLIT('$oneOfValuesString', '\\\\|\\\\|')[CAST($sqlRandom * $oneOfArrayLength AS INT)] AS ${structField.dataType.sql})"
    }
  }

  class RandomOneOfWeightedDataGenerator(val structField: StructField, val faker: Faker = new Faker(), oneOfValues: Array[String]) extends DataGenerator[Any] {
    private lazy val oneOfValuesWithWeights = getOneOfWithWeightsList
    private lazy val sumOfWeights = oneOfValuesWithWeights.map(_._2).sum
    private lazy val cumulativeWeight = oneOfValuesWithWeights
      .scanLeft(("", 0.0)) { case ((_, prev), (value, weight)) =>
        (s"$value", prev + weight / sumOfWeights)
      }
      .tail
    validateMetadata(structField)

    override def generate: Any = {
      cumulativeWeight.find(_._2 >= random.nextDouble) match {
        case Some((value, _)) => value
        case None => oneOfValuesWithWeights.last._1
      }
    }

    override def generateSqlExpression: String = {
      val caseExpr = cumulativeWeight
        .map { case (value, cumProb) => s"WHEN ${structField.name}_weight <= $cumProb THEN '$value'" }
        .mkString("\n    ")
      s"""CAST(
         |  CASE
         |    $caseExpr
         |    ELSE '${oneOfValuesWithWeights.last._1}'
         |  END
         |AS ${structField.dataType.sql})""".stripMargin
    }

    private def getOneOfWithWeightsList: Array[(String, Double)] = {
      //need to split each element of the array to get the value and the weight
      //values.map(v => s"${v._1}->${v._2}")
      oneOfValues.map { value =>
        val split = value.split("->")
        (split(0), split(1).toDouble)
      }
    }
  }

  private def validateMetadata(structField: StructField): Unit = {
    assert(structField.metadata.contains(ONE_OF_GENERATOR), s"$ONE_OF_GENERATOR not defined for data generator in metadata, unable to generate data, name=${structField.name}, " +
      s"type=${structField.dataType}, metadata=${structField.metadata}")
  }

  private def getOneOfList(structField: StructField): Array[String] = {
    val tryStringArray = Try(structField.metadata.getStringArray(ONE_OF_GENERATOR))
    tryStringArray match {
      case Failure(_) =>
        val tryString = Try(structField.metadata.getString(ONE_OF_GENERATOR))
        tryString match {
          case Failure(exception) => throw new IllegalArgumentException(s"Failed to get $ONE_OF_GENERATOR from field metadata, " +
            s"field-name=${structField.name}, field-type=${structField.dataType.typeName}", exception)
          case Success(value) => value.split(ONE_OF_GENERATOR_DELIMITER)
        }
      case Success(value) => value
    }
  }

  def isWeightedOneOf(oneOfValues: Array[String]): Boolean = {
    oneOfValues.forall(_.contains("->"))
  }
}
