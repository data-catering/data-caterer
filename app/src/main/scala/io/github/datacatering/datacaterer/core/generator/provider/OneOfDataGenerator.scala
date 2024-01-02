package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.{ONE_OF_GENERATOR, ONE_OF_GENERATOR_DELIMITER}
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

import scala.util.{Failure, Success, Try}

object OneOfDataGenerator {

  def getGenerator(structField: StructField, faker: Faker = new Faker()): DataGenerator[Any] = {
    new RandomOneOfDataGenerator(structField, faker)
  }

  class RandomOneOfDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Any] {
    private lazy val oneOfValues = getOneOfList
    private lazy val oneOfArrayLength = oneOfValues.length
    assert(structField.metadata.contains(ONE_OF_GENERATOR), s"$ONE_OF_GENERATOR not defined for data generator in metadata, unable to generate data, name=${structField.name}, " +
      s"type=${structField.dataType}, metadata=${structField.metadata}")

    override def generate: Any = {
      oneOfValues(random.nextInt(oneOfArrayLength))
    }

    override def generateSqlExpression: String = {
      val oneOfValuesString = oneOfValues.mkString("||")
      s"CAST(SPLIT('$oneOfValuesString', '\\\\|\\\\|')[CAST(RAND() * $oneOfArrayLength AS INT)] AS ${structField.dataType.sql})"
    }

    private def getOneOfList: Array[String] = {
      val tryStringArray = Try(structField.metadata.getStringArray(ONE_OF_GENERATOR))
      tryStringArray match {
        case Failure(_) =>
          val tryString = Try(structField.metadata.getString(ONE_OF_GENERATOR))
          tryString match {
            case Failure(exception) => throw new RuntimeException(s"Failed to get $ONE_OF_GENERATOR from field metadata, " +
              s"field-name=${structField.name}, field-type=${structField.dataType.typeName}", exception)
            case Success(value) => value.split(ONE_OF_GENERATOR_DELIMITER)
          }
        case Success(value) => value
      }
    }
  }

}
