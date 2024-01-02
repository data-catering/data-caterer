package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.REGEX_GENERATOR
import io.github.datacatering.datacaterer.core.model.Constants.GENERATE_REGEX_UDF
import io.github.datacatering.datacaterer.core.exception.InvalidDataGeneratorConfigurationException
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

import scala.util.Try

object RegexDataGenerator {

  def getGenerator(structField: StructField, faker: Faker = new Faker()): DataGenerator[_] = {
    new RandomRegexDataGenerator(structField, faker)
  }

  class RandomRegexDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private val regex = Try(structField.metadata.getString(REGEX_GENERATOR))
      .getOrElse(throw new InvalidDataGeneratorConfigurationException(structField, REGEX_GENERATOR))

    override val edgeCases: List[String] = List()

    override def generate: String = {
      faker.regexify(regex)
    }

    override def generateSqlExpression: String = {
      s"$GENERATE_REGEX_UDF('$regex')"
    }
  }

}
