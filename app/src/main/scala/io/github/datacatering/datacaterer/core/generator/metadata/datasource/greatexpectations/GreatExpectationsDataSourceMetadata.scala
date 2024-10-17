package io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.GREAT_EXPECTATIONS_FILE
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceMetadata
import org.apache.log4j.Logger

case class GreatExpectationsDataSourceMetadata(
                                                name: String,
                                                format: String,
                                                connectionConfig: Map[String, String],
                                              ) extends DataSourceMetadata {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override val hasSourceData: Boolean = false

  override def getDataSourceValidations(dataSourceReadOptions: Map[String, String]): List[ValidationBuilder] = {
    val expectationsFile = dataSourceReadOptions(GREAT_EXPECTATIONS_FILE)
    LOGGER.info(s"Retrieving Great Expectations expectations from file, file=$expectationsFile")
    val validations = GreatExpectationsDataValidations.getDataValidations(expectationsFile)
    if (validations.nonEmpty) {
      LOGGER.info(s"Found Great Expectations expectations and converted to data validations, num-validations=${validations.size}")
    } else {
      LOGGER.warn("Unable to find any Great Expectation expectations")
    }
    validations
  }
}
