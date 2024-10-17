package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.ValidationConfiguration
import org.apache.spark.sql.SparkSession

object ValidationParser {

  def parseValidation(validationFolderPath: String)(implicit sparkSession: SparkSession): Array[ValidationConfiguration] = {
    YamlFileParser.parseFiles[ValidationConfiguration](validationFolderPath)
  }
}
