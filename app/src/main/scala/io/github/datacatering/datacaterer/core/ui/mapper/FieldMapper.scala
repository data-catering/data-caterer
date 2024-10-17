package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants.{DATA_CONTRACT_CLI, MARQUEZ, ONE_OF_GENERATOR, ONE_OF_GENERATOR_DELIMITER, OPEN_API, OPEN_DATA_CONTRACT_STANDARD, OPEN_LINEAGE_NAMESPACE, OPEN_METADATA, OPEN_METADATA_AUTH_TYPE, PATH, REGEX_GENERATOR, SCHEMA_LOCATION, URL}
import io.github.datacatering.datacaterer.api.model.DataType
import io.github.datacatering.datacaterer.api.{FieldBuilder, MetadataSourceBuilder}
import io.github.datacatering.datacaterer.core.exception.InvalidMetadataDataSourceOptionsException
import io.github.datacatering.datacaterer.core.model.Constants.CONNECTION_TYPE
import io.github.datacatering.datacaterer.core.ui.mapper.UiMapper.checkOptions
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, FieldRequest, MetadataSourceRequest}
import org.apache.log4j.Logger

object FieldMapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def fieldMapping(dataSourceRequest: DataSourceRequest): (Option[MetadataSourceBuilder], Option[List[FieldBuilder]]) = {
    dataSourceRequest.fields.map(fields => {
      fields.optMetadataSource.map(metadataSource =>
        (Some(dataGenerationMetadataSourceMapping(metadataSource)), None)
      ).getOrElse(
        (None, Some(fieldMapping(dataSourceRequest.name, fields.optFields)))
      )
    }).getOrElse((None, None))
  }

  private def dataGenerationMetadataSourceMapping(metadataSource: MetadataSourceRequest): MetadataSourceBuilder = {
    // metadata source exists and should have options defined (at least type and groupType)
    if (metadataSource.overrideOptions.isEmpty) {
      throw InvalidMetadataDataSourceOptionsException(metadataSource.name)
    }
    val builder = MetadataSourceBuilder()
    val baseOptions = metadataSource.overrideOptions.getOrElse(Map())
    if (!baseOptions.contains(CONNECTION_TYPE)) {
      throw new IllegalArgumentException("Unable to determine metadata source type")
    }
    val builderWithOptions = baseOptions(CONNECTION_TYPE) match {
      case DATA_CONTRACT_CLI =>
        checkOptions(metadataSource.name, List(PATH), baseOptions)
        builder.dataContractCli(baseOptions(PATH))
      case OPEN_METADATA =>
        checkOptions(metadataSource.name, List(URL, OPEN_METADATA_AUTH_TYPE), baseOptions)
        builder.openMetadata(baseOptions(URL), baseOptions(OPEN_METADATA_AUTH_TYPE), baseOptions)
      case OPEN_API =>
        checkOptions(metadataSource.name, List(SCHEMA_LOCATION), baseOptions)
        builder.openApi(baseOptions(SCHEMA_LOCATION))
      case MARQUEZ =>
        checkOptions(metadataSource.name, List(URL, OPEN_LINEAGE_NAMESPACE), baseOptions)
        builder.marquez(baseOptions(URL), baseOptions(OPEN_LINEAGE_NAMESPACE))
      case OPEN_DATA_CONTRACT_STANDARD =>
        checkOptions(metadataSource.name, List(PATH), baseOptions)
        builder.openDataContractStandard(baseOptions(PATH))
      case x =>
        LOGGER.warn(s"Unsupported metadata source for data generation, metadata-connection-type=$x")
        builder
    }
    builderWithOptions
  }

  /*
   * Get field mapping based on manually defined fields or setting the metadata source to get schema details.
   * Try get metadata source details first. If doesn't exist, fallback to manual fields.
   * If no manual fields or metadata source exist, then metadata will be gathered from data source.
   */
  private def fieldMapping(
                            dataSourceName: String,
                            optFields: Option[List[FieldRequest]] = None
                          ): List[FieldBuilder] = {
    optFields.map(fields => {
      fields.map(field => {
        assert(field.name.nonEmpty, s"Field name cannot be empty, data-source-name=$dataSourceName")
        assert(field.`type`.nonEmpty, s"Field type cannot be empty, data-source-name=$dataSourceName, field-name=${field.name}")
        val options = field.options.getOrElse(Map())
        val baseBuild = FieldBuilder().name(field.name).`type`(DataType.fromString(field.`type`)).options(options)
        val withRegex = options.get(REGEX_GENERATOR).map(regex => baseBuild.regex(regex)).getOrElse(baseBuild)
        val withOneOf = options.get(ONE_OF_GENERATOR).map(oneOf => withRegex.oneOf(oneOf.split(ONE_OF_GENERATOR_DELIMITER).map(_.trim): _*)).getOrElse(withRegex)
        val optNested = field.nested.map(nestedFields => fieldMapping(dataSourceName, nestedFields.optFields))
        optNested.map(nested => withOneOf.schema(nested: _*)).getOrElse(withOneOf)
      })
    }).getOrElse(List())
  }

}
