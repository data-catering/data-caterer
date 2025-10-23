package io.github.datacatering.datacaterer.api.model

import io.github.datacatering.datacaterer.api.model.Constants.{CONFLUENT_SCHEMA_REGISTRY, DATA_CONTRACT_CLI, GREAT_EXPECTATIONS, JSON_SCHEMA, MARQUEZ, METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT, METADATA_SOURCE_TYPE, OPEN_API, OPEN_DATA_CONTRACT_STANDARD, OPEN_METADATA, YAML_PLAN, YAML_TASK}

trait MetadataSource {

  val `type`: String
  val connectionOptions: Map[String, String] = Map()

  def allOptions: Map[String, String] = {
    connectionOptions ++ Map(
      METADATA_SOURCE_TYPE -> `type`
    )
  }
}

case class MarquezMetadataSource(override val connectionOptions: Map[String, String] = Map(METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT -> "true")) extends MetadataSource {

  override val `type`: String = MARQUEZ

}

case class OpenMetadataSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = OPEN_METADATA

}

case class OpenAPISource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = OPEN_API

}

case class GreatExpectationsSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = GREAT_EXPECTATIONS

}

case class OpenDataContractStandardSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = OPEN_DATA_CONTRACT_STANDARD

}

case class DataContractCliSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = DATA_CONTRACT_CLI

}

case class ConfluentSchemaRegistrySource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = CONFLUENT_SCHEMA_REGISTRY

}

case class JsonSchemaSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = JSON_SCHEMA

}

case class YamlPlanSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = YAML_PLAN

}

case class YamlTaskSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = YAML_TASK

}
