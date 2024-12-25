package io.github.datacatering.datacaterer.core.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object ObjectMapperUtil {

  val yamlObjectMapper = new ObjectMapper(new YAMLFactory())
  yamlObjectMapper.registerModule(DefaultScalaModule)
  yamlObjectMapper.setSerializationInclusion(Include.NON_ABSENT)

  val jsonObjectMapper = new ObjectMapper()
  jsonObjectMapper.registerModule(DefaultScalaModule)
  jsonObjectMapper.registerModule(new JavaTimeModule())
  jsonObjectMapper.registerModule(new JodaModule())
  jsonObjectMapper.setSerializationInclusion(Include.NON_ABSENT)

}
