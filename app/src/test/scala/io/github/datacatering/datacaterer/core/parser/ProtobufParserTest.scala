package io.github.datacatering.datacaterer.core.parser

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
class ProtobufParserTest extends AnyFunSuiteLike {

  test("Can read all structs from proto file") {
    val protoFile = new File("src/test/resources/sample/files/protobuf/example.proto")
    val fields = ProtobufParser.getSchemaFromProtoFile(Paths.get(protoFile.toURI))

    assertResult(15)(fields.size)
    assertResult(Some("long"))(fields.find(f => f.name == "int").get.`type`)
    assertResult(Some("string"))(fields.find(f => f.name == "text").get.`type`)
    assertResult(Some("string"))(fields.find(f => f.name == "enum_val").get.`type`)
    assertResult(Map("oneOf" -> "SECOND,NOTHING,FIRST"))(fields.find(f => f.name == "enum_val").get.options)
    assertResult(Some("struct<other: string>"))(fields.find(f => f.name == "message").get.`type`)
    assertResult(Some("long"))(fields.find(f => f.name == "optional_int").get.`type`)
    assertResult(Some("string"))(fields.find(f => f.name == "optional_text").get.`type`)
    assertResult(Some("string"))(fields.find(f => f.name == "optional_enum_val").get.`type`)
    assertResult(Map("oneOf" -> "SECOND,NOTHING,FIRST"))(fields.find(f => f.name == "optional_enum_val").get.options)
    assertResult(Some("array<long>"))(fields.find(f => f.name == "repeated_num").get.`type`)
    assertResult(Some("array<struct<other: string>>"))(fields.find(f => f.name == "repeated_message").get.`type`)
    assertResult(Some("integer"))(fields.find(f => f.name == "option_a").get.`type`)
    assertResult(Some("string"))(fields.find(f => f.name == "option_b").get.`type`)
    assertResult(Some("map<string,string>"))(fields.find(f => f.name == "map").get.`type`)
    assertResult(Some("struct<nst_msg: string>"))(fields.find(f => f.name == "nst_msg_val").get.`type`)
    assertResult(Some("struct<import: string>"))(fields.find(f => f.name == "import_example").get.`type`)
  }

}
