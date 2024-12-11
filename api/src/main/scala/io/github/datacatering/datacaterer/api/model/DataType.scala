package io.github.datacatering.datacaterer.api.model

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

trait DataType {
  override def toString: String = getClass.getSimpleName.toLowerCase.stripSuffix("type$")
}

object DataType {
  def fromString(str: String): DataType = {
    str.toLowerCase match {
      case "string" => StringType
      case "int" | "integer" => IntegerType
      case "long" => LongType
      case "short" | "tinyint" | "smallint" => ShortType
      case "decimal" => DecimalType
      case "double" => DoubleType
      case "float" => FloatType
      case "date" => DateType
      case "date-time" | "datetime" | "timestamp" => TimestampType
      case "boolean" | "bool" => BooleanType
      case "binary" => BinaryType
      case "byte" => ByteType
      case "array" | "list" | "seq" => ArrayType
      case _ => StructType
    }
  }
}

class StringType extends DataType

case object StringType extends StringType {
  def instance: StringType.type = this
}

class IntegerType extends DataType

case object IntegerType extends IntegerType {
  def instance: IntegerType.type = this
}

class LongType extends DataType

case object LongType extends LongType {
  def instance: LongType.type = this
}

class ShortType extends DataType

case object ShortType extends ShortType {
  def instance: ShortType.type = this
}

class DecimalType(precision: Int = 10, scale: Int = 0) extends DataType {
  assert(scale < precision, "Scale required to be less than precision")

  override def toString: String = s"decimal($precision, $scale)"
}

case object DecimalType extends DecimalType(10, 0) {
  def instance: DecimalType.type = this
}

class DoubleType extends DataType

case object DoubleType extends DoubleType {
  def instance: DoubleType.type = this
}

class FloatType extends DataType

case object FloatType extends FloatType {
  def instance: FloatType.type = this
}

class DateType extends DataType

case object DateType extends DateType {
  def instance: DateType.type = this
}

class TimestampType extends DataType

case object TimestampType extends TimestampType {
  def instance: TimestampType.type = this
}

class BooleanType extends DataType

case object BooleanType extends BooleanType {
  def instance: BooleanType.type = this
}

class BinaryType extends DataType

case object BinaryType extends BinaryType {
  def instance: BinaryType.type = this
}

class ByteType extends DataType

case object ByteType extends ByteType {
  def instance: ByteType.type = this
}

class ArrayType(`type`: DataType = StringType) extends DataType {
  override def toString: String = s"array<${`type`.toString}>"
}

case object ArrayType extends ArrayType(StringType) {
  def instance: ArrayType.type = this
}

class MapType(keyType: DataType = StringType, valueType: DataType = StringType) extends DataType {
  override def toString: String = s"map<${keyType.toString},${valueType.toString}>"
}

case object MapType extends MapType(StringType, StringType) {
  def instance: MapType.type = this
}

class StructType(innerType: List[(String, DataType)] = List()) extends DataType {

  def this(innerType: java.util.List[java.util.Map.Entry[String, DataType]]) = {
    this(innerType.asScala.map(entry => entry.getKey -> entry.getValue).toList)
  }

  override def toString: String = {
    val innerStructType = innerType.map(t => s"${t._1}: ${t._2.toString}").mkString(",")
    s"struct<$innerStructType>"
  }
}

case object StructType extends StructType(List()) {
  def instance: StructType.type = this
}

object HeaderType {
  def getType: DataType = new ArrayType(new StructType(List("key" -> StringType, "value" -> BinaryType)))
}