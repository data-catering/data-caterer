package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataGeneratorType
import io.github.datacatering.datacaterer.core.model.Constants._
import io.github.datacatering.datacaterer.core.util.GeneratorUtil
import net.datafaker.Faker
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
import scala.util.Try

object RandomDataGenerator {

  def getGeneratorForStructType(structType: StructType, faker: Faker = new Faker()): Array[DataGenerator[_]] = {
    structType.fields.map(getGeneratorForStructField(_, faker))
  }

  def getGeneratorForStructField(structField: StructField, faker: Faker = new Faker()): DataGenerator[_] = {
    structField.dataType match {
      case StringType => new RandomStringDataGenerator(structField, faker)
      case IntegerType => new RandomIntDataGenerator(structField, faker)
      case LongType => new RandomLongDataGenerator(structField, faker)
      case ShortType => new RandomShortDataGenerator(structField, faker)
      case DecimalType() => new RandomDecimalDataGenerator(structField, faker)
      case DoubleType => new RandomDoubleDataGenerator(structField, faker)
      case FloatType => new RandomFloatDataGenerator(structField, faker)
      case DateType => new RandomDateDataGenerator(structField, faker)
      case TimestampType => new RandomTimestampDataGenerator(structField, faker)
      case BooleanType => new RandomBooleanDataGenerator(structField, faker)
      case BinaryType => new RandomBinaryDataGenerator(structField, faker)
      case ByteType => new RandomByteDataGenerator(structField, faker)
      case ArrayType(dt, _) => new RandomArrayDataGenerator(structField, dt, faker)
      case MapType(kt, vt, _) => new RandomMapDataGenerator(structField, kt, vt, faker)
      case StructType(_) => new RandomStructTypeDataGenerator(structField, faker)
      case x => throw UnsupportedDataGeneratorType(s"Unsupported type for random data generation: name=${structField.name}, type=${x.typeName}")
    }
  }

  def getGeneratorForDataType(dataType: DataType, faker: Faker = new Faker()): DataGenerator[_] = {
    getGeneratorForStructField(StructField("", dataType))
  }

  class RandomStringDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private val minLength = tryGetValue(structField.metadata, MINIMUM_LENGTH, 1)
    private val maxLength = tryGetValue(structField.metadata, MAXIMUM_LENGTH, 20)
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, field-name=${structField.name}, minLength=$minLength, maxLength=$maxLength")
    private lazy val tryExpression = Try(structField.metadata.getString(EXPRESSION))
    private val characterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 "
    private val characterSetSize = characterSet.length

    override val edgeCases: List[String] = List("", "\n", "\r", "\t", " ", "\\u0000", "\\ufff",
      "İyi günler", "Спасибо", "Καλημέρα", "صباح الخير", "Förlåt", "你好吗", "Nhà vệ sinh ở đâu", "こんにちは", "नमस्ते", "Բարեւ", "Здравейте")

    override def generate: String = {
      if (tryExpression.isSuccess) {
        faker.expression(tryExpression.get)
      } else {
        val stringLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
        random.alphanumeric.take(stringLength).mkString
      }
    }

    override def generateSqlExpression: String = {
      if (tryExpression.isSuccess) {
        s"$GENERATE_FAKER_EXPRESSION_UDF('${tryExpression.get}')"
      } else {
        val randLength = s"(SELECT CAST(ROUND((length / $DATA_CATERER_RANDOM_LENGTH_MAX_VALUE) * ${maxLength - minLength} + $minLength, 0) AS INT) FROM $DATA_CATERER_RANDOM_LENGTH)"
        s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $randLength), i -> SUBSTR('$characterSet', CEIL(RAND() * $characterSetSize), 1)))"
      }
    }
  }

  class RandomIntDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Int] {
    private val min = tryGetValue(structField.metadata, MINIMUM, 0)
    private val max = tryGetValue(structField.metadata, MAXIMUM, 100000)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Int] = List(Int.MaxValue, Int.MinValue, 0)

    override def generate: Int = {
      faker.random().nextInt(min, max)
    }

    override def generateSqlExpression: String = sqlExpressionForNumeric(structField.metadata, "INT", sqlRandom)
  }

  class RandomShortDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Short] {
    private val min = tryGetValue(structField.metadata, MINIMUM, 0)
    private val max = tryGetValue(structField.metadata, MAXIMUM, 1000)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Short] = List(Short.MaxValue, Short.MinValue, 0)

    override def generate: Short = {
      (random.nextDouble() * (max - min) + min).toShort
    }

    override def generateSqlExpression: String = sqlExpressionForNumeric(structField.metadata, "SHORT", sqlRandom)
  }

  class RandomLongDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Long] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0L)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000L)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Long] = List(Long.MaxValue, Long.MinValue, 0)

    override def generate: Long = {
      faker.random().nextLong(min, max)
    }

    override def generateSqlExpression: String = sqlExpressionForNumeric(structField.metadata, "LONG", sqlRandom)
  }

  class RandomDecimalDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[BigDecimal] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, BigDecimal.valueOf(0))
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, BigDecimal.valueOf(100000))
    private val decimalType = structField.dataType.asInstanceOf[DecimalType]
    private lazy val precision = tryGetValue(structField.metadata, NUMERIC_PRECISION, decimalType.precision)
    private lazy val scale = tryGetValue(structField.metadata, NUMERIC_SCALE, decimalType.scale)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[BigDecimal] = List(Long.MaxValue, Long.MinValue, 0)

    override def generate: BigDecimal = {
      random.nextDouble() * (max - min) + min
    }

    override def generateSqlExpression: String = sqlExpressionForNumeric(structField.metadata, s"DECIMAL($precision, $scale)", sqlRandom)
  }

  class RandomDoubleDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Double] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0.0)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000.0)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Double] = List(Double.PositiveInfinity, Double.MaxValue, Double.MinPositiveValue,
      0.0, -0.0, Double.MinValue, Double.NegativeInfinity, Double.NaN)

    override def generate: Double = {
      faker.random().nextDouble(min, max)
    }

    override def generateSqlExpression: String = sqlExpressionForNumeric(structField.metadata, "DOUBLE", sqlRandom)
  }

  class RandomFloatDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Float] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0.0.toFloat)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000.0.toFloat)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Float] = List(Float.PositiveInfinity, Float.MaxValue, Float.MinPositiveValue,
      0.0f, -0.0f, Float.MinValue, Float.NegativeInfinity, Float.NaN)

    override def generate: Float = {
      faker.random().nextDouble(min, max).toFloat
    }

    override def generateSqlExpression: String = sqlExpressionForNumeric(structField.metadata, "FLOAT", sqlRandom)
  }

  class RandomDateDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Date] {
    private lazy val min = getMinValue
    private lazy val max = getMaxValue
    assert(min.isBefore(max) || min.isEqual(max), s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")
    private lazy val maxDays = java.time.temporal.ChronoUnit.DAYS.between(min, max).toInt

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L206
    override val edgeCases: List[Date] = List(
      Date.valueOf("0001-01-01"),
      Date.valueOf("1582-10-15"),
      Date.valueOf("1970-01-01"),
      Date.valueOf("9999-12-31")
    )

    override def generate: Date = {
      Date.valueOf(min.plusDays(random.nextInt(maxDays)))
    }

    private def getMinValue: LocalDate = {
      Try(structField.metadata.getString(MINIMUM)).map(LocalDate.parse)
        .getOrElse(LocalDate.now().minusDays(365))
    }

    private def getMaxValue: LocalDate = {
      Try(structField.metadata.getString(MAXIMUM)).map(LocalDate.parse)
        .getOrElse(LocalDate.now())
    }

    override def generateSqlExpression: String = {
      s"DATE_ADD('${min.toString}', CAST($sqlRandom * $maxDays AS INT))"
    }
  }

  class RandomTimestampDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Timestamp] {
    private lazy val min = getMinValue
    private lazy val max = getMaxValue
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L159
    override val edgeCases: List[Timestamp] = List(
      Timestamp.valueOf("0001-01-01 00:00:00"),
      Timestamp.valueOf("1582-10-15 23:59:59"),
      Timestamp.valueOf("1970-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59")
    )

    override def generate: Timestamp = {
      val milliSecondsSinceEpoch = (random.nextDouble() * (max - min) + min).toLong
      Timestamp.from(Instant.ofEpochMilli(milliSecondsSinceEpoch))
    }

    private def getMinValue: Long = {
      Try(structField.metadata.getString(MINIMUM)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now().minus(365, ChronoUnit.DAYS)))
        .toInstant.toEpochMilli
    }

    private def getMaxValue: Long = {
      Try(structField.metadata.getString(MAXIMUM)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now()))
        .toInstant.toEpochMilli + 1L
    }

    override def generateSqlExpression: String = {
      s"CAST(TIMESTAMP_MILLIS(CAST($sqlRandom * ${max - min} + $min AS LONG)) AS TIMESTAMP)"
    }
  }

  class RandomBooleanDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Boolean] {
    override def generate: Boolean = {
      random.nextBoolean()
    }

    override def generateSqlExpression: String = {
      s"BOOLEAN(ROUND($sqlRandom))"
    }
  }

  class RandomBinaryDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Array[Byte]] {
    private lazy val minLength = tryGetValue(structField.metadata, MINIMUM_LENGTH, 1)
    private lazy val maxLength = tryGetValue(structField.metadata, MAXIMUM_LENGTH, 20)
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, " +
      s"field-name=${structField.name}, minLength=$minLength, maxLength=$maxLength")

    override val edgeCases: List[Array[Byte]] = List(Array(), "\n".getBytes, "\r".getBytes, "\t".getBytes,
      " ".getBytes, "\\u0000".getBytes, "\\ufff".getBytes, Array(Byte.MinValue), Array(Byte.MaxValue))

    override def generate: Array[Byte] = {
      val byteLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
      faker.random().nextRandomBytes(byteLength)
    }

    override def generateSqlExpression: String = {
      s"TO_BINARY(ARRAY_JOIN(TRANSFORM(ARRAY_REPEAT(1, CAST($sqlRandom * ${maxLength - minLength} + $minLength AS INT)), i -> CHAR(ROUND($sqlRandom * 94 + 32, 0))), ''), 'utf-8')"
    }
  }

  class RandomByteDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Byte] {
    override val edgeCases: List[Byte] = List(Byte.MinValue, Byte.MaxValue)

    override def generate: Byte = {
      faker.random().nextRandomBytes(1).head
    }

    override def generateSqlExpression: String = {
      s"TO_BINARY(CHAR(ROUND($sqlRandom * 94 + 32, 0)))"
    }
  }

  class RandomArrayDataGenerator[T](val structField: StructField, val dataType: DataType, val faker: Faker = new Faker()) extends ArrayDataGenerator[T] {
    override lazy val arrayMinSize: Int = tryGetValue(structField.metadata, ARRAY_MINIMUM_LENGTH, 0)
    override lazy val arrayMaxSize: Int = tryGetValue(structField.metadata, ARRAY_MAXIMUM_LENGTH, 5)
    assert(arrayMinSize >= 0, s"arrayMinSize has to be greater than or equal to 0, " +
      s"field-name=${structField.name}, arrayMinSize=$arrayMinSize")
    assert(arrayMinSize <= arrayMaxSize, s"arrayMinSize has to be less than or equal to arrayMaxSize, " +
      s"field-name=${structField.name}, arrayMinSize=$arrayMinSize, arrayMaxSize=$arrayMaxSize")

    override def elementGenerator: DataGenerator[T] = {
      dataType match {
        case structType: StructType =>
          new RandomStructTypeDataGenerator(StructField(structField.name, structType), faker).asInstanceOf[DataGenerator[T]]
        case _ =>
          getGeneratorForStructField(structField.copy(dataType = dataType), faker).asInstanceOf[DataGenerator[T]]
      }
    }

    override def generateSqlExpression: String = {
      val nestedSqlExpressions = dataType match {
        case structType: StructType =>
          val structGen = new RandomStructTypeDataGenerator(StructField(structField.name, structType))
          structGen.generateSqlExpressionWrapper
        case _ =>
          getGeneratorForStructField(structField.copy(dataType = dataType)).generateSqlExpressionWrapper
      }
      s"TRANSFORM(ARRAY_REPEAT(1, CAST($sqlRandom * ${arrayMaxSize - arrayMinSize} + $arrayMinSize AS INT)), i -> $nestedSqlExpressions)"
    }
  }

  class RandomMapDataGenerator[T, K](
                                      val structField: StructField,
                                      val keyDataType: DataType,
                                      val valueDataType: DataType,
                                      val faker: Faker = new Faker()
                                    ) extends MapDataGenerator[T, K] {
    override lazy val mapMinSize: Int = tryGetValue(structField.metadata, MAP_MINIMUM_SIZE, 0)
    override lazy val mapMaxSize: Int = tryGetValue(structField.metadata, MAP_MAXIMUM_SIZE, 5)
    assert(mapMinSize >= 0, s"mapMinSize has to be greater than or equal to 0, " +
      s"field-name=${structField.name}, mapMinSize=$mapMinSize")
    assert(mapMinSize <= mapMaxSize, s"mapMinSize has to be less than or equal to mapMaxSize, " +
      s"field-name=${structField.name}, mapMinSize=$mapMinSize, mapMaxSize=$mapMaxSize")

    override def keyGenerator: DataGenerator[T] = getGeneratorForDataType(keyDataType).asInstanceOf[DataGenerator[T]]

    override def valueGenerator: DataGenerator[K] = getGeneratorForDataType(valueDataType).asInstanceOf[DataGenerator[K]]

    //generate two arrays, key array and value array, then use map_from_arrays(col(keyArr), col(valueArr))
    //how to make it empty map when size is 0
    override def generateSqlExpression: String = {
      val keyDataGenerator = getGeneratorForDataType(keyDataType)
      val valueDataGenerator = getGeneratorForDataType(valueDataType)
      val keySql = keyDataGenerator.generateSqlExpressionWrapper
      val valueSql = valueDataGenerator.generateSqlExpressionWrapper
      s"STR_TO_MAP(CONCAT_WS(',', TRANSFORM(ARRAY_REPEAT(1, CAST($sqlRandom * ${mapMaxSize - mapMinSize} + $mapMinSize AS INT)), i -> CONCAT($keySql, '->', $valueSql))), '->', ',')"
    }
  }

  class RandomStructTypeDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Row] {
    override def generate: Row = {
      structField.dataType match {
        case ArrayType(dt, _) =>
          val listGenerator = new RandomArrayDataGenerator(structField, dt, faker)
          Row.fromSeq(listGenerator.generate)
        case StructType(fields) =>
          val dataGenerators = fields.map(field => getGeneratorForStructField(field, faker))
          Row.fromSeq(dataGenerators.map(_.generateWrapper()))
      }
    }

    override def generateSqlExpression: String = {
      val nestedSqlExpression = structField.dataType match {
        case ArrayType(dt, _) =>
          val listGenerator = new RandomArrayDataGenerator(structField, dt)
          listGenerator.generateSqlExpressionWrapper
        case StructType(fields) =>
          fields.map(f => GeneratorUtil.getDataGenerator(f, faker))
            .map(f => s"'${f.structField.name}', ${f.generateSqlExpressionWrapper}")
            .mkString(",")
        case _ =>
          getGeneratorForStructField(structField).generateSqlExpressionWrapper
      }
      s"NAMED_STRUCT($nestedSqlExpression)"
    }
  }


  def sqlExpressionForNumeric(metadata: Metadata, typeName: String, sqlRand: String): String = {
    val (min, max, diff, mean) = typeName.toUpperCase match {
      case "INT" =>
        val min = tryGetValue(metadata, MINIMUM, 0)
        val max = tryGetValue(metadata, MAXIMUM, 100000)
        val diff = max - min
        val mean = tryGetValue(metadata, MEAN, diff.toDouble)
        (min, max, diff, mean)
      case "SHORT" =>
        val min = tryGetValue(metadata, MINIMUM, 0)
        val max = tryGetValue(metadata, MAXIMUM, 1000)
        val diff = max - min
        val mean = tryGetValue(metadata, MEAN, diff.toDouble)
        (min, max, diff, mean)
      case "LONG" =>
        val min = tryGetValue(metadata, MINIMUM, 0L)
        val max = tryGetValue(metadata, MAXIMUM, 100000L)
        val diff = max - min
        val mean = tryGetValue(metadata, MEAN, diff.toDouble)
        (min, max, diff, mean)
      case x if x.startsWith("DECIMAL") =>
        val min = tryGetValue(metadata, MINIMUM, BigDecimal.valueOf(0))
        val max = tryGetValue(metadata, MAXIMUM, BigDecimal.valueOf(100000))
        val diff = max - min
        val mean = tryGetValue(metadata, MEAN, diff.toDouble)
        (min, max, diff, mean)
      case "DOUBLE" =>
        val min = tryGetValue(metadata, MINIMUM, 0.0)
        val max = tryGetValue(metadata, MAXIMUM, 100000.0)
        val diff = max - min
        val mean = tryGetValue(metadata, MEAN, diff)
        (min, max, diff, mean)
      case "FLOAT" =>
        val min = tryGetValue(metadata, MINIMUM, 0.0.toFloat)
        val max = tryGetValue(metadata, MAXIMUM, 100000.0.toFloat)
        val diff = max - min
        val mean = tryGetValue(metadata, MEAN, diff)
        (min, max, diff, mean)
    }
    val defaultValue = tryGetValue(metadata, DEFAULT_VALUE, "")
    val standardDeviation = tryGetValue(metadata, STANDARD_DEVIATION, 1.0)
    val distinctCount = tryGetValue(metadata, DISTINCT_COUNT, 0)
    val count = tryGetValue(metadata, ROW_COUNT, 0)
    val isUnique = tryGetValue(metadata, IS_UNIQUE, "false")
    // allow for different distributions (exponential, gaussian)
    val distribution = tryGetValue(metadata, DISTRIBUTION, "")
    val rateParameter = tryGetValue(metadata, DISTRIBUTION_RATE_PARAMETER, 1.0)

    val baseFormula = if (isIncrementalNumber(defaultValue, distinctCount, count, isUnique)) {
      if (metadata.contains(MAXIMUM)) {
        s"$max + $INDEX_INC_FIELD + 1" //index col starts at 0
      } else {
        s"$INDEX_INC_FIELD + 1"
      }
    } else if (metadata.contains(STANDARD_DEVIATION) && metadata.contains(MEAN)) {
      val randNormal = sqlRand.replace("RAND", "RANDN")
      s"$randNormal * $standardDeviation + $mean"
    } else if (distribution.equalsIgnoreCase(DISTRIBUTION_NORMAL)) {
      val randNormal = sqlRand.replace("RAND", "RANDN")
      s"$randNormal + $min"
    } else if (distribution.equalsIgnoreCase(DISTRIBUTION_EXPONENTIAL)) {
      s"GREATEST($min, LEAST($max, $diff * (-LN(1 - $sqlRand) / $rateParameter) + $min))"
    } else {
      s"$sqlRand * $diff + $min"
    }

    val rounded = if (metadata.contains(ROUND)) {
      val roundValue = metadata.getString(ROUND)
      s"ROUND($baseFormula, $roundValue)"
    } else baseFormula

    if (!rounded.contains(INDEX_INC_FIELD) && (typeName == "INT" || typeName == "SHORT" || typeName == "LONG")) {
      s"CAST(ROUND($rounded, 0) AS $typeName)"
    } else {
      s"CAST($rounded AS $typeName)"
    }
  }

  private def isIncrementalNumber(defaultValue: String, distinctCount: Int, count: Int, isUnique: String) = {
    defaultValue.toLowerCase.startsWith("nextval") || (distinctCount == count && distinctCount > 0) || isUnique == "true"
  }

  def tryGetValue[T](metadata: Metadata, key: String, default: T)(implicit converter: Converter[T]): T = {
    Try(converter.convert(metadata.getString(key)))
      .getOrElse(default)
  }

  trait Converter[T] {
    self =>
    def convert(v: String): T
  }

  object Converter {
    implicit val stringLoader: Converter[String] = (v: String) => v

    implicit val intLoader: Converter[Int] = (v: String) => v.toInt

    implicit val longLoader: Converter[Long] = (v: String) => v.toLong

    implicit val shortLoader: Converter[Short] = (v: String) => v.toShort

    implicit val doubleLoader: Converter[Double] = (v: String) => v.toDouble

    implicit val floatLoader: Converter[Float] = (v: String) => v.toFloat

    implicit val decimalLoader: Converter[BigDecimal] = (v: String) => BigDecimal(v)
  }
}

