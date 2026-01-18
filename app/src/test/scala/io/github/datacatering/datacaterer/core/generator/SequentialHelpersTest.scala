package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import org.scalatest.funsuite.AnyFunSuite

class SequentialHelpersTest extends AnyFunSuite {

  // ========== sequence() Tests ==========

  test("sequence() with defaults should generate simple sequential numbers") {
    val field = FieldBuilder()
      .name("id")
      .sequence()
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("1 + " + INDEX_INC_FIELD))
    assert(sql.get.toString.contains("CONCAT"))
  }

  test("sequence() with custom start should use specified starting value") {
    val field = FieldBuilder()
      .name("order_num")
      .sequence(start = 1000)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("1000 + " + INDEX_INC_FIELD))
  }

  test("sequence() with step should increment by specified amount") {
    val field = FieldBuilder()
      .name("even_numbers")
      .sequence(start = 0, step = 2)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("0 + " + INDEX_INC_FIELD + " * 2"))
  }

  test("sequence() with prefix should prepend string") {
    val field = FieldBuilder()
      .name("order_id")
      .sequence(prefix = "ORD-")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'ORD-'"))
    assert(sql.get.toString.contains("CONCAT"))
  }

  test("sequence() with suffix should append string") {
    val field = FieldBuilder()
      .name("customer_id")
      .sequence(suffix = "-CUST")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'-CUST'"))
  }

  test("sequence() with padding should zero-pad numbers") {
    val field = FieldBuilder()
      .name("padded_id")
      .sequence(start = 1, padding = 8)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("LPAD"))
    assert(sql.get.toString.contains("8"))
    assert(sql.get.toString.contains("'0'"))
  }

  test("sequence() with all options should combine correctly") {
    val field = FieldBuilder()
      .name("full_id")
      .sequence(start = 1000, step = 1, prefix = "ID-", padding = 8, suffix = "-END")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("1000"))
    assert(sqlStr.contains("'ID-'"))
    assert(sqlStr.contains("'-END'"))
    assert(sqlStr.contains("LPAD"))
    assert(sqlStr.contains("8"))
    assert(sqlStr.contains("CONCAT"))
  }

  // ========== dailyBatchSequence() Tests ==========

  test("dailyBatchSequence() with defaults should generate batch IDs") {
    val field = FieldBuilder()
      .name("batch_id")
      .dailyBatchSequence()
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'BATCH-'"))
    assert(sql.get.toString.contains("DATE_FORMAT"))
    assert(sql.get.toString.contains("CURRENT_DATE()"))
    assert(sql.get.toString.contains("'yyyyMMdd'"))
    assert(sql.get.toString.contains("LPAD"))
    assert(sql.get.toString.contains(INDEX_INC_FIELD))
  }

  test("dailyBatchSequence() with custom prefix should use specified prefix") {
    val field = FieldBuilder()
      .name("job_id")
      .dailyBatchSequence(prefix = "JOB-")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'JOB-'"))
  }

  test("dailyBatchSequence() with custom date format should use specified format") {
    val field = FieldBuilder()
      .name("batch_id")
      .dailyBatchSequence(dateFormat = "yyyy-MM-dd")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'yyyy-MM-dd'"))
  }

  // ========== semanticVersion() Tests ==========

  test("semanticVersion() with defaults should start at 1.0.0") {
    val field = FieldBuilder()
      .name("version")
      .semanticVersion()
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    val sqlStr = sql.get.toString
    assert(sqlStr.contains("'1'"))
    assert(sqlStr.contains("'0'"))
    assert(sqlStr.contains(INDEX_INC_FIELD))
    assert(sqlStr.contains("CONCAT"))
  }

  test("semanticVersion() with custom major and minor should use specified values") {
    val field = FieldBuilder()
      .name("version")
      .semanticVersion(major = 2, minor = 5)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'2'"))
    assert(sql.get.toString.contains("'5'"))
  }

  test("semanticVersion() with patchIncrement=false should keep patch at 0") {
    val field = FieldBuilder()
      .name("static_version")
      .semanticVersion(major = 1, minor = 0, patchIncrement = false)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("'0'"))
    assert(!sql.get.toString.contains(INDEX_INC_FIELD))
  }

  test("semanticVersion() with patchIncrement=true should increment patch") {
    val field = FieldBuilder()
      .name("version")
      .semanticVersion(major = 3, minor = 2, patchIncrement = true)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains(INDEX_INC_FIELD))
  }

  // ========== Integration Tests ==========

  test("sequential helpers should work independently") {
    val seqField = FieldBuilder().name("seq").sequence(start = 100).field
    val batchField = FieldBuilder().name("batch").dailyBatchSequence().field
    val versionField = FieldBuilder().name("version").semanticVersion(major = 2).field

    assert(seqField.options.get(SQL_GENERATOR).isDefined)
    assert(batchField.options.get(SQL_GENERATOR).isDefined)
    assert(versionField.options.get(SQL_GENERATOR).isDefined)
  }

  test("sequence() should handle negative start values") {
    val field = FieldBuilder()
      .name("negative_seq")
      .sequence(start = -100, step = 1)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("-100"))
  }

  test("sequence() with no padding should not use LPAD") {
    val field = FieldBuilder()
      .name("no_padding")
      .sequence(start = 1, padding = 0)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(!sql.get.toString.contains("LPAD"))
  }

  // ========== Java Compatibility Tests ==========

  test("Java compatibility - sequence") {
    val field = FieldBuilder()
      .name("java_seq")
      .sequence()

    assert(field.field.options.get(SQL_GENERATOR).isDefined)
  }

  test("Java compatibility - dailyBatchSequence") {
    val field = FieldBuilder()
      .name("java_batch")
      .dailyBatchSequence()

    assert(field.field.options.get(SQL_GENERATOR).isDefined)
  }

  test("Java compatibility - semanticVersion") {
    val field = FieldBuilder()
      .name("java_version")
      .semanticVersion()

    assert(field.field.options.get(SQL_GENERATOR).isDefined)
  }
}
