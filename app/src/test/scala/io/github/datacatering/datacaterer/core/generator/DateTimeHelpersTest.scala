package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.generator.provider.RandomDataGenerator
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class DateTimeHelpersTest extends AnyFunSuite {

  // ========== withinDays Tests ==========

  test("withinDays() with default days should set min/max") {
    val field = FieldBuilder()
      .name("recent_date")
      .withinDays()
      .field

    val currentDate = java.time.LocalDate.now()
    val minDate = currentDate.minusDays(30)
    val minOptions = field.options.get(MINIMUM)
    val maxOptions = field.options.get(MAXIMUM)
    assert(minOptions.isDefined)
    assert(maxOptions.isDefined)
    assert(minOptions.get == minDate.toString)
    assert(maxOptions.get == currentDate.toString)
  }

  test("withinDays() with custom days should use specified value") {
    val field = FieldBuilder()
      .name("last_week")
      .withinDays(7)
      .field

    val currentDate = java.time.LocalDate.now()
    val minDate = currentDate.minusDays(7)
    val minOptions = field.options.get(MINIMUM)
    val maxOptions = field.options.get(MAXIMUM)
    assert(minOptions.isDefined)
    assert(maxOptions.isDefined)
    assert(minOptions.get == minDate.toString)
    assert(maxOptions.get == currentDate.toString)
  }

  // ========== futureDays Tests ==========

  test("futureDays() with default days should set min/max") {
    val field = FieldBuilder()
      .name("scheduled_date")
      .futureDays()
      .field

    val currentDate = java.time.LocalDate.now()
    val maxDate = currentDate.plusDays(90)
    val minOptions = field.options.get(MINIMUM)
    val maxOptions = field.options.get(MAXIMUM)
    assert(minOptions.isDefined)
    assert(maxOptions.isDefined)
    assert(minOptions.get == currentDate.toString)
    assert(maxOptions.get == maxDate.toString)
  }

  test("futureDays() with custom days should use specified value") {
    val field = FieldBuilder()
      .name("next_month")
      .futureDays(30)
      .field

    val currentDate = java.time.LocalDate.now()
    val maxDate = currentDate.plusDays(30)
    val minOptions = field.options.get(MINIMUM)
    val maxOptions = field.options.get(MAXIMUM)
    assert(minOptions.isDefined)
    assert(maxOptions.isDefined)
    assert(minOptions.get == currentDate.toString)
    assert(maxOptions.get == maxDate.toString)
  }

  // ========== excludeWeekends Tests ==========

  test("excludeWeekends() should set metadata flag") {
    val field = FieldBuilder()
      .name("business_date")
      .withinDays(30)
      .excludeWeekends()
      .field

    val excludeWeekends = field.options.get(DATE_EXCLUDE_WEEKENDS)
    assert(excludeWeekends.isDefined)
    assert(excludeWeekends.get == "true")
  }

  test("excludeWeekends() SQL generation logic should be correct") {
    // Test the SQL generation for weekend exclusion
    import org.apache.spark.sql.types.{DateType => SparkDateType}

    val metadata = new MetadataBuilder()
      .putString(MINIMUM, "2026-01-11") // Sunday
      .putString(MAXIMUM, "2026-01-18")
      .putString(DATE_EXCLUDE_WEEKENDS, "true")
      .build()

    val structField = StructField("weekday_date", SparkDateType, nullable = false, metadata)
    val generator = RandomDataGenerator.getGeneratorForStructField(structField, new net.datafaker.Faker())
    val sql = generator.generateSqlExpression

    // Verify SQL contains date calculations for finding Monday and weekday offsets
    assert(sql.contains("DAYOFWEEK"), "Should calculate day of week")
    assert(sql.contains("DATE_ADD"), "Should use DATE_ADD for date arithmetic")
    assert(sql.contains("MOD"), "Should use modulo for finding Monday")
  }

  // ========== businessHours Tests ==========

  test("businessHours() with default hours should generate SQL") {
    val field = FieldBuilder()
      .name("created_at")
      .businessHours()
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("TIMESTAMP_SECONDS"))
    assert(sql.get.toString.contains("UNIX_TIMESTAMP"))
    assert(sql.get.toString.contains("32400"))  // 9 * 3600 = 32400
  }

  test("businessHours() with custom hours should use specified range") {
    val field = FieldBuilder()
      .name("work_time")
      .businessHours(startHour = 8, endHour = 18)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("28800"))  // 8 * 3600 = 28800
    assert(sql.get.toString.contains("36000"))  // (18 - 8) * 3600 = 36000
  }

  test("businessHours() should validate hour ranges") {
    assertThrows[IllegalArgumentException] {
      FieldBuilder()
        .name("invalid")
        .businessHours(startHour = 25, endHour = 17)
    }

    assertThrows[IllegalArgumentException] {
      FieldBuilder()
        .name("invalid")
        .businessHours(startHour = 18, endHour = 9)
    }
  }

  // ========== timeBetween Tests ==========

  test("timeBetween() should generate SQL with time range") {
    val field = FieldBuilder()
      .name("meeting_time")
      .timeBetween("09:00", "17:00")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("TIMESTAMP_SECONDS"))
    assert(sql.get.toString.contains("32400"))  // 9 * 3600
    assert(sql.get.toString.contains("28800"))  // (17 - 9) * 3600
  }

  test("timeBetween() should parse hours and minutes correctly") {
    val field = FieldBuilder()
      .name("lunch_time")
      .timeBetween("12:30", "13:30")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("45000"))  // 12*3600 + 30*60 = 45000
    assert(sql.get.toString.contains("3600"))   // 1 hour difference
  }

  // ========== dailySequence Tests ==========

  test("dailySequence() with default increment should generate SQL") {
    val field = FieldBuilder()
      .name("seq_date")
      .dailySequence("2024-01-01")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("DATE_ADD"))
    assert(sql.get.toString.contains("'2024-01-01'"))
    assert(sql.get.toString.contains(INDEX_INC_FIELD))
    assert(sql.get.toString.contains("* 1"))
  }

  test("dailySequence() with custom increment should use specified value") {
    val field = FieldBuilder()
      .name("weekly_date")
      .dailySequence("2024-01-01", incrementDays = 7)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("* 7"))
  }

  // ========== hourlySequence Tests ==========

  test("hourlySequence() with default increment should generate SQL") {
    val field = FieldBuilder()
      .name("seq_timestamp")
      .hourlySequence("2024-01-01 00:00:00")
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("TIMESTAMP_SECONDS"))
    assert(sql.get.toString.contains("UNIX_TIMESTAMP"))
    assert(sql.get.toString.contains("'2024-01-01 00:00:00'"))
    assert(sql.get.toString.contains(INDEX_INC_FIELD))
    assert(sql.get.toString.contains("* 3600"))
  }

  test("hourlySequence() with custom increment should use specified value") {
    val field = FieldBuilder()
      .name("minute_sequence")
      .hourlySequence("2024-01-01 00:00:00", incrementSeconds = 60)
      .field

    val sql = field.options.get(SQL_GENERATOR)
    assert(sql.isDefined)
    assert(sql.get.toString.contains("* 60"))
  }

  // ========== Integration Tests ==========

  test("date helpers should be chainable") {
    val field = FieldBuilder()
      .name("business_date")
      .withinDays(60)
      .excludeWeekends()
      .field

    assert(field.options.get(MINIMUM).isDefined)
    assert(field.options.get(MAXIMUM).isDefined)
    assert(field.options.get(DATE_EXCLUDE_WEEKENDS).contains("true"))
  }

  // ========== Java Compatibility Tests ==========

  test("Java compatibility - withinDays") {
    val field = FieldBuilder()
      .name("java_date")
      .withinDays()

    assert(field.field.options.get(MINIMUM).isDefined)
    assert(field.field.options.get(MAXIMUM).isDefined)
  }

  test("Java compatibility - futureDays") {
    val field = FieldBuilder()
      .name("java_future")
      .futureDays()

    assert(field.field.options.get(MINIMUM).isDefined)
    assert(field.field.options.get(MAXIMUM).isDefined)
  }

  test("Java compatibility - businessHours") {
    val field = FieldBuilder()
      .name("java_hours")
      .businessHours()

    assert(field.field.options.get(SQL_GENERATOR).isDefined)
  }

  test("Java compatibility - dailySequence") {
    val field = FieldBuilder()
      .name("java_seq")
      .dailySequence("2024-01-01")

    assert(field.field.options.get(SQL_GENERATOR).isDefined)
  }
}
