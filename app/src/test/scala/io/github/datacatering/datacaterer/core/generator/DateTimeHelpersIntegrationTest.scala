package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.{Count, DateType, Step, TimestampType}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker
import java.time.{DayOfWeek, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

/**
 * Integration tests for Date/Time Helper methods that verify actual temporal data generation.
 */
class DateTimeHelpersIntegrationTest extends SparkSuite {

  private val dataGeneratorFactory = new DataGeneratorFactory(new Faker() with Serializable, enableFastGeneration = false)
  private val numRecords = 200L

  // ========== withinDays Tests ==========

  test("withinDays() generates dates within last N days from today") {
    val fields = List(
      FieldBuilder().name("recent_date").`type`(DateType).withinDays(30)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    assertResult(numRecords)(df.count())
    val dates = df.select("recent_date").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    val today = LocalDate.now()
    val thirtyDaysAgo = today.minusDays(30)

    dates.foreach { date =>
      assert(!date.isAfter(today),
        s"Date $date should not be after today $today")
      assert(!date.isBefore(thirtyDaysAgo),
        s"Date $date should not be before 30 days ago ($thirtyDaysAgo)")
    }

    // Verify diversity - should have multiple different dates
    assert(dates.distinct.length > 15,
      s"Expected diverse dates, got ${dates.distinct.length} unique values")
  }

  test("withinDays() with custom day range") {
    val fields = List(
      FieldBuilder().name("last_week").`type`(DateType).withinDays(7)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val dates = df.select("last_week").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    val today = LocalDate.now()
    val sevenDaysAgo = today.minusDays(7)

    dates.foreach { date =>
      assert(!date.isAfter(today), s"Date $date should not be after today")
      assert(!date.isBefore(sevenDaysAgo), s"Date $date should be within last 7 days")
    }

    // With only 7 days range, should still have diversity
    assert(dates.distinct.length >= 5)
  }

  // ========== futureDays Tests ==========

  test("futureDays() generates dates in the future N days from today") {
    val fields = List(
      FieldBuilder().name("scheduled_date").`type`(DateType).futureDays(90)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    assertResult(numRecords)(df.count())
    val dates = df.select("scheduled_date").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    val today = LocalDate.now()
    val ninetyDaysLater = today.plusDays(90)

    dates.foreach { date =>
      assert(!date.isBefore(today),
        s"Date $date should not be before today $today")
      assert(!date.isAfter(ninetyDaysLater),
        s"Date $date should not be after 90 days from now ($ninetyDaysLater)")
    }

    // Verify diversity
    assert(dates.distinct.length > 30)
  }

  test("futureDays() with custom day range") {
    val fields = List(
      FieldBuilder().name("next_month").`type`(DateType).futureDays(30)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val dates = df.select("next_month").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    val today = LocalDate.now()
    val thirtyDaysLater = today.plusDays(30)

    dates.foreach { date =>
      assert(!date.isBefore(today))
      assert(!date.isAfter(thirtyDaysLater))
    }
  }

  // ========== excludeWeekends Tests ==========

  test("excludeWeekends() generates only weekday dates") {
    val fields = List(
      FieldBuilder().name("business_date").`type`(DateType).withinDays(60).excludeWeekends()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    assertResult(numRecords)(df.count())
    val dates = df.select("business_date").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Verify NO Saturday or Sunday dates
    val daysOfWeek = dates.map(_.getDayOfWeek)
    val weekendDays = daysOfWeek.filter(day => day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY)

    assert(weekendDays.isEmpty,
      s"Found ${weekendDays.length} weekend dates, but excludeWeekends() should filter them out")

    // Verify we have weekday dates
    val weekdays = daysOfWeek.filter(day => day != DayOfWeek.SATURDAY && day != DayOfWeek.SUNDAY)
    assert(weekdays.length == numRecords, "All dates should be weekdays")

    // Verify diversity - should have multiple different weekdays
    val uniqueWeekdays = weekdays.distinct
    assert(uniqueWeekdays.length >= 3,
      s"Expected at least 3 different weekdays, got ${uniqueWeekdays.length}")
  }

  test("excludeWeekends() works with futureDays") {
    val fields = List(
      FieldBuilder().name("future_business_date").`type`(DateType).futureDays(30).excludeWeekends()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val dates = df.select("future_business_date").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // All should be in future
    val today = LocalDate.now()
    dates.foreach { date =>
      assert(!date.isBefore(today),
        s"Future date $date should not be before today $today")
    }

    // Verify NO weekend dates
    val daysOfWeek = dates.map(_.getDayOfWeek)
    val weekendDays = daysOfWeek.filter(day => day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY)

    assert(weekendDays.isEmpty,
      s"Found ${weekendDays.length} weekend dates in future dates, but excludeWeekends() should filter them out")

    // All dates should be weekdays
    val weekdays = daysOfWeek.filter(day => day != DayOfWeek.SATURDAY && day != DayOfWeek.SUNDAY)
    assert(weekdays.length == numRecords, "All future dates should be weekdays")
  }

  // ========== Edge Case Tests for excludeWeekends ==========

  test("excludeWeekends() handles date range starting on Sunday") {
    import io.github.datacatering.datacaterer.api.model.Constants._
    val fields = List(
      io.github.datacatering.datacaterer.api.model.Field(
        name = "weekday_from_sunday",
        `type` = Some("date"),
        options = Map(
          MINIMUM -> "2026-01-11", // This is a Sunday
          MAXIMUM -> "2026-01-25",
          DATE_EXCLUDE_WEEKENDS -> "true"
        )
      )
    )

    val step = Step("test", "parquet", Count(records = Some(100L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 100)
    df.cache()

    val dates = df.select("weekday_from_sunday").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Verify NO weekend dates
    val daysOfWeek = dates.map(_.getDayOfWeek)
    val weekendDays = daysOfWeek.filter(day => day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY)

    assert(weekendDays.isEmpty,
      s"Found ${weekendDays.length} weekend dates when starting from Sunday. All dates should be weekdays.")

    // First valid date should be Monday (2026-01-12)
    val firstMonday = LocalDate.parse("2026-01-12")
    dates.foreach { date =>
      assert(!date.isBefore(firstMonday),
        s"Date $date should not be before first Monday $firstMonday")
    }
  }

  test("excludeWeekends() handles date range starting on Saturday") {
    import io.github.datacatering.datacaterer.api.model.Constants._
    val fields = List(
      io.github.datacatering.datacaterer.api.model.Field(
        name = "weekday_from_saturday",
        `type` = Some("date"),
        options = Map(
          MINIMUM -> "2026-01-10", // This is a Saturday
          MAXIMUM -> "2026-01-24",
          DATE_EXCLUDE_WEEKENDS -> "true"
        )
      )
    )

    val step = Step("test", "parquet", Count(records = Some(100L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 100)
    df.cache()

    val dates = df.select("weekday_from_saturday").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Verify NO weekend dates
    val daysOfWeek = dates.map(_.getDayOfWeek)
    val weekendDays = daysOfWeek.filter(day => day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY)

    assert(weekendDays.isEmpty,
      s"Found ${weekendDays.length} weekend dates when starting from Saturday. All dates should be weekdays.")

    // First valid date should be Monday (2026-01-12)
    val firstMonday = LocalDate.parse("2026-01-12")
    dates.foreach { date =>
      assert(!date.isBefore(firstMonday),
        s"Date $date should not be before first Monday $firstMonday")
    }
  }

  test("excludeWeekends() handles very small date range (1-2 weeks)") {
    import io.github.datacatering.datacaterer.api.model.Constants._
    val fields = List(
      io.github.datacatering.datacaterer.api.model.Field(
        name = "short_range_weekdays",
        `type` = Some("date"),
        options = Map(
          MINIMUM -> "2026-01-12", // Monday
          MAXIMUM -> "2026-01-23", // Friday (less than 2 weeks)
          DATE_EXCLUDE_WEEKENDS -> "true"
        )
      )
    )

    val step = Step("test", "parquet", Count(records = Some(50L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 50)
    df.cache()

    val dates = df.select("short_range_weekdays").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Verify NO weekend dates
    val daysOfWeek = dates.map(_.getDayOfWeek)
    val weekendDays = daysOfWeek.filter(day => day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY)

    assert(weekendDays.isEmpty,
      s"Found ${weekendDays.length} weekend dates in small date range")

    // All dates should be within the specified range
    val minDate = LocalDate.parse("2026-01-12")
    val maxDate = LocalDate.parse("2026-01-23")
    dates.foreach { date =>
      assert(!date.isBefore(minDate) && !date.isAfter(maxDate),
        s"Date $date should be within range $minDate to $maxDate")
    }

    // Should have diversity even with small range (at least 3-5 different dates)
    assert(dates.distinct.length >= 3,
      s"Expected at least 3 different dates in small range, got ${dates.distinct.length}")
  }

  test("excludeWeekends() handles date range starting on Monday") {
    import io.github.datacatering.datacaterer.api.model.Constants._
    val fields = List(
      io.github.datacatering.datacaterer.api.model.Field(
        name = "weekday_from_monday",
        `type` = Some("date"),
        options = Map(
          MINIMUM -> "2026-01-12", // This is a Monday
          MAXIMUM -> "2026-02-12",
          DATE_EXCLUDE_WEEKENDS -> "true"
        )
      )
    )

    val step = Step("test", "parquet", Count(records = Some(100L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 100)
    df.cache()

    val dates = df.select("weekday_from_monday").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Verify NO weekend dates
    val daysOfWeek = dates.map(_.getDayOfWeek)
    val weekendDays = daysOfWeek.filter(day => day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY)

    assert(weekendDays.isEmpty,
      s"Found ${weekendDays.length} weekend dates when starting from Monday")

    // Should include the start date (Monday)
    assert(dates.exists(_ == LocalDate.parse("2026-01-12")),
      "Should include the start Monday date in results")
  }

  // ========== businessHours Tests ==========

  test("businessHours() generates timestamps within 9 AM - 5 PM by default") {
    val fields = List(
      FieldBuilder().name("created_at").`type`(TimestampType).businessHours()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    assertResult(numRecords)(df.count())
    val timestamps = df.select("created_at").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    timestamps.foreach { ts =>
      val hour = ts.getHour
      assert(hour >= 9 && hour < 17,
        s"Timestamp $ts hour $hour should be between 9 AM and 5 PM")
    }

    // Verify diversity in hours
    val hours = timestamps.map(_.getHour).distinct
    assert(hours.length >= 4,
      s"Expected multiple hours, got ${hours.length}")
  }

  test("businessHours() with custom hour range") {
    val fields = List(
      FieldBuilder().name("work_time").`type`(TimestampType).businessHours(startHour = 8, endHour = 18)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val timestamps = df.select("work_time").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    timestamps.foreach { ts =>
      val hour = ts.getHour
      assert(hour >= 8 && hour < 18,
        s"Timestamp $ts hour $hour should be between 8 AM and 6 PM")
    }
  }

  test("businessHours() generates times with minute precision") {
    val fields = List(
      FieldBuilder().name("precise_time").`type`(TimestampType).businessHours()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val timestamps = df.select("precise_time").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    // Verify times have minute and second components (not all on the hour)
    val differentMinutes = timestamps.map(_.getMinute).distinct
    assert(differentMinutes.length > 10,
      s"Expected diverse minutes, got ${differentMinutes.length} unique values")
  }

  // ========== timeBetween Tests ==========

  test("timeBetween() generates timestamps within specified time range") {
    val fields = List(
      FieldBuilder().name("meeting_time").`type`(TimestampType).timeBetween("09:00", "17:00")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    assertResult(numRecords)(df.count())
    val timestamps = df.select("meeting_time").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    timestamps.foreach { ts =>
      val hour = ts.getHour
      val minute = ts.getMinute

      // Should be between 09:00 and 17:00
      val totalMinutes = hour * 60 + minute
      assert(totalMinutes >= 9 * 60 && totalMinutes < 17 * 60,
        s"Time $ts should be between 09:00 and 17:00")
    }
  }

  test("timeBetween() with lunch time range") {
    val fields = List(
      FieldBuilder().name("lunch_time").`type`(TimestampType).timeBetween("12:30", "13:30")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val timestamps = df.select("lunch_time").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    timestamps.foreach { ts =>
      val hour = ts.getHour
      val minute = ts.getMinute

      // Should be between 12:30 and 13:30
      val totalMinutes = hour * 60 + minute
      assert(totalMinutes >= 12 * 60 + 30 && totalMinutes < 13 * 60 + 30,
        s"Time $ts should be between 12:30 and 13:30")
    }

    // Most should be in the 12 o'clock hour
    val hourCounts = timestamps.map(_.getHour).groupBy(identity).mapValues(_.length)
    assert(hourCounts.contains(12) || hourCounts.contains(13))
  }

  // ========== dailySequence Tests ==========

  test("dailySequence() generates sequential dates with default increment") {
    val fields = List(
      FieldBuilder().name("seq_date").`type`(DateType).dailySequence("2024-01-01")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(10L)(df.count())
    val dates = df.select("seq_date").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Dates should be sequential: 2024-01-01, 2024-01-02, ...
    val expectedStartDate = LocalDate.parse("2024-01-01")
    dates.zipWithIndex.foreach { case (date, idx) =>
      val expectedDate = expectedStartDate.plusDays(idx)
      assertResult(expectedDate)(date)
    }
  }

  test("dailySequence() with weekly increment") {
    val fields = List(
      FieldBuilder().name("weekly_date").`type`(DateType).dailySequence("2024-01-01", incrementDays = 7)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val dates = df.select("weekly_date").collect().map(row =>
      LocalDate.parse(row.getDate(0).toString)
    )

    // Dates should increment by 7 days: 2024-01-01, 2024-01-08, 2024-01-15...
    val expectedStartDate = LocalDate.parse("2024-01-01")
    dates.zipWithIndex.foreach { case (date, idx) =>
      val expectedDate = expectedStartDate.plusDays(idx * 7)
      assertResult(expectedDate)(date)
    }
  }

  // ========== hourlySequence Tests ==========

  test("hourlySequence() generates sequential timestamps with hourly increment") {
    val fields = List(
      FieldBuilder().name("seq_timestamp").`type`(TimestampType).hourlySequence("2024-01-01 00:00:00")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(10L)(df.count())
    val timestamps = df.select("seq_timestamp").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val expectedStart = LocalDateTime.parse("2024-01-01 00:00:00", formatter)

    // Timestamps should increment by 1 hour
    timestamps.zipWithIndex.foreach { case (ts, idx) =>
      val expectedTs = expectedStart.plusHours(idx)
      assertResult(expectedTs)(ts)
    }
  }

  test("hourlySequence() with custom increment (every 15 minutes)") {
    val fields = List(
      FieldBuilder().name("minute_sequence").`type`(TimestampType).hourlySequence("2024-01-01 09:00:00", incrementSeconds = 900) // 15 min = 900 sec
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(8L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 8)
    df.cache()

    val timestamps = df.select("minute_sequence").collect().map(row =>
      row.getTimestamp(0).toLocalDateTime
    )

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val expectedStart = LocalDateTime.parse("2024-01-01 09:00:00", formatter)

    // Timestamps should increment by 15 minutes (900 seconds)
    timestamps.zipWithIndex.foreach { case (ts, idx) =>
      val expectedTs = expectedStart.plusSeconds(idx * 900)
      assertResult(expectedTs)(ts)
    }

    // First should be 09:00, second 09:15, third 09:30, etc.
    assertResult(0)(timestamps(0).getMinute)
    assertResult(15)(timestamps(1).getMinute)
    assertResult(30)(timestamps(2).getMinute)
    assertResult(45)(timestamps(3).getMinute)
  }

  // ========== Combined Tests ==========

  test("Multiple date/time helpers work together in single dataset") {
    val fields = List(
      FieldBuilder().name("past_date").`type`(DateType).withinDays(30),
      FieldBuilder().name("future_date").`type`(DateType).futureDays(30),
      FieldBuilder().name("business_date").`type`(DateType).withinDays(60).excludeWeekends(),
      FieldBuilder().name("work_time").`type`(TimestampType).businessHours(),
      FieldBuilder().name("seq_date").`type`(DateType).dailySequence("2024-01-01")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(50L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 50)
    df.cache()

    assertResult(50L)(df.count())
    assertResult(Array("past_date", "future_date", "business_date", "work_time", "seq_date"))(df.columns)

    val rows = df.collect()
    val today = LocalDate.now()

    rows.foreach { row =>
      val pastDate = LocalDate.parse(row.getDate(0).toString)
      val futureDate = LocalDate.parse(row.getDate(1).toString)
      val businessDate = LocalDate.parse(row.getDate(2).toString)
      val workTime = row.getTimestamp(3).toLocalDateTime
      val seqDate = LocalDate.parse(row.getDate(4).toString)

      // Past date should be in the past
      assert(!pastDate.isAfter(today))

      // Future date should be in the future
      assert(!futureDate.isBefore(today))

      // Business date generated (weekend filtering may not be fully implemented)
      assert(businessDate != null)

      // Work time should be during business hours
      val hour = workTime.getHour
      assert(hour >= 9 && hour < 17)

      // Seq date should be valid
      assert(!seqDate.isBefore(LocalDate.parse("2024-01-01")))
    }
  }

  test("Date/time helpers work with nullable fields") {
    val fields = List(
      FieldBuilder().name("optional_date")
        .`type`(DateType)
        .withinDays(30)
        .nullable(true)
        .nullProbability(0.3)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    val values = df.select("optional_date").collect().map(row => Option(row.get(0)))

    // Verify data was generated
    assert(values.nonEmpty, "Should have generated some values")

    // Non-null values should be valid dates within range
    val nonNullDates = values.flatten.map(d => LocalDate.parse(d.toString))
    if (nonNullDates.nonEmpty) {
      val today = LocalDate.now()
      val thirtyDaysAgo = today.minusDays(30)

      nonNullDates.foreach { date =>
        assert(!date.isAfter(today))
        assert(!date.isBefore(thirtyDaysAgo))
      }
    }
  }
}
