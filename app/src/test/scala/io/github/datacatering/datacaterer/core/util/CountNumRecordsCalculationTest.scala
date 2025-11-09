package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{Count, LoadPattern, LoadPatternStep, PerFieldCount}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.CountOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Focused unit tests for Count.numRecords calculation with various execution strategies.
 * These tests verify that the correct number of records is calculated based on:
 * - Simple duration + rate
 * - Duration + rate + patterns (ramp, wave, stepped, spike)
 * - Combinations with perField counts
 * 
 * This addresses the issue where integration tests take too long or don't stop running
 * due to incorrect record count calculations for pattern-based execution strategies.
 */
class CountNumRecordsCalculationTest extends AnyFunSuite with Matchers {

  // ====================
  // Basic Duration + Rate Tests (no pattern)
  // ====================
  
  test("numRecords with duration and rate only (no pattern) - simple seconds") {
    val count = Count(
      duration = Some("2s"),
      rate = Some(50),
      rateUnit = Some("1s")
    )
    
    // Expected: 2 seconds * 50 records/second = 100 records
    count.numRecords shouldBe 100L
  }
  
  test("numRecords with duration and rate only - minutes duration") {
    val count = Count(
      duration = Some("1m"),
      rate = Some(30),
      rateUnit = Some("1s")
    )
    
    // Expected: 60 seconds * 30 records/second = 1800 records
    count.numRecords shouldBe 1800L
  }
  
  test("numRecords with duration and rate only - hours duration") {
    val count = Count(
      duration = Some("1h"),
      rate = Some(10),
      rateUnit = Some("1s")
    )
    
    // Expected: 3600 seconds * 10 records/second = 36000 records
    count.numRecords shouldBe 36000L
  }
  
  test("numRecords with duration and rate - fractional result should be converted to long") {
    val count = Count(
      duration = Some("3s"),
      rate = Some(7),
      rateUnit = Some("1s")
    )
    
    // Expected: 3 seconds * 7 records/second = 21 records
    count.numRecords shouldBe 21L
  }
  
  test("numRecords with duration and rate - large values") {
    val count = Count(
      duration = Some("10m"),
      rate = Some(1000),
      rateUnit = Some("1s")
    )
    
    // Expected: 600 seconds * 1000 records/second = 600000 records
    count.numRecords shouldBe 600000L
  }
  
  test("numRecords falls back to records when duration is defined but rate is not") {
    val count = Count(
      records = Some(500L),
      duration = Some("2s"),
      rate = None
    )
    
    // Expected: Falls back to records field = 500
    count.numRecords shouldBe 500L
  }
  
  test("numRecords falls back to records when rate is defined but duration is not") {
    val count = Count(
      records = Some(750L),
      duration = None,
      rate = Some(50)
    )
    
    // Expected: Falls back to records field = 750
    count.numRecords shouldBe 750L
  }
  
  // ====================
  // Ramp Pattern Tests
  // ====================
  
  test("numRecords with ramp pattern - should calculate based on average rate") {
    val count = Count(
      duration = Some("3s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "ramp",
        startRate = Some(20),
        endRate = Some(80)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected calculation:
    // Ramp pattern: average rate = (startRate + endRate) / 2 = (20 + 80) / 2 = 50
    // Total records = duration * average_rate = 3s * 50 = 150 records
    count.numRecords shouldBe 150L
  }
  
  test("numRecords with ramp pattern - startRate = endRate behaves like constant") {
    val count = Count(
      duration = Some("5s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "ramp",
        startRate = Some(100),
        endRate = Some(100)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: Average rate = 100, so 5s * 100 = 500 records
    count.numRecords shouldBe 500L
  }
  
  test("numRecords with ramp pattern - decreasing ramp (stress cooldown)") {
    val count = Count(
      duration = Some("4s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "ramp",
        startRate = Some(200),
        endRate = Some(50)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: Average rate = (200 + 50) / 2 = 125, so 4s * 125 = 500 records
    count.numRecords shouldBe 500L
  }
  
  // ====================
  // Wave Pattern Tests
  // ====================
  
  test("numRecords with wave pattern - should approximate based on baseRate") {
    val count = Count(
      duration = Some("4s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "wave",
        baseRate = Some(50),
        amplitude = Some(30),
        frequency = Some(1.0)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected calculation:
    // Wave pattern oscillates around baseRate with amplitude
    // Over a complete wave cycle, the average is approximately baseRate
    // Total records ≈ duration * baseRate = 4s * 50 = 200 records
    count.numRecords shouldBe 200L
  }
  
  test("numRecords with wave pattern - multiple frequencies") {
    val count = Count(
      duration = Some("10s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "wave",
        baseRate = Some(100),
        amplitude = Some(20),
        frequency = Some(2.0)  // 2 complete waves
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: Average rate = baseRate = 100, so 10s * 100 = 1000 records
    count.numRecords shouldBe 1000L
  }
  
  test("numRecords with wave pattern - zero amplitude behaves like constant") {
    val count = Count(
      duration = Some("5s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "wave",
        baseRate = Some(60),
        amplitude = Some(0),
        frequency = Some(1.0)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: No variation, constant rate = baseRate = 60, so 5s * 60 = 300 records
    count.numRecords shouldBe 300L
  }
  
  // ====================
  // Stepped Pattern Tests
  // ====================
  
  test("numRecords with stepped pattern - single step") {
    val count = Count(
      duration = Some("3s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "stepped",
        steps = Some(List(
          LoadPatternStep(rate = 50, duration = "3s")
        ))
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: Single step at 50 req/s for 3s = 3s * 50 = 150 records
    count.numRecords shouldBe 150L
  }
  
  test("numRecords with stepped pattern - multiple steps with different rates") {
    val count = Count(
      duration = Some("3s"),  // Note: Total duration should match sum of step durations
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "stepped",
        steps = Some(List(
          LoadPatternStep(rate = 20, duration = "1s"),   // 1s * 20 = 20
          LoadPatternStep(rate = 50, duration = "1s"),   // 1s * 50 = 50
          LoadPatternStep(rate = 80, duration = "1s")    // 1s * 80 = 80
        ))
      )),
      rateUnit = Some("1s")
    )
    
    // Expected calculation:
    // Step 1: 1s * 20 = 20 records
    // Step 2: 1s * 50 = 50 records
    // Step 3: 1s * 80 = 80 records
    // Total: 20 + 50 + 80 = 150 records
    count.numRecords shouldBe 150L
  }
  
  test("numRecords with stepped pattern - varying step durations") {
    val count = Count(
      duration = Some("6s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "stepped",
        steps = Some(List(
          LoadPatternStep(rate = 10, duration = "2s"),   // 2s * 10 = 20
          LoadPatternStep(rate = 30, duration = "3s"),   // 3s * 30 = 90
          LoadPatternStep(rate = 50, duration = "1s")    // 1s * 50 = 50
        ))
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: 20 + 90 + 50 = 160 records
    count.numRecords shouldBe 160L
  }
  
  test("numRecords with stepped pattern - steps with minutes") {
    val count = Count(
      duration = Some("2m"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "stepped",
        steps = Some(List(
          LoadPatternStep(rate = 100, duration = "1m"),   // 60s * 100 = 6000
          LoadPatternStep(rate = 200, duration = "1m")    // 60s * 200 = 12000
        ))
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: 6000 + 12000 = 18000 records
    count.numRecords shouldBe 18000L
  }
  
  // ====================
  // Spike Pattern Tests
  // ====================
  
  test("numRecords with spike pattern - single spike") {
    val count = Count(
      duration = Some("10s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "spike",
        baseRate = Some(50),
        spikeRate = Some(500),
        spikeStart = Some(0.5),      // Spike starts at 50% through duration
        spikeDuration = Some(0.1)    // Spike lasts 10% of duration (1s)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected calculation:
    // spikeDuration in seconds: 10s * 0.1 = 1s
    // Base load: 9s * 50 = 450 records
    // Spike: 1s * 500 = 500 records
    // Total: 450 + 500 = 950 records
    count.numRecords shouldBe 950L
  }
  
  test("numRecords with spike pattern - spike at beginning") {
    val count = Count(
      duration = Some("5s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "spike",
        baseRate = Some(20),
        spikeRate = Some(200),
        spikeStart = Some(0.0),      // Spike at start
        spikeDuration = Some(0.2)    // 20% of duration (1s)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: (1s * 200) + (4s * 20) = 200 + 80 = 280 records
    count.numRecords shouldBe 280L
  }
  
  test("numRecords with spike pattern - spike at end") {
    val count = Count(
      duration = Some("8s"),
      rate = None,
      pattern = Some(LoadPattern(
        `type` = "spike",
        baseRate = Some(30),
        spikeRate = Some(300),
        spikeStart = Some(0.75),     // Spike starts at 75% (6s)
        spikeDuration = Some(0.25)   // Spike lasts 25% (2s)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: (6s * 30) + (2s * 300) = 180 + 600 = 780 records
    count.numRecords shouldBe 780L
  }
  
  // ====================
  // Pattern with PerField Tests
  // ====================
  
  test("numRecords with duration, rate, and perField count") {
    val count = Count(
      duration = Some("2s"),
      rate = Some(50),
      perField = Some(PerFieldCount(
        fieldNames = List("account_id"),
        count = Some(5)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected calculation:
    // Base records from duration: 2s * 50 = 100 records
    // Note: perField doesn't multiply duration-based counts currently
    // This is consistent with the implementation
    count.numRecords shouldBe 100L
  }
  
  test("numRecords with ramp pattern and perField count") {
    val count = Count(
      duration = Some("4s"),
      pattern = Some(LoadPattern(
        `type` = "ramp",
        startRate = Some(10),
        endRate = Some(90)
      )),
      perField = Some(PerFieldCount(
        fieldNames = List("user_id"),
        count = Some(3)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected calculation:
    // Ramp average rate: (10 + 90) / 2 = 50
    // Base records: 4s * 50 = 200
    // Note: perField doesn't multiply pattern-based counts
    count.numRecords shouldBe 200L
  }
  
  test("numRecords with stepped pattern and perField count") {
    val count = Count(
      duration = Some("2s"),
      pattern = Some(LoadPattern(
        `type` = "stepped",
        steps = Some(List(
          LoadPatternStep(rate = 40, duration = "1s"),
          LoadPatternStep(rate = 60, duration = "1s")
        ))
      )),
      perField = Some(PerFieldCount(
        fieldNames = List("order_id"),
        count = Some(2)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected:
    // Base: (1s * 40) + (1s * 60) = 100 records
    // Note: perField doesn't multiply pattern-based counts
    count.numRecords shouldBe 100L
  }
  
  test("numRecords with wave pattern and perField count") {
    val count = Count(
      duration = Some("5s"),
      pattern = Some(LoadPattern(
        `type` = "wave",
        baseRate = Some(40),
        amplitude = Some(10),
        frequency = Some(1.0)
      )),
      perField = Some(PerFieldCount(
        fieldNames = List("transaction_id"),
        count = Some(4)
      )),
      rateUnit = Some("1s")
    )
    
    // Expected:
    // Base: 5s * 40 (baseRate) = 200 records
    // Note: perField doesn't multiply pattern-based counts
    count.numRecords shouldBe 200L
  }
  
  // ====================
  // Edge Cases and Fallbacks
  // ====================
  
  test("numRecords with pattern but no duration falls back to default") {
    val count = Count(
      records = Some(1000L),
      pattern = Some(LoadPattern(
        `type` = "ramp",
        startRate = Some(10),
        endRate = Some(50)
      ))
    )
    
    // Expected: Falls back to records field = 1000
    count.numRecords shouldBe 1000L
  }
  
  test("numRecords with empty pattern type falls back to records") {
    val count = Count(
      records = Some(750L),
      duration = Some("3s"),
      pattern = Some(LoadPattern(`type` = "unknown"))
    )
    
    // Expected: Falls back to records field = 750
    count.numRecords shouldBe 750L
  }
  
  test("numRecords with no duration, rate, pattern, or records uses default") {
    val count = Count(
      records = None,
      duration = None,
      rate = None,
      pattern = None
    )
    
    // Expected: Falls back to default (1000)
    count.numRecords shouldBe 1000L
  }
  
  test("numRecords prioritizes duration+rate over records when both present") {
    val count = Count(
      records = Some(5000L),
      duration = Some("2s"),
      rate = Some(100)
    )
    
    // Expected: Duration+rate takes precedence: 2s * 100 = 200 records
    count.numRecords shouldBe 200L
  }
  
  // ====================
  // Complex Realistic Scenarios
  // ====================
  
  test("Realistic HTTP load test scenario - ramp up load") {
    val count = Count(
      duration = Some("3s"),
      pattern = Some(LoadPattern(
        `type` = "ramp",
        startRate = Some(20),
        endRate = Some(80)
      )),
      rateUnit = Some("1s")
    )
    
    // Simulate a realistic HTTP load test ramping from 20 to 80 req/s over 3 seconds
    // Average rate = 50 req/s, total = 150 requests
    count.numRecords shouldBe 150L
  }
  
  test("Realistic breaking point test - stepped increase") {
    val count = Count(
      duration = Some("9s"),
      pattern = Some(LoadPattern(
        `type` = "stepped",
        steps = Some(List(
          LoadPatternStep(rate = 100, duration = "3s"),
          LoadPatternStep(rate = 500, duration = "3s"),
          LoadPatternStep(rate = 1000, duration = "3s")
        ))
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: (3s * 100) + (3s * 500) + (3s * 1000) = 300 + 1500 + 3000 = 4800 records
    count.numRecords shouldBe 4800L
  }
  
  test("Realistic daily traffic pattern - wave with multiple cycles") {
    val count = Count(
      duration = Some("1m"),  // 1 minute to simulate a day in compressed time
      pattern = Some(LoadPattern(
        `type` = "wave",
        baseRate = Some(100),
        amplitude = Some(50),
        frequency = Some(3.0)  // 3 cycles = morning, afternoon, evening peaks
      )),
      rateUnit = Some("1s")
    )
    
    // Expected: 60s * 100 (average = baseRate) = 6000 records
    count.numRecords shouldBe 6000L
  }
}

