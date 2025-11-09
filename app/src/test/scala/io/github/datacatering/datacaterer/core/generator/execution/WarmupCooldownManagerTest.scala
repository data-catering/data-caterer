package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Plan, TestConfig}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WarmupCooldownManagerTest extends AnyFunSuite with Matchers {

  // Controllable time provider for deterministic testing
  class MockTimeProvider {
    private var currentTime: Long = 0L

    def getCurrentTime: Long = currentTime
    def advance(millis: Long): Unit = currentTime += millis
    def reset(): Unit = currentTime = 0L
  }

  test("No warmup or cooldown configured") {
    val plan = Plan(testConfig = None)
    val manager = new WarmupCooldownManager(plan)

    manager.hasWarmup shouldBe false
    manager.hasCooldown shouldBe false
  }

  test("Warmup configured") {
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("30s"))))
    val manager = new WarmupCooldownManager(plan)

    manager.hasWarmup shouldBe true
    manager.hasCooldown shouldBe false
    manager.getWarmupDurationMs shouldBe 30000
  }

  test("Cooldown configured") {
    val plan = Plan(testConfig = Some(TestConfig(cooldown = Some("10s"))))
    val manager = new WarmupCooldownManager(plan)

    manager.hasWarmup shouldBe false
    manager.hasCooldown shouldBe true
    manager.getCooldownDurationMs shouldBe 10000
  }

  test("Both warmup and cooldown configured") {
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("1m"), cooldown = Some("30s"))))
    val manager = new WarmupCooldownManager(plan)

    manager.hasWarmup shouldBe true
    manager.hasCooldown shouldBe true
    manager.getWarmupDurationMs shouldBe 60000
    manager.getCooldownDurationMs shouldBe 30000
  }

  test("Warmup phase detection with mock time") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("1m"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    // Not started yet
    manager.isInWarmupPhase shouldBe false

    // Start test at time 0
    manager.startTest()
    manager.isInWarmupPhase shouldBe true
    manager.isWarmupComplete shouldBe false

    // Advance time by 30 seconds (still in warmup)
    mockTime.advance(30000)
    manager.isInWarmupPhase shouldBe true
    manager.isWarmupComplete shouldBe false

    // Advance time by another 30 seconds (warmup complete)
    mockTime.advance(30000)
    manager.isInWarmupPhase shouldBe false
    manager.isWarmupComplete shouldBe true
  }

  test("Execution phase detection with mock time") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("1m"), cooldown = Some("30s"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    manager.startTest()

    // During warmup
    manager.isInExecutionPhase shouldBe false
    manager.isInWarmupPhase shouldBe true

    // After warmup completes
    mockTime.advance(60000)
    manager.isInExecutionPhase shouldBe true
    manager.isInWarmupPhase shouldBe false
    manager.isInCooldownPhase shouldBe false
  }

  test("Cooldown phase detection with mock time") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(cooldown = Some("30s"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    manager.startTest()

    // Advance some time for execution
    mockTime.advance(60000)

    // End execution
    manager.endExecution()
    manager.isInCooldownPhase shouldBe true
    manager.isInExecutionPhase shouldBe false
    manager.isCooldownComplete shouldBe false

    // Advance time through cooldown
    mockTime.advance(30000)
    manager.isInCooldownPhase shouldBe false
    manager.isCooldownComplete shouldBe true
  }

  test("Parse duration formats") {
    val testCases = List(
      ("30s", 30000L),
      ("5m", 300000L),
      ("1h", 3600000L),
      ("2m30s", 150000L),
      ("1h30m", 5400000L)
    )

    testCases.foreach { case (duration, expectedMs) =>
      val plan = Plan(testConfig = Some(TestConfig(warmup = Some(duration))))
      val manager = new WarmupCooldownManager(plan)
      manager.hasWarmup shouldBe true
      manager.getWarmupDurationMs shouldBe expectedMs
    }
  }

  test("Get current phase with mock time") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("1m"), cooldown = Some("30s"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    // Not started
    manager.getCurrentPhase shouldBe "not started"

    // Start test - in warmup
    manager.startTest()
    manager.getCurrentPhase shouldBe "warmup"

    // After warmup - in execution
    mockTime.advance(60000)
    manager.getCurrentPhase shouldBe "execution"

    // After execution ends - in cooldown
    manager.endExecution()
    manager.getCurrentPhase shouldBe "cooldown"

    // After cooldown
    mockTime.advance(30000)
    manager.getCurrentPhase shouldBe "execution" // Falls back to execution when all phases complete
  }

  test("Should start cooldown check") {
    val plan = Plan(testConfig = Some(TestConfig(cooldown = Some("10s"))))
    val manager = new WarmupCooldownManager(plan)

    manager.startTest()
    manager.shouldStartCooldown(mainExecutionComplete = false) shouldBe false
    manager.shouldStartCooldown(mainExecutionComplete = true) shouldBe true

    // After execution ends, should not start cooldown again
    manager.endExecution()
    manager.shouldStartCooldown(mainExecutionComplete = true) shouldBe false
  }

  test("No cooldown - should not start cooldown") {
    val plan = Plan(testConfig = None)
    val manager = new WarmupCooldownManager(plan)

    manager.startTest()
    manager.shouldStartCooldown(mainExecutionComplete = true) shouldBe false
  }

  test("Get summary") {
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("30s"), cooldown = Some("10s"))))
    val manager = new WarmupCooldownManager(plan)

    val summary = manager.getSummary
    summary should include("warmup=30s")
    summary should include("cooldown=10s")
  }

  test("No warmup/cooldown summary") {
    val plan = Plan(testConfig = None)
    val manager = new WarmupCooldownManager(plan)

    val summary = manager.getSummary
    summary should include("warmup=none")
    summary should include("cooldown=none")
  }

  test("Get remaining warmup time with mock time") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("1m"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    // Before start
    manager.getRemainingWarmupTime shouldBe 0L

    // After start
    manager.startTest()
    manager.getRemainingWarmupTime shouldBe 60000L

    // Advance 30 seconds
    mockTime.advance(30000)
    manager.getRemainingWarmupTime shouldBe 30000L

    // Advance past warmup
    mockTime.advance(30000)
    manager.getRemainingWarmupTime shouldBe 0L
  }

  test("Get remaining cooldown time with mock time") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(cooldown = Some("30s"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    manager.startTest()
    mockTime.advance(60000) // Some execution time

    // Before cooldown starts
    manager.getRemainingCooldownTime shouldBe 0L

    // Start cooldown
    manager.endExecution()
    manager.getRemainingCooldownTime shouldBe 30000L

    // Advance 15 seconds
    mockTime.advance(15000)
    manager.getRemainingCooldownTime shouldBe 15000L

    // Advance past cooldown
    mockTime.advance(15000)
    manager.getRemainingCooldownTime shouldBe 0L
  }

  test("Test lifecycle from start to finish") {
    val mockTime = new MockTimeProvider
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("30s"), cooldown = Some("10s"))))
    val manager = new WarmupCooldownManager(plan, () => mockTime.getCurrentTime)

    // Phase 1: Not started
    manager.getCurrentPhase shouldBe "not started"
    manager.hasWarmup shouldBe true
    manager.hasCooldown shouldBe true

    // Phase 2: Start test (warmup begins)
    manager.startTest()
    manager.getCurrentPhase shouldBe "warmup"
    manager.getTestStartTime shouldBe Some(0L)
    manager.getWarmupEndTime shouldBe Some(30000L)

    // Phase 3: During warmup
    mockTime.advance(15000)
    manager.isInWarmupPhase shouldBe true
    manager.getRemainingWarmupTime shouldBe 15000L

    // Phase 4: Warmup complete, execution begins
    mockTime.advance(15000)
    manager.isWarmupComplete shouldBe true
    manager.getCurrentPhase shouldBe "execution"

    // Phase 5: During execution
    mockTime.advance(60000)
    manager.isInExecutionPhase shouldBe true

    // Phase 6: End execution, cooldown begins
    manager.endExecution()
    manager.getExecutionEndTime shouldBe Some(90000L)
    manager.getCurrentPhase shouldBe "cooldown"
    manager.isInCooldownPhase shouldBe true

    // Phase 7: During cooldown
    mockTime.advance(5000)
    manager.getRemainingCooldownTime shouldBe 5000L

    // Phase 8: Cooldown complete
    mockTime.advance(5000)
    manager.isCooldownComplete shouldBe true
    manager.isInCooldownPhase shouldBe false
  }

  test("Warmup complete with no warmup configured") {
    val plan = Plan(testConfig = None)
    val manager = new WarmupCooldownManager(plan)

    manager.isWarmupComplete shouldBe true // No warmup = always complete
  }

  test("Cooldown complete with no cooldown configured") {
    val plan = Plan(testConfig = None)
    val manager = new WarmupCooldownManager(plan)

    manager.isCooldownComplete shouldBe true // No cooldown = always complete
  }

  test("Multiple duration units in single string") {
    val plan = Plan(testConfig = Some(TestConfig(warmup = Some("1h30m45s"))))
    val manager = new WarmupCooldownManager(plan)

    val expected = (1 * 3600 + 30 * 60 + 45) * 1000L // Convert to milliseconds
    manager.getWarmupDurationMs shouldBe expected
  }
}
