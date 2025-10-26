package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.Plan
import io.github.datacatering.datacaterer.core.ui.model.{PlanRunExecution, PlanRunRequest, PlanRunRequests}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

class PlanRepositoryTest extends AnyFunSuiteLike with BeforeAndAfterAll with MockitoSugar with Matchers {

  private val testKit: ActorTestKit = ActorTestKit()
  private val tempTestDirectory = "/tmp/data-caterer-plan-repository-test"
  var planRepository: ActorRef[PlanRepository.PlanCommand] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("data-caterer-install-dir", tempTestDirectory)
    planRepository = testKit.spawn(PlanRepository(), "plan-repository")
  }

  override def afterAll(): Unit = testKit.shutdownTestKit()

  test("savePlan should save plan details correctly") {
    val planRunRequest = PlanRunRequest(
      id = "test-plan-001",
      plan = Plan(name = "test-plan"),
      tasks = List()
    )

    planRepository ! PlanRepository.SavePlan(planRunRequest)

    Thread.sleep(200)
    val planFile = Path.of(s"$tempTestDirectory/plan/test-plan.json")
    Files.exists(planFile) shouldBe true
  }

  test("getAllPlans should return saved plans") {
    cleanFolder()
    val plan1 = PlanRunRequest(id = "plan-001", plan = Plan(name = "plan1"))
    val plan2 = PlanRunRequest(id = "plan-002", plan = Plan(name = "plan2"))

    planRepository ! PlanRepository.SavePlan(plan1)
    planRepository ! PlanRepository.SavePlan(plan2)

    Thread.sleep(150)

    val probe = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(probe.ref)

    val plans = probe.receiveMessage()
    plans.plans.map(_.plan.name) should contain allOf ("plan1", "plan2")
  }

  test("getPlan should return correct plan details") {
    cleanFolder()
    val planRunRequest = PlanRunRequest(
      id = "get-test-001",
      plan = Plan(name = "test-plan-get")
    )
    planRepository ! PlanRepository.SavePlan(planRunRequest)

    Thread.sleep(100)

    val probe = testKit.createTestProbe[PlanRunRequest]()
    planRepository ! PlanRepository.GetPlan("test-plan-get", probe.ref)

    val retrievedPlan = probe.receiveMessage()
    retrievedPlan.plan.name shouldEqual "test-plan-get"
  }

  test("removePlan should delete the specified plan") {
    cleanFolder()
    val plan = PlanRunRequest(id = "remove-test-001", plan = Plan(name = "test-plan-remove"))
    planRepository ! PlanRepository.SavePlan(plan)

    Thread.sleep(100)

    planRepository ! PlanRepository.RemovePlan("test-plan-remove")
    Thread.sleep(100)
    val planFile = Path.of(s"$tempTestDirectory/plan/test-plan-remove.json")
    Files.exists(planFile) shouldBe false
  }

  test("removePlan should handle non-existent plan gracefully") {
    cleanFolder()
    planRepository ! PlanRepository.RemovePlan("nonExistentPlan")
    // Should not throw exception
  }

  test("getPlanRunStatus should handle non-existent execution") {
    val probe = testKit.createTestProbe[PlanRunExecution]()
    val executionId = "test-exec-123"

    // Should not crash when execution doesn't exist
    noException should be thrownBy {
      planRepository ! PlanRepository.GetPlanRunStatus(executionId, probe.ref)
    }
  }

  test("getPlanRuns should return execution history") {
    val probe = testKit.createTestProbe[PlanRepository.PlanRunExecutionDetails]()

    planRepository ! PlanRepository.GetPlanRuns(probe.ref)

    val response = probe.receiveMessage()
    response shouldBe a[PlanRepository.PlanRunExecutionDetails]
  }

  test("savePlan should overwrite existing plan with same name") {
    cleanFolder()
    val plan1 = PlanRunRequest(id = "overwrite-001", plan = Plan(name = "overwrite-test", description = "version1"))
    val plan2 = PlanRunRequest(id = "overwrite-002", plan = Plan(name = "overwrite-test", description = "version2"))

    planRepository ! PlanRepository.SavePlan(plan1)
    Thread.sleep(100)

    planRepository ! PlanRepository.SavePlan(plan2)
    Thread.sleep(100)

    val probe = testKit.createTestProbe[PlanRunRequest]()
    planRepository ! PlanRepository.GetPlan("overwrite-test", probe.ref)

    val retrievedPlan = probe.receiveMessage()
    retrievedPlan.plan.description shouldBe "version2"
  }

  private def cleanFolder(folder: String = "plan"): Unit = {
    val path = Paths.get(s"$tempTestDirectory/$folder").toFile
    if (path.exists()) {
      path.listFiles().foreach(_.delete())
    }
  }
}
