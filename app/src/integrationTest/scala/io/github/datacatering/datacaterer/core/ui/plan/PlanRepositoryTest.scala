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
  // Use unique directory for this test to avoid conflicts with other tests
  private val tempTestDirectory = s"/tmp/data-caterer-test-${java.util.UUID.randomUUID().toString.take(8)}"
  var planRepository: ActorRef[PlanRepository.PlanCommand] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Use unique actor name to avoid conflicts with other tests
    val uniqueActorName = s"plan-repository-${java.util.UUID.randomUUID().toString.take(8)}"
    planRepository = testKit.spawn(PlanRepository(tempTestDirectory), uniqueActorName)
  }

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    // Clean up test directory
    val testDir = Paths.get(tempTestDirectory).toFile
    if (testDir.exists()) {
      Option(testDir.listFiles()).foreach(_.foreach { subDir =>
        Option(subDir.listFiles()).foreach(_.foreach(_.delete()))
        subDir.delete()
      })
      testDir.delete()
    }
  }

  test("savePlan should save plan details correctly") {
    val planRunRequest = PlanRunRequest(
      id = "test-plan-001",
      plan = Plan(name = "test-plan"),
      tasks = List()
    )
    val probe = testKit.createTestProbe[PlanRepository.PlanResponse]()

    planRepository ! PlanRepository.SavePlan(planRunRequest, Some(probe.ref))

    // Wait for acknowledgment that save completed
    probe.expectMessage(PlanRepository.PlanSaved("test-plan"))

    val planFile = Path.of(s"$tempTestDirectory/plan/test-plan.json")
    Files.exists(planFile) shouldBe true
  }

  test("getAllPlans should return saved plans") {
    cleanFolder()
    val plan1 = PlanRunRequest(id = "plan-001", plan = Plan(name = "plan1"))
    val plan2 = PlanRunRequest(id = "plan-002", plan = Plan(name = "plan2"))
    val saveProbe = testKit.createTestProbe[PlanRepository.PlanResponse]()

    planRepository ! PlanRepository.SavePlan(plan1, Some(saveProbe.ref))
    saveProbe.expectMessage(PlanRepository.PlanSaved("plan1"))

    planRepository ! PlanRepository.SavePlan(plan2, Some(saveProbe.ref))
    saveProbe.expectMessage(PlanRepository.PlanSaved("plan2"))

    val getProbe = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(getProbe.ref)

    val plans = getProbe.receiveMessage()
    plans.plans.map(_.plan.name) should contain allOf ("plan1", "plan2")
  }

  test("getPlan should return correct plan details") {
    cleanFolder()
    val planRunRequest = PlanRunRequest(
      id = "get-test-001",
      plan = Plan(name = "test-plan-get")
    )
    val saveProbe = testKit.createTestProbe[PlanRepository.PlanResponse]()

    planRepository ! PlanRepository.SavePlan(planRunRequest, Some(saveProbe.ref))
    saveProbe.expectMessage(PlanRepository.PlanSaved("test-plan-get"))

    val getProbe = testKit.createTestProbe[PlanRunRequest]()
    planRepository ! PlanRepository.GetPlan("test-plan-get", getProbe.ref)

    val retrievedPlan = getProbe.receiveMessage()
    retrievedPlan.plan.name shouldEqual "test-plan-get"
  }

  test("removePlan should delete the specified plan") {
    cleanFolder()
    val plan = PlanRunRequest(id = "remove-test-001", plan = Plan(name = "test-plan-remove"))
    val probe = testKit.createTestProbe[PlanRepository.PlanResponse]()

    planRepository ! PlanRepository.SavePlan(plan, Some(probe.ref))
    probe.expectMessage(PlanRepository.PlanSaved("test-plan-remove"))

    planRepository ! PlanRepository.RemovePlan("test-plan-remove", Some(probe.ref))
    probe.expectMessage(PlanRepository.PlanRemoved("test-plan-remove", true))

    val planFile = Path.of(s"$tempTestDirectory/plan/test-plan-remove.json")
    Files.exists(planFile) shouldBe false
  }

  test("removePlan should handle non-existent plan gracefully") {
    cleanFolder()
    val probe = testKit.createTestProbe[PlanRepository.PlanResponse]()

    planRepository ! PlanRepository.RemovePlan("nonExistentPlan", Some(probe.ref))
    probe.expectMessage(PlanRepository.PlanRemoved("nonExistentPlan", false))
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
    val saveProbe = testKit.createTestProbe[PlanRepository.PlanResponse]()

    planRepository ! PlanRepository.SavePlan(plan1, Some(saveProbe.ref))
    saveProbe.expectMessage(PlanRepository.PlanSaved("overwrite-test"))

    planRepository ! PlanRepository.SavePlan(plan2, Some(saveProbe.ref))
    saveProbe.expectMessage(PlanRepository.PlanSaved("overwrite-test"))

    val getProbe = testKit.createTestProbe[PlanRunRequest]()
    planRepository ! PlanRepository.GetPlan("overwrite-test", getProbe.ref)

    val retrievedPlan = getProbe.receiveMessage()
    retrievedPlan.plan.description shouldBe "version2"
  }

  private def cleanFolder(folder: String = "plan"): Unit = {
    val path = Paths.get(s"$tempTestDirectory/$folder").toFile
    if (path.exists()) {
      path.listFiles().foreach(_.delete())
    }
  }
}
