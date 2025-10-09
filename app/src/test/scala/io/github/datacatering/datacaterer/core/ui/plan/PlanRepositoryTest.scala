package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.Constants.{CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH, PATH}
import io.github.datacatering.datacaterer.api.model.{Count, Plan, Step, TaskSummary, YamlDataSourceValidation, YamlValidationConfiguration}
import io.github.datacatering.datacaterer.api.{FieldBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.model.Constants.CONNECTION_GROUP_DATA_SOURCE
import io.github.datacatering.datacaterer.core.ui.model.{ConfigurationRequest, Connection, PlanRunExecution, PlanRunRequest, PlanRunRequests, SaveConnectionsRequest}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Args, BeforeAndAfterAll, Status}

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class PlanRepositoryTest extends SparkSuite with BeforeAndAfterAll with MockitoSugar with Matchers {

  private val testKit: ActorTestKit = ActorTestKit()
  private val tempTestDirectory = "/tmp/data-caterer-plan-repository-test"
  var planRepository: ActorRef[PlanRepository.PlanCommand] = _
  var connectionRepository: ActorRef[ConnectionRepository.ConnectionCommand] = _

  override def runTests(testName: Option[String], args: Args): Status = {
    testNames.map { test => runTest(test, args) }.last
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("data-caterer-install-dir", tempTestDirectory)
    planRepository = testKit.spawn(PlanRepository(), "plan-repository")
    connectionRepository = testKit.spawn(ConnectionRepository(), "connection-repository")
  }

  override def afterAll(): Unit = testKit.shutdownTestKit()

  test("runPlan should handle successful plan execution with no generation or validation") {
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    val probe = testKit.createTestProbe[PlanResponseHandler.Response]()

    planRepository ! PlanRepository.RunPlan(planRunRequest, probe.ref)

    probe.expectMessage(PlanResponseHandler.OK)
  }
  test("runPlan should handle successful plan execution with generation and validation") {
    cleanFolder("record-tracking-valid")
    cleanFolder("data/my-csv")
    cleanFolder("connection")
    val plan = Plan("name", tasks = List(TaskSummary("task1", "my-csv")), validations = List("my-validation"))
    val tasks = List(Step(
      "task1",
      "csv",
      Count(Some(10)),
      Map(PATH -> s"$tempTestDirectory/data/my-csv", "header" -> "true"),
      List(FieldBuilder().name("name").field)
    ))
    val validations = List(YamlValidationConfiguration(
      "my-validation",
      "desc",
      Map(
        "my-csv" -> List(YamlDataSourceValidation(
          Map(PATH -> s"$tempTestDirectory/data/my-csv", "header" -> "true"),
          validations = List(
            ValidationBuilder().field("name").isNull(true).validation,
            ValidationBuilder().count().isEqual(10).validation
          )
        ))
      )))
    val configuration = ConfigurationRequest(folder = Map(CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH -> s"$tempTestDirectory/record-tracking-valid"))
    val planRunRequest = PlanRunRequest("id", plan, tasks, validations, Some(configuration))
    val probe = testKit.createTestProbe[PlanResponseHandler.Response]()

    planRepository ! PlanRepository.RunPlan(planRunRequest, probe.ref)

    probe.receiveMessage(FiniteDuration(60, TimeUnit.SECONDS)) shouldBe a[PlanResponseHandler.KO]
    val saveConnectionsRequest = SaveConnectionsRequest(List(Connection("my-csv", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map(PATH -> s"$tempTestDirectory/data/my-csv"))))
    connectionRepository ! ConnectionRepository.SaveConnections(saveConnectionsRequest)

    Thread.sleep(10)
    val probe1 = testKit.createTestProbe[PlanResponseHandler.Response]()
    planRepository ! PlanRepository.RunPlan(planRunRequest, probe1.ref)

    probe1.expectMessage(FiniteDuration(60, TimeUnit.SECONDS), PlanResponseHandler.OK)
    sparkSession.read.option("header", "true").csv(s"$tempTestDirectory/data/my-csv").count() shouldBe 10
  }

  test("runPlanDeleteData should handle successful plan execution with no generation or validation") {
    val configurationRequest = ConfigurationRequest()
    val planRunRequest = PlanRunRequest("id", Plan("name"), configuration = Some(configurationRequest))
    val probe = testKit.createTestProbe[PlanResponseHandler.Response]()

    planRepository ! PlanRepository.RunPlanDeleteData(planRunRequest, probe.ref)

    probe.expectMessage(PlanResponseHandler.OK)
  }

  test("savePlan should save plan to file") {
    cleanPlansFolder()
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    planRepository ! PlanRepository.SavePlan(planRunRequest)
  }

  test("getPlans should get non-empty plans") {
    cleanPlansFolder()
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    planRepository ! PlanRepository.SavePlan(planRunRequest)

    val probe = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(probe.ref)
    val res = probe.receiveMessage().plans
    // Should include at least the saved plan, may include YAML plans from test resources
    res.size should be >= 1
    res.find(_.plan.name == planRunRequest.plan.name) shouldBe Some(planRunRequest)

    val probe2 = testKit.createTestProbe[PlanRunRequest]()
    planRepository ! PlanRepository.GetPlan(planRunRequest.plan.name, probe2.ref)
    probe2.receiveMessage() shouldBe planRunRequest
  }

  test("getPlanRunStatus should return None for non-existent plan") {
    cleanPlansFolder()
    val probe = testKit.createTestProbe[PlanRunExecution]()
    planRepository ! PlanRepository.GetPlanRunStatus("non-existent-plan", probe.ref)
    probe.expectNoMessage()
  }

  test("getPlanRunStatus should return execution status for existing plan") {
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    val probe1 = testKit.createTestProbe[PlanResponseHandler.Response]()
    planRepository ! PlanRepository.RunPlan(planRunRequest, probe1.ref)
    probe1.expectMessage(PlanResponseHandler.OK)

    val probe = testKit.createTestProbe[PlanRunExecution]()
    planRepository ! PlanRepository.GetPlanRunStatus(planRunRequest.id, probe.ref)
    val res = probe.receiveMessage()
    res.id shouldBe planRunRequest.id
    res.name shouldBe planRunRequest.plan.name
    res.status shouldBe "finished"
    res.generationSummary.size shouldBe 1
    res.validationSummary.size shouldBe 1
    res.failedReason shouldBe Some("")
  }

  test("getPlanRunReportPath should return None for non-existent plan") {
    cleanPlansFolder()
    val probe = testKit.createTestProbe[String]()
    planRepository ! PlanRepository.GetPlanRunReportPath("non-existent-plan", probe.ref)
    probe.expectNoMessage()
  }

  test("getPlanRunReportPath should return report path for existing plan") {
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    val probe1 = testKit.createTestProbe[PlanResponseHandler.Response]()
    planRepository ! PlanRepository.RunPlan(planRunRequest, probe1.ref)
    probe1.expectMessage(PlanResponseHandler.OK)

    val probe = testKit.createTestProbe[String]()
    planRepository ! PlanRepository.GetPlanRunReportPath(planRunRequest.id, probe.ref)
    probe.receiveMessage() should include("/opt/app/report")
  }

  test("removePlan should remove plan") {
    cleanPlansFolder()
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    planRepository ! PlanRepository.SavePlan(planRunRequest)

    val probe = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(probe.ref)
    val initialPlans = probe.receiveMessage().plans
    initialPlans.find(_.plan.name == planRunRequest.plan.name) shouldBe defined

    planRepository ! PlanRepository.RemovePlan(planRunRequest.plan.name)

    val probe2 = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(probe2.ref)
    val plansAfterRemoval = probe2.receiveMessage().plans
    plansAfterRemoval.find(_.plan.name == planRunRequest.plan.name) shouldBe None
  }

  test("removePlan should not remove plan if it does not exist") {
    cleanPlansFolder()
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    planRepository ! PlanRepository.SavePlan(planRunRequest)

    val probe = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(probe.ref)
    val initialPlans = probe.receiveMessage().plans
    val initialCount = initialPlans.size
    initialPlans.find(_.plan.name == planRunRequest.plan.name) shouldBe defined

    planRepository ! PlanRepository.RemovePlan("non-existent-plan")

    val probe2 = testKit.createTestProbe[PlanRunRequests]()
    planRepository ! PlanRepository.GetPlans(probe2.ref)
    val plansAfterRemoval = probe2.receiveMessage().plans
    plansAfterRemoval.size shouldBe initialCount
    plansAfterRemoval.find(_.plan.name == planRunRequest.plan.name) shouldBe defined
  }

  test("getPlanRuns should get non-empty plan runs") {
    cleanPlansFolder()
    cleanExecutionsFolder()
    val planRunRequest = PlanRunRequest("id", Plan("name"))
    val probe1 = testKit.createTestProbe[PlanResponseHandler.Response]()
    planRepository ! PlanRepository.RunPlan(planRunRequest, probe1.ref)
    probe1.expectMessage(PlanResponseHandler.OK)

    val probe = testKit.createTestProbe[PlanRepository.PlanRunExecutionDetails]()
    planRepository ! PlanRepository.GetPlanRuns(probe.ref)
    val res = probe.receiveMessage().planExecutionByPlan
    res.size shouldBe 1
    res.head.name shouldBe planRunRequest.plan.name
    res.head.executions.size shouldBe 1
    res.head.executions.head.id shouldBe planRunRequest.id
  }

  test("startupSpark should start spark session") {
    planRepository ! PlanRepository.StartupSpark()
  }

  private def cleanFolder(folder: String = "plan"): Unit = {
    val path = Paths.get(s"$tempTestDirectory/$folder").toFile
    if (path.exists()) {
      path.listFiles().foreach(_.delete())
    }
  }

  private def cleanPlansFolder(): Unit = {
    cleanFolder()
  }

  private def cleanExecutionsFolder(): Unit = {
    cleanFolder("execution")
  }
}
