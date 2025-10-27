package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.core.model.Constants.{CONNECTION_GROUP_DATA_SOURCE, CONNECTION_GROUP_METADATA_SOURCE}
import io.github.datacatering.datacaterer.core.ui.model.{Connection, GetConnectionsResponse, SaveConnectionsRequest}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

class ConnectionRepositoryTest extends AnyFunSuiteLike with BeforeAndAfterAll with MockitoSugar with Matchers {

  private val testKit: ActorTestKit = ActorTestKit()
  // Use unique directory for this test to avoid conflicts with other tests
  private val tempTestDirectory = s"/tmp/data-caterer-test-${java.util.UUID.randomUUID().toString.take(8)}"
  var connectionRepository: ActorRef[ConnectionRepository.ConnectionCommand] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Use unique actor name to avoid conflicts with other tests
    val uniqueActorName = s"connection-repository-${java.util.UUID.randomUUID().toString.take(8)}"
    connectionRepository = testKit.spawn(ConnectionRepository(tempTestDirectory), uniqueActorName)
  }

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    // Clean up test directory
    val testDir = Paths.get(tempTestDirectory).toFile
    if (testDir.exists()) {
      testDir.listFiles().foreach(_.delete())
      testDir.delete()
    }
  }

  test("saveConnection should save connection details correctly") {
    val connection = Connection("testConnection", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)))

    Thread.sleep(10)
    val connectionFile = Path.of(s"$tempTestDirectory/connection/testConnection.csv")
    Files.exists(connectionFile) shouldBe true
  }

  test("getConnection should return correct connection details") {
    cleanFolder()
    val connection = Connection("testConnection", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)))

    Thread.sleep(50) // Wait for async save to complete
    val connectionSaveFolder = s"$tempTestDirectory/connection"
    val retrievedConnection = ConnectionRepository.getConnection("testConnection", connectionSaveFolder)
    retrievedConnection shouldEqual connection
  }

  test("getConnection should throw exception for non-existent connection") {
    val connectionSaveFolder = s"$tempTestDirectory/connection"
    an[Exception] should be thrownBy ConnectionRepository.getConnection("nonExistentConnection", connectionSaveFolder)
  }

  test("getAllConnections should return all saved connections") {
    cleanFolder()
    val connection1 = Connection("testConnection1", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    val connection2 = Connection("testConnection2", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection1, connection2)))
    val probe = testKit.createTestProbe[GetConnectionsResponse]()

    connectionRepository ! ConnectionRepository.GetConnections(Some(CONNECTION_GROUP_DATA_SOURCE), probe.ref)

    probe.receiveMessage() shouldEqual GetConnectionsResponse(List(connection1, connection2))
  }

  test("getAllConnections should filter connections by group type") {
    cleanFolder()
    val connection1 = Connection("testConnection1", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    val connection2 = Connection("testConnection2", "csv", Some(CONNECTION_GROUP_METADATA_SOURCE), Map("key" -> "value"))
    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection1, connection2)))
    val probe = testKit.createTestProbe[GetConnectionsResponse]()

    connectionRepository ! ConnectionRepository.GetConnections(Some(CONNECTION_GROUP_DATA_SOURCE), probe.ref)

    probe.receiveMessage().connections should contain only connection1
  }

  test("removeConnection should delete the specified connection") {
    cleanFolder()
    val connection = Connection("testConnection", "type", Some("group"), Map("key" -> "value"))
    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)))

    connectionRepository ! ConnectionRepository.RemoveConnection("testConnection")
    Thread.sleep(10)
    val connectionFile = Path.of(s"$tempTestDirectory/connection/testConnection.csv")
    Files.exists(connectionFile) shouldBe false
  }

  test("removeConnection should handle non-existent connection gracefully") {
    cleanFolder()
    connectionRepository ! ConnectionRepository.RemoveConnection("nonExistentConnection")
  }

  private def cleanFolder(folder: String = "connection"): Unit = {
    val path = Paths.get(s"$tempTestDirectory/$folder").toFile
    if (path.exists()) {
      path.listFiles().foreach(_.delete())
    }
  }
}
