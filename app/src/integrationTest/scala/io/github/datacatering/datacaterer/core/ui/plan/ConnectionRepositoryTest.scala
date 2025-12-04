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
    val probe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)), Some(probe.ref))

    // Wait for acknowledgment that save completed
    probe.expectMessage(ConnectionRepository.ConnectionsSaved(1))

    val connectionFile = Path.of(s"$tempTestDirectory/connection/testConnection.csv")
    Files.exists(connectionFile) shouldBe true
  }

  test("getConnection should return correct connection details") {
    cleanFolder()
    val connection = Connection("testConnection", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    val probe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)), Some(probe.ref))
    probe.expectMessage(ConnectionRepository.ConnectionsSaved(1))

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
    val saveProbe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection1, connection2)), Some(saveProbe.ref))
    saveProbe.expectMessage(ConnectionRepository.ConnectionsSaved(2))

    val getProbe = testKit.createTestProbe[GetConnectionsResponse]()
    connectionRepository ! ConnectionRepository.GetConnections(Some(CONNECTION_GROUP_DATA_SOURCE), getProbe.ref)

    getProbe.receiveMessage() shouldEqual GetConnectionsResponse(List(connection1, connection2))
  }

  test("getAllConnections should filter connections by group type") {
    cleanFolder()
    val connection1 = Connection("testConnection1", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    val connection2 = Connection("testConnection2", "csv", Some(CONNECTION_GROUP_METADATA_SOURCE), Map("key" -> "value"))
    val saveProbe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection1, connection2)), Some(saveProbe.ref))
    saveProbe.expectMessage(ConnectionRepository.ConnectionsSaved(2))

    val getProbe = testKit.createTestProbe[GetConnectionsResponse]()
    connectionRepository ! ConnectionRepository.GetConnections(Some(CONNECTION_GROUP_DATA_SOURCE), getProbe.ref)

    getProbe.receiveMessage().connections should contain only connection1
  }

  test("removeConnection should delete the specified connection") {
    cleanFolder()
    val connection = Connection("testConnection", "type", Some("group"), Map("key" -> "value"))
    val probe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)), Some(probe.ref))
    probe.expectMessage(ConnectionRepository.ConnectionsSaved(1))

    connectionRepository ! ConnectionRepository.RemoveConnection("testConnection", Some(probe.ref))
    probe.expectMessage(ConnectionRepository.ConnectionRemoved("testConnection", true))

    val connectionFile = Path.of(s"$tempTestDirectory/connection/testConnection.csv")
    Files.exists(connectionFile) shouldBe false
  }

  test("removeConnection should handle non-existent connection gracefully") {
    cleanFolder()
    val probe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.RemoveConnection("nonExistentConnection", Some(probe.ref))
    probe.expectMessage(ConnectionRepository.ConnectionRemoved("nonExistentConnection", false))
  }

  test("saved connections should have source=file when retrieved") {
    cleanFolder()
    val connection = Connection("testSourceConnection", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    val probe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)), Some(probe.ref))
    probe.expectMessage(ConnectionRepository.ConnectionsSaved(1))

    val connectionSaveFolder = s"$tempTestDirectory/connection"
    val retrievedConnection = ConnectionRepository.getConnection("testSourceConnection", connectionSaveFolder)
    
    // Connections loaded from files should have source = "file"
    retrievedConnection.source shouldEqual "file"
    retrievedConnection.isFromFile shouldBe true
    retrievedConnection.isFromConfig shouldBe false
  }

  test("getAllConnections should return connections with correct source field") {
    cleanFolder()
    val connection = Connection("testSourceConnection2", "csv", Some(CONNECTION_GROUP_DATA_SOURCE), Map("key" -> "value"))
    val saveProbe = testKit.createTestProbe[ConnectionRepository.ConnectionResponse]()

    connectionRepository ! ConnectionRepository.SaveConnections(SaveConnectionsRequest(List(connection)), Some(saveProbe.ref))
    saveProbe.expectMessage(ConnectionRepository.ConnectionsSaved(1))

    val getProbe = testKit.createTestProbe[GetConnectionsResponse]()
    connectionRepository ! ConnectionRepository.GetConnections(Some(CONNECTION_GROUP_DATA_SOURCE), getProbe.ref)

    val response = getProbe.receiveMessage()
    val fileConnections = response.connections.filter(_.name == "testSourceConnection2")
    fileConnections should have size 1
    fileConnections.head.source shouldEqual "file"
    fileConnections.head.isFromFile shouldBe true
  }

  private def cleanFolder(folder: String = "connection"): Unit = {
    val path = Paths.get(s"$tempTestDirectory/$folder").toFile
    if (path.exists()) {
      path.listFiles().foreach(_.delete())
    }
  }
}
