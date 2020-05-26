package model

import akka.Done
import akka.stream.alpakka.dynamodb.DynamoAttributes
import akka.stream.alpakka.dynamodb.scaladsl.{DynamoClient, DynamoDb}
import akka.stream.scaladsl.Sink
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, DeleteItemResult, KeySchemaElement, KeyType, ProvisionedThroughput, SSESpecification, ScalarAttributeType}
import com.gu.scanamo.{ScanamoAlpakka, Table}
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.query.{KeyEquals, UniqueKey}
import dynamoclient.DynamoDBClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class EmployeeRepository(alpakkaClient: DynamoDBClient) {
  val client: DynamoClient = alpakkaClient.alpakkaClient
  val tableName = "employees"
  val table = Table[Employee](tableName)

  override def createTable : Future[Done] =
  {
    val createTableRequest : CreateTableRequest = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(
        new KeySchemaElement().withAttributeName("employeeId").withKeyType(KeyType.HASH)
      )
      .withAttributeDefinitions(
        new AttributeDefinition().withAttributeName("employeeId").withAttributeType(ScalarAttributeType.S)
      )
      .withProvisionedThroughput(
        new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
      )

    DynamoDb.source(request)
      .withAttributes(DynamoAttributes.client(m_client))
      .runWith(Sink.head).flatMap(_ => retry(waitForDynamoTableToBeActive(tableName)))
  }

  def getEmployee(id: String): Future[Option[Either[DynamoReadError, Employee]]] = {
    ScanamoAlpakka.get[Employee](client)(tableName)(UniqueKey(KeyEquals('employeeId, id)))
  }

  def upsert(employee: Employee): Future[Option[Either[DynamoReadError, Employee]]] = {
    println("test")
    ScanamoAlpakka.put(client)(tableName)(employee)
  }

  def getAll: Future[List[Either[DynamoReadError, Employee]]] = {
    ScanamoAlpakka.scan[Employee](client)(tableName)
  }

  def delete(id: String): Future[DeleteItemResult] = {
    ScanamoAlpakka.delete[Employee](client)(tableName)(UniqueKey(KeyEquals('id, id)))
  }
}
