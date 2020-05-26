import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import dynamoclient.DynamoDBClient
import model.{Employee, EmployeeRepository}

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App{

  implicit val system: ActorSystem = ActorSystem("scanamo-alpakka")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

 val alpakkaClient = new DynamoDBClient

  val employeeRepository = new EmployeeRepository(alpakkaClient)

  employeeRepository.createTable
  employeeRepository.upsert(Employee("1","Utkarsha","Software Consultant")).map{result=>
    println("test1")
    println(result.get)
  }
  employeeRepository.getAll.map{ list=>
    println(list)
  }
}