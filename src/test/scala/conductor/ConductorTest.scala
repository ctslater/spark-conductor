

package conductor

import org.scalatest.{FlatSpec, OptionValues, Matchers}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.ByteString
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import Directives._

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

class ConductorTest extends UnitSpec {
  "Conductor" should "start with empty state" in {
    val conductor = new Conductor
    conductor.state.partitionsByJobId.size shouldBe 0
  }

  "Conductor" should "update state with new partitions" in {
    // This internal reads state and should be removed.
    val job = JobId("0")
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(job, partitions)
    val partitionsForJob = conductor.state.partitionsByJobId.get(job)
    partitionsForJob.size shouldBe 1
  }

  "Conductor" should "return partitions after update" in {
    // This internal reads state and should be removed.
    val job = JobId("0")
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(job, partitions)
    conductor.getPriorityPartitions().size shouldBe 0
  }

  "Conductor" should "return most common partition" in {
    // This internal reads state and should be removed.
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(JobId("0"),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId("1"),
                                      List("partition1", "partition3"))
    conductor.getPriorityPartitions() should contain ("partition1")
    conductor.getPriorityPartitions() should not contain ("partition2")
    conductor.getPriorityPartitions() should not contain ("partition3")
  }

  "Conductor" should "accept job updates" in {
    // This internal reads state and should be removed.
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(JobId("0"),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId("1"),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId("0"), List("partition2"))
    conductor.getPriorityPartitions() shouldBe List("partition2")
    }

  "Conductor" should "only return a small number of priority partitions" in {
    // This internal reads state and should be removed.
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(JobId("0"),
                                      Range(0,30).map(n => s"part$n").toList)
    conductor.refreshPartitionsForJob(JobId("1"),
                                      Range(0,30).map(n => s"part$n").toList)
    conductor.getPriorityPartitions().size should be < 15
    }

}

class ConductorHttpTest extends UnitSpec with ScalatestRouteTest with ConductorService {

  val jsonJob1 = ByteString(
    s"""
       |{
       |    "job_id": "1",
       |    "partitions": ["part1","part2","part3"]
       |}
    """.stripMargin)

  val jsonJob2 = ByteString(
    s"""
       |{
       |    "job_id": "2",
       |    "partitions": ["part3","part4","part5"]
       |}
    """.stripMargin)

  "ConductorApi" should "accept a first job" in {
      val postRequest = HttpRequest(
        HttpMethods.POST,
        uri = "/partitions",
        entity = HttpEntity(MediaTypes.`application/json`, jsonJob1))

      postRequest ~> route ~> check {
        conductor.getAllPartitions() should contain ("part1")
    }
  }

  "ConductorApi" should "accept a second job" in {
      HttpRequest(
        HttpMethods.POST,
        uri = "/partitions",
        entity = HttpEntity(MediaTypes.`application/json`, jsonJob1))

      val postRequest = HttpRequest(
        HttpMethods.POST,
        uri = "/partitions",
        entity = HttpEntity(MediaTypes.`application/json`, jsonJob2))

      postRequest ~> route ~> check {
        conductor.getAllPartitions() should contain ("part4")
        conductor.getPriorityPartitions() shouldBe List("part3")
    }
  }

  "ConductorApi" should "report priority partitions" in {

    Seq(jsonJob1, jsonJob2).map(json =>
      HttpRequest(
        HttpMethods.POST,
        uri = "/partitions",
        entity = HttpEntity(MediaTypes.`application/json`, json)))

      val postRequest = HttpRequest( HttpMethods.GET, uri = "/priority")
      postRequest ~> route ~> check {
        responseAs[PriorityPartitionsResponse].files shouldBe List("part3")
    }

  }

}
