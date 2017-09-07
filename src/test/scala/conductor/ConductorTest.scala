

package conductor

import org.scalatest.{FlatSpec, OptionValues, Matchers}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.ByteString
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import Directives._


// import conductor.Conductor

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

class ConductorTest extends UnitSpec {
  "Conductor" should "start with empty state" in {
    val conductor = new Conductor
    conductor.state.partitionsByJobId.size shouldBe 0
  }

  "Conductor" should "update state with new partitions" in {
    // This internal reads state and should be removed.
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(job, partitions)
    val partitionsForJob = conductor.state.partitionsByJobId.get(job)
    partitionsForJob.size shouldBe 1
  }

  "Conductor" should "return partitions after update" in {
    // This internal reads state and should be removed.
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(job, partitions)
    conductor.getPriorityPartitions().files.size shouldBe 0
  }

  "Conductor" should "return most common partition" in {
    // This internal reads state and should be removed.
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(JobId(0),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId(1),
                                      List("partition1", "partition3"))
    conductor.getPriorityPartitions().files should contain ("partition1")
    conductor.getPriorityPartitions().files should not contain ("partition2")
    conductor.getPriorityPartitions().files should not contain ("partition3")
  }

  "Conductor" should "accept job updates" in {
    // This internal reads state and should be removed.
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(JobId(0),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId(1),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId(0), List("partition2"))
    conductor.getPriorityPartitions().files shouldBe List("partition2")
    }

}

class ConductorHttpTest extends UnitSpec with ScalatestRouteTest with ConductorService {

  "ConductorApi" should "Accept job updates" in {

      val jsonRequest = ByteString(
        s"""
           |{
           |    "job_id": "7",
           |    "partitions": {"part1","part2","part3"}
           |}
        """.stripMargin)

      val postRequest = HttpRequest(
        HttpMethods.POST,
        uri = "/partitions",
        entity = HttpEntity(MediaTypes.`application/json`, jsonRequest))

      postRequest ~> route ~> check {
           status.isSuccess() shouldEqual true
    }

  }
}
