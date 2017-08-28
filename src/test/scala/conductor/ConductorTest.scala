

package conductor

import org.scalatest.{FlatSpec, OptionValues, Matchers}
// import conductor.Conductor

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

class ConductorTest extends UnitSpec {
  "Conductor" should "start with empty state" in {
    val conductor = new Conductor
    conductor.state.partitionsByJobId.size shouldBe 0
  }

  "Conductor" should "Accept new partitions from a job" in {
    val conductor = new Conductor
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    conductor.refreshPartitionsForJob(job, partitions)
    conductor.state.partitionsByJobId.get(job).size shouldBe 1
  }

  "Conductor" should "Prioritize top partitions" in {
    val conductor = new Conductor
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    conductor.refreshPartitionsForJob(job, partitions)
    conductor.getPriorityPartitions().files.size shouldBe 2
  }

}
