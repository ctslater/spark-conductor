

package conductor

import org.scalatest.{FlatSpec, OptionValues, Matchers}
// import conductor.Conductor

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

class ConductorTest extends UnitSpec {
  "Conductor" should "start with empty state" in {
    val conductor = new Conductor
    conductor.state.partitionsByJobId.size shouldBe 0
  }

  "Conductor" should "Update state with new partitions" in {
    // This internal reads state and should be removed.
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(job, partitions)
    val partitionsForJob = conductor.state.partitionsByJobId.get(job)
    partitionsForJob.size shouldBe 1
  }

  "Conductor" should "Return partitions after update" in {
    // This internal reads state and should be removed.
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(job, partitions)
    conductor.getPriorityPartitions().files.size shouldBe 0
  }

  "Conductor" should "Return most common partition" in {
    // This internal reads state and should be removed.
    val job = JobId(0)
    val partitions = List("partition1", "partition2")
    val conductor = new Conductor
    conductor.refreshPartitionsForJob(JobId(0),
                                      List("partition1", "partition2"))
    conductor.refreshPartitionsForJob(JobId(1),
                                      List("partition1", "partition3"))
    conductor.getPriorityPartitions().files should contain ("partition1")
    conductor.getPriorityPartitions().files should not contain ("partition2")
    conductor.getPriorityPartitions().files should not contain ("partition3")
  }

}
