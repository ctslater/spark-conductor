
package conductor


import scala.collection.mutable.HashMap

case class PriorityPartitions(files: List[String])

case class PartitionsRequiredForJob(jobId: JobId,
                                    partitions: List[String],
                                    lastRefreshed: String)

case class JobId(jobIdNumber: Integer)

class SystemState {
  val partitionsByJobId: HashMap[JobId, PartitionsRequiredForJob] = HashMap()
}

class Conductor {
  val state = new SystemState

  def refreshPartitionsForJob(job: JobId, partitions: List[String]): Unit = {

    state.partitionsByJobId.update(job, PartitionsRequiredForJob(job,
                                                                 partitions,
                                                                 "datetime"))
  }

  def getPriorityPartitions(): PriorityPartitions = {
    PriorityPartitions(state.partitionsByJobId.values.head.partitions)
  }

}
