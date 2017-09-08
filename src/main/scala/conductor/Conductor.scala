
package conductor

import scala.collection.mutable.HashMap

case class PartitionsRequiredForJob(jobId: JobId,
                                    partitions: List[String],
                                    lastRefreshed: String)
case class JobId(jobIdNumber: Int)


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

  def getPriorityPartitions(): Seq[String] = {
    val allPartitions = state.partitionsByJobId.values.map(_.partitions).flatten
    val incidenceCounts: Map[String, Integer] = allPartitions.groupBy(identity).mapValues(_.size)
    val sortedCounts = incidenceCounts.toList.sortBy(_._2).filter(_._2 > 1)

    sortedCounts.map(_._1).toSeq

  }

  def getAllPartitions(): Seq[String] = {
    val allPartitions = state.partitionsByJobId.values.map(_.partitions).flatten
    allPartitions.toSeq
  }

}
