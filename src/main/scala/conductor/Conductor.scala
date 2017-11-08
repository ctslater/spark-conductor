
package conductor

import scala.collection.mutable.HashMap

case class PartitionsRequiredForJob(jobId: JobId,
                                    partitions: List[String],
                                    lastRefreshed: String)
case class JobId(jobIdNumber: String)


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

    val maxPartitionsToReturn = 10

    def partName[A,B](x: Tuple2[A,B]): A = x._1
    def counts[A,B](x: Tuple2[A,B]): B = x._2

    val allPartitions = state.partitionsByJobId.values.map(_.partitions).flatten
    val incidenceCounts: Map[String, Integer] = allPartitions.groupBy(identity).mapValues(_.size)
    val sortedCounts = incidenceCounts.toList.sortBy(counts).filter(counts(_) > 1)

    sortedCounts.map(partName).toSeq.slice(0, maxPartitionsToReturn)

  }

  def getAllPartitions(): Seq[String] = {
    val allPartitions = state.partitionsByJobId.values.map(_.partitions).flatten
    allPartitions.toSeq
  }

}
