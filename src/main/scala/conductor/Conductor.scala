
package conductor

import scala.collection.mutable.HashMap
import java.time.{LocalDateTime, Duration}

case class PartitionsRequiredForJob(jobId: JobId,
                                    partitions: List[String],
                                    lastRefreshed: LocalDateTime)
case class JobId(jobIdNumber: String)


class SystemState {
  val partitionsByJobId: HashMap[JobId, PartitionsRequiredForJob] = HashMap()
}

class Conductor {
  val state = new SystemState

  def refreshPartitionsForJob(job: JobId, partitions: List[String], datetime: LocalDateTime): Unit = {

    state.partitionsByJobId.update(job, PartitionsRequiredForJob(job,
                                                                 partitions,
                                                                 datetime))
  }

  def getPriorityPartitions(now: LocalDateTime): Seq[String] = {

    val maxPartitionsToReturn = 10
    val jobExpirationTime = Duration.ofMinutes(5)

    def partName[A,B](x: Tuple2[A,B]): A = x._1
    def counts[A,B](x: Tuple2[A,B]): B = x._2

    state.partitionsByJobId.retain(
            (id: JobId, partitions: PartitionsRequiredForJob) =>
            Duration.between(partitions.lastRefreshed, now).compareTo(jobExpirationTime) <= 0)

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
