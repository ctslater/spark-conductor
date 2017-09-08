
package conductor

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io.StdIn

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._


case class PriorityPartitionsResponse(files: Seq[String])
case class JobUpdate(job_id: Int, partitions: List[String])

trait ConductorService {

  implicit val system:ActorSystem
  implicit val materializer:ActorMaterializer
  // needed for the future flatMap/onComplete in the end
  // Gave some problems when marked as implicit.
  val executionContext = system.dispatcher

  implicit val priorityPartitionsRespFormat = jsonFormat1(PriorityPartitionsResponse)
  implicit val jobUpdateFormat = jsonFormat2(JobUpdate)

  val conductor = new Conductor

  val route =
    path("partitions") {
      post {
        decodeRequest {
          entity(as[JobUpdate]) {  update => complete {
            conductor.refreshPartitionsForJob(JobId(update.job_id), update.partitions)
            "Success"
            }
          }
        }
      }
    } ~
    path("priority") {
      get {
        complete(PriorityPartitionsResponse(conductor.getPriorityPartitions))
      }
    }
}

class ConductorServer(implicit val system:ActorSystem,
  implicit val materializer:ActorMaterializer) extends ConductorService

object ConductorServer {
  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val server = new ConductorServer
    val bindingFuture = Http().bindAndHandle(server.route, "localhost", 8080)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
