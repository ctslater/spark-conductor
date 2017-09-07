
package conductor

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io.StdIn

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._


trait ConductorService {

  implicit val system:ActorSystem
  implicit val materializer:ActorMaterializer
  // needed for the future flatMap/onComplete in the end
  val executionContext = system.dispatcher

  implicit val priorityTasksFormat = jsonFormat1(PriorityPartitions)

  val conductor = new Conductor
  conductor.refreshPartitionsForJob(JobId(0),
                                    List("partition1", "partition2"))
  conductor.refreshPartitionsForJob(JobId(1),
                                    List("partition1", "partition3"))

  val route =
    path("hello") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
      }
    } ~
    path("getPartitions") {
      get {
        complete(conductor.getPriorityPartitions)
      }
    } /* ~
    path("updatePartitions") {
      put {
        decodeRequest {
          entity(as[JobUpdate]) {  update => conductor.refreshPartitionsForJob(update) }
        }
      }
    } */

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
