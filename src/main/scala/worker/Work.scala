package worker
import akka.actor.ActorRef

case class Work(workId: String, job: Any)

case class WorkRequest(work: Work, respondTo: ActorRef)

case class WorkResult(workId: String, result: Any)