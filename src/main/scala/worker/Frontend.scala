package worker

import scala.concurrent.duration._
import akka.actor.Actor
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Send
import akka.pattern._
import akka.util.Timeout

object Frontend {
  case object Ok
  case object NotOk
}

class Frontend extends Actor {
  import Frontend._
  import context.dispatcher
  val mediator = DistributedPubSubExtension(context.system).mediator

  def receive = {
    case work: Work =>
      implicit val timeout = Timeout(5.seconds)
      val workRequest = WorkRequest(work, sender)
      (mediator ? Send("/user/master/active", workRequest, localAffinity = false)) map {
        case Master.Ack(_) => Ok
      } recover { case _ => NotOk } pipeTo sender

  }

}