package worker

import java.util.UUID
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef

object WorkProducer {
  case object Tick
}

class WorkProducer(frontend: ActorRef, tracker: ActorRef) extends Actor with ActorLogging {
  import WorkProducer._
  import context.dispatcher
  import context._
  def scheduler = context.system.scheduler
  def rnd = ThreadLocalRandom.current
  def nextWorkId(): String = UUID.randomUUID().toString

  var n = 0

  def receive = producing

  tracker ! ProducerTracker.ILive
  self ! Tick

  override def postStop(): Unit = {
    tracker ! ProducerTracker.IDie
    super.postStop()
  }

  def producing: Receive = {
    case Tick =>
      n += 1
      log.info("Produced work: {}", n)
      val work = Work(nextWorkId(), n)
      frontend ! work
      //context.become(waitAccepted(work), discardOld = false)
      context.become(waitAccepted(work))
  }

  def waitAccepted(work: Work): Receive = {
    case Frontend.Ok =>
        context.become(waitResult)
    case Frontend.NotOk =>
      log.info("Work not accepted, retry after a while")
      scheduler.scheduleOnce(3.seconds, frontend, work)
  }

  def waitResult: Receive = {
    case WorkResult(workId, result) => {
      log.info("Received result: {}", result)
      stop(self)
      //context.become(producing)
      //scheduler.scheduleOnce(rnd.nextInt(3, 10).seconds, self, Tick)
    }
  }

}