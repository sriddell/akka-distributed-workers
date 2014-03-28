package worker

import scala.collection.immutable.Queue
import akka.actor.Actor
import akka.actor.Terminated
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import akka.contrib.pattern.DistributedPubSubMediator.Put
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import akka.actor.Props

object Master {

  val ResultsTopic = "results"

  def props(workTimeout: FiniteDuration): Props =
    Props(classOf[Master], workTimeout)

  case class Ack(workId: String)

  private sealed trait WorkerStatus
  private case object Idle extends WorkerStatus
  private case class Busy(work: Work, deadline: Deadline) extends WorkerStatus
  private case class WorkerState(ref: ActorRef, status: WorkerStatus, respondTo: ActorRef)

  private case object CleanupTick
  private case object StatsTick

}

class Master(workTimeout: FiniteDuration) extends Actor with ActorLogging {
  import Master._
  import MasterWorkerProtocol._
  import scala.concurrent.duration._
  val mediator = DistributedPubSubExtension(context.system).mediator

  mediator ! Put(self)

  private var workers = Map[String, WorkerState]()
  //pending work along with who asked for it to be done
  private var pendingWork = Queue[Tuple2[ActorRef,Work]]()
  private var workIds = Set[String]()

  import context.dispatcher
  val cleanupTask = context.system.scheduler.schedule(workTimeout / 2, workTimeout / 2,
    self, CleanupTick)
  val statsTask = context.system.scheduler.schedule(0.seconds, 5.seconds, self, StatsTick)

  override def postStop(): Unit = {
    cleanupTask.cancel()
    statsTask.cancel()
  }

  def receive = {
    case RegisterWorker(workerId) =>
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(ref = sender))
      } else {
        log.debug("Worker registered: {}", workerId)
        println("Registering worker" + sender())
        workers += (workerId -> WorkerState(sender, status = Idle, respondTo = null))
        context.watch(sender)
        if (pendingWork.nonEmpty)
          sender ! WorkIsReady
      }

    case Terminated(worker) => {
      //this is really ineffecient
      //need to look at changing the keys of workers to being ActorRef of the
      //worker directly.
      var oldKey: String = null
      workers.foreach {
        case (key,value) =>
          if (value.ref == worker) {
            oldKey = key
          }
      }

      if (oldKey != null) {
        workers -= oldKey
      }
    }

    case WorkerRequestsWork(workerId) =>
      if (pendingWork.nonEmpty) {
        workers.get(workerId) match {
          case Some(s @ WorkerState(_, Idle, _)) =>
            val ((respondTo, work), rest) = pendingWork.dequeue
            pendingWork = rest
            log.debug("Giving worker {} some work {}", workerId, work.job)
            // TODO store in Eventsourced
            sender ! work
            workers += (workerId -> s.copy(status = Busy(work, Deadline.now + workTimeout), respondTo = respondTo))
          case _ =>

        }
      }

    case WorkIsDone(workerId, workId, result) =>
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work, _), _)) if work.workId == workId =>
          log.debug("Work is done: {} => {} by worker {}", work, result, workerId)
          // TODO store in Eventsourced
          val respondTo = s.respondTo
          workers += (workerId -> s.copy(status = Idle, respondTo = null))
          //mediator ! DistributedPubSubMediator.Publish(ResultsTopic, WorkResult(workId, result))
          respondTo ! WorkResult(workId, result)
          sender ! MasterWorkerProtocol.Ack(workId)
        case _ =>
          if (workIds.contains(workId)) {
            // previous Ack was lost, confirm again that this is done
            sender ! MasterWorkerProtocol.Ack(workId)
          }
      }

    case WorkFailed(workerId, workId) =>
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work, _), _)) if work.workId == workId =>
          log.info("Work failed: {}", work)
          // TODO store in Eventsourced
          val respondTo = s.respondTo
          workers += (workerId -> s.copy(status = Idle, respondTo = null))
          pendingWork = pendingWork enqueue(respondTo->work)
          notifyWorkers()
        case _ =>
      }

    case WorkRequest(work, respondTo) =>
      // idempotent
      if (workIds.contains(work.workId)) {
        sender ! Master.Ack(work.workId)
      } else {
        log.debug("Accepted work: {}", work)
        // TODO store in Eventsourced
        pendingWork = pendingWork enqueue(respondTo -> work)
        workIds += work.workId
        sender ! Master.Ack(work.workId)
        notifyWorkers()
      }

    case CleanupTick =>
      for ((workerId, s @ WorkerState(_, Busy(work, timeout),_)) <- workers) {
        if (timeout.isOverdue) {
          log.info("Work timed out: {}", work)
          // TODO store in Eventsourced
          workers -= workerId
          val respondTo = s.respondTo
          pendingWork = pendingWork enqueue(respondTo -> work)
          notifyWorkers()
        }
      }
    case StatsTick => {
        println("Registered workers:" + workers.keySet.size)
        println("Pending work size:" + pendingWork.length)
    }
  }

  def notifyWorkers(): Unit =
    if (pendingWork.nonEmpty) {
      // could pick a few random instead of all
      workers.foreach {
        case (_, WorkerState(ref, Idle, _)) => ref ! WorkIsReady
        case _                           => // busy
      }
    }

  // TODO cleanup old workers
  // TODO cleanup old workIds

}