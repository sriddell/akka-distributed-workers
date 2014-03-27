package worker

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.Terminated
import akka.contrib.pattern.ClusterClient.SendToAll
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Stop
import akka.actor.SupervisorStrategy.Restart
import akka.actor.ActorInitializationException
import akka.actor.DeathPactException


object ProducerTracker {
  case object Tick
  case object ILive
  case object IDie
}

class ProducerTracker extends Actor with ActorLogging {
  import ProducerTracker._

  var registeredCount : Int = 0
  var totalCount : Int = 0

  import context.dispatcher
  val cleanupTask = context.system.scheduler.schedule(0.seconds, 10.seconds, self, Tick)

  def receive = {
    case ILive => {
        registeredCount = registeredCount + 1
        totalCount = totalCount + 1
    }
    case IDie => {
        registeredCount = registeredCount - 1
    }
    case Tick => {
        println(">>>>>>>>>>>>>>>>>>>>>>>>Have " + registeredCount + " requests active")
        println(">>>>>>>>>>>>>>>>>>>>>>>>Have seen " + totalCount + " requests")
    }
  }

}