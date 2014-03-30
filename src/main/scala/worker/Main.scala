package worker

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.contrib.pattern.ClusterClient
import akka.contrib.pattern.ClusterSingletonManager

object Main extends Startup {

  def main(args: Array[String]): Unit = {
    val systemType = args(0)
    var joinAddress: akka.actor.Address = null
    if (args.length > 1) {
        var port:Int = args(2).toInt
        joinAddress = akka.actor.Address("akka.tcp","Workers",args(1),port)
    }
    if (systemType == "backend") {
        if (joinAddress != null) {
            startBackend(Some(joinAddress),"backend")
        } else {
            val addr = startBackend(None,"backend")
            println("Started on " + addr)
        }
    }
    if (systemType == "worker") {
        startWorker(joinAddress)
    }
    if (systemType == "frontEnd") {
        startFrontend(joinAddress)
    }
    /*
    val joinAddress = startBackend(None, "backend")
    Thread.sleep(5000)
    startBackend(Some(joinAddress), "backend")
    startWorker(joinAddress)
    Thread.sleep(5000)
    startFrontend(joinAddress)*/
  }

}

trait Startup {

  def systemName = "Workers"
  def workTimeout = 10.seconds

  def startBackend(joinAddressOption: Option[Address], role: String): Address = {
    val conf = ConfigFactory.parseString(s"akka.cluster.roles=[$role]").
      withFallback(ConfigFactory.load())
    val system = ActorSystem(systemName, conf)
    val joinAddress = joinAddressOption.getOrElse(Cluster(system).selfAddress)
    Cluster(system).join(joinAddress)
    system.actorOf(ClusterSingletonManager.props(Master.props(workTimeout), "active",
      PoisonPill, Some(role)), "master")
    println("backend running at " + Cluster(system).selfAddress)
    joinAddress
  }

  def startWorker(contactAddress: akka.actor.Address): Unit = {
    val system = ActorSystem("justtheworkers")
    val initialContacts = Set(
      system.actorSelection(RootActorPath(contactAddress) / "user" / "receptionist"))
    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clusterClient")
    for (i <- 1 to 10) {
        system.actorOf(Worker.props(clusterClient, Props[WorkExecutor]), "worker-" + i)
    }
  }

  def startFrontend(joinAddress: akka.actor.Address): Unit = {
    val system = ActorSystem(systemName)
    Cluster(system).join(joinAddress)
    val frontend = system.actorOf(Props[Frontend], "frontend")
    val tracker = system.actorOf(Props[ProducerTracker], "tracker")
    for (i <- 1 to 5000000) {
        system.actorOf(Props(classOf[WorkProducer], frontend, tracker), "producer-" + i)
        Thread.sleep(5)
    }
    //system.actorOf(Props[WorkResultConsumer], "consumer")
  }
}
