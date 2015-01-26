package org.averous.client

import akka.actor.{ Actor, ActorRef, PoisonPill, ActorLogging, ActorSystem, Props, ReceiveTimeout }
import org.akkaresque.perform
import org.akkaresque.ResQ
import org.akkaresque.Worker
import org.akkaresque.work
import com.redis._
import org.json4s._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization
import org.json4s.DefaultFormats
import org.json4s.native.Json
import scalaj.http.Http
import scala.collection.mutable.Map

object Demo {
  
  def main(args: Array[String]): Unit = {
    try {
      
      println("OK")
      Averous.config = Map("averous_ip" -> "http://localhost", "averous_port" -> 8090, "redis_ip" -> "localhost", "redis_port" -> "6379", "queue_name" -> "event_queue")
      val averous = Averous()
      averous.log("event_type", Map("post_id" ->"2322", "cat"->"meow", "dog"->"raf"));

      //Averous.startWorker();
      //Averous.startOverFailur();
      
    } catch {
      case ex: Exception =>
        print(ex.printStackTrace.toString)
    }
  }
}

object Averous {
  
  var config = Map("averous_ip" -> "http://localhost", "averous_port" -> 8090, "redis_ip" -> "localhost", "redis_port" -> "6379", "queue_name" -> "event_queue")

  def apply(): Averous = {
    //read from config file the parameters
    val ip   = config.get("redis_ip").get.toString
    val port = config.get("redis_port").get.toString
    val averous_ip   = config.get("averous_ip").get.toString
    val averous_port = config.get("averous_port").get.toString

    val pool  = new RedisClientPool(ip,  port.toInt)
    val queue = config.get("queue_name").get.toString;

    val averousSystem = ActorSystem("AverousApplication")
    val ref = averousSystem.actorOf(Props(new AverousActor(averous_ip, averous_port.toInt)), "averous")
    val path_to_worker = ref.path.toString
    new Averous(pool, queue, path_to_worker)
  }

  def startOverFailur() = {
    //read from config file the parameters
    val ip   = config.get("redis_ip").get.toString
    val port = config.get("redis_port").get.toString
    
    val averous_ip   = config.get("averous_ip").get.toString
    val averous_port = config.get("averous_port").get.toString

    val pool  = new RedisClientPool(ip, port.toInt)
    val queue = "failed_events"

    val system = ActorSystem("AverousApplication")
    system.actorOf(Props(new AverousActor(averous_ip, averous_port.toInt)), "averous")
    val worker = system.actorOf(Props(new Worker(List(queue), pool,0,5)))
    worker ! work("bang!")
  }

  def startWorker() = {
    //read from config file the parameters
    val ip   = config.get("redis_ip").get.toString
    val port = config.get("redis_port").get.toString

    val averous_ip   = config.get("averous_ip").get.toString
    val averous_port = config.get("averous_port").get.toString

    val pool  = new RedisClientPool(ip, port.toInt)
    val queue = config.get("queue_name").get.toString;

    val system = ActorSystem("AverousApplication")
    system.actorOf(Props(new AverousActor(averous_ip, averous_port.toInt)), "averous");
    val worker = system.actorOf(Props(new Worker(List(queue), pool,0,5)))
    worker ! work("bang!")
  }

}

class Averous(pool : RedisClientPool, queue: String, worker: String) {
  val resque      = ResQ(pool)
  val queue_name  = queue
  val worker_name = worker 
  val time_format = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"

  def log(event_type : String, params : Map[String, String]) = {
    val format = new java.text.SimpleDateFormat(time_format)
    val now_formated = format.format(java.util.Calendar.getInstance().getTime())
    params += ("event_time" -> now_formated)
    params += ("event_type" -> event_type)
    val josn_params = Json(DefaultFormats).write(params)
    resque.enqueue(worker_name, queue_name, List(josn_params))
  }

  def logFailedEvent(event : String) = {
    resque.enqueue(worker_name, "failed_events", List(event))
  }

}

class AverousActor(averous_ip: String, averous_port: Integer)
  extends Actor with ActorLogging {

  def receive = {
    case perform(args) =>
      try {
        log.info("Got a Job" + args)
        val url  = averous_ip + ":" + averous_port
        var json_args = args(0)
        json_args = json_args.replace("\\", "");
        val result = Http.postData(url, json_args).header("Content-Type", "application/json").header("Charset", "UTF-8").responseCode
        if(result == 200) {
          sender ! "Done"
        } else {
          throw new Exception("Error Delivering event to Averous Server")
        }
      } catch {
        case e: Exception =>
          //re push the event into queue
          repushEvent(args(0))
          sender ! akka.actor.Status.Failure(e)
      }
  }

  def repushEvent(event: String) = {
    val averous = Averous()
    averous.logFailedEvent(event);
  }

}