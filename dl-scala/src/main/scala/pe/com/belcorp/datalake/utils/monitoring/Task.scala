package pe.com.belcorp.datalake.utils.monitoring

import java.io.{PrintWriter, StringWriter}
import java.time.Instant

import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import pe.com.belcorp.datalake.utils.{Goodies, Params, PartitioningSpecification}
import play.api.libs.json.{JsObject, JsValue, Json, Writes}

/**
  * Class representing a job/task/step being processed and monitored
  *
  * @param system system being processed (SICC, PLANIT, etc)
  * @param params the process parameters
  */
class Task(system: String, params: Params) {
  import Task._

  /**
    * Unique ID assigned to each task, composed of
    * - system being processed
    * - partitions being processed
    * - a unique timestamp
    */
  val id: String = s"${system}:${getFlowId()}:${getExecutionId()}"

  /**
    * Generate a success notification.
    * Will only be called if a valid SNS topic was supplied as parameter
    *
    * @param status status identifying the current state of the job
    * @param payload extra data to be added to the notification
    */
  def success(status: String, payload: EventPayload = null): Unit =
    event(status, system, ok = true, payload)

  /**
    * Generate a failure notification
    * Will only be called if a valid SNS topic was supplied as parameter
    *
    * @param status status identifying the current state of the job
    * @param payload extra data to be added to the notification
    */
  def failure(status: String, payload: EventPayload = null): Unit =
    event(status, system, ok = false, payload)

  /**
    * Generate a failure notification
    * Will only be called if a valid SNS topic was supplied as parameter
    *
    * @param status status identifying the current state of the job
    * @param exception exception to be reported
    */
  def failure(status: String, exception: Exception): Unit =
    event(status, system, ok = false, Json.obj(
      "message" -> exception.getMessage,
      "stacktrace" -> formatStackTrace(exception)
    ))

  /**
    * Executes the supplied function only if a valid SNS topic was supplied
    * as parameter. Avoids expensive computations if no notification is needed.
    *
    * @param f function to be executed, receiving the task itself
    */
  def ifMonitoring[_T](f: Task => _T): Unit = {
    optSns.foreach { _ => f(this) }
  }

  private def formatStackTrace(exception: Exception): String = {
    val sw = new StringWriter()
    exception.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  private def event(
    status: String, system: String, ok: Boolean, payload: EventPayload
  ): Unit = optSns.foreach { sns =>
      val message = Event(
        id, status, system, params.partitioningSpecification, ok, payload
      ).toJson

      sns.publish(topic, message)
    }

  private def getFlowId(): String = params.partitioningSpecification.toKeyString
  private def getExecutionId(): String = {
    params.executionId.getOrElse(Goodies.now.toEpochMilli.toString)
  }

  private val topic: String = params.monitoringTopic.getOrElse(null)
  private val optSns: Option[AmazonSNS] =
    if(topic != null) Some(AmazonSNSClientBuilder.defaultClient())
    else None
}

/**
  * Global object for handling tasks.
  * The methods in this object assume that you won't be handling multiple tasks
  * simultaneously in the same program invocation - in other words, they aren't
  * thread-safe. You also should not use this object inside a function executed
  * by a Spark executor - use it only in the Spark driver.
  */
object Task {
  type EventPayload = JsObject

  private var currentTask: Option[Task] = None

  /**
    * Starts tracking a new job. Discards any previous tasks.
    * @param system system being processed (SICC, PLANIT, etc)
    * @param params the process parameters
    */
  def init(system: String, params: Params): Unit = synchronized {
    currentTask = Some(new Task(system, params))
  }

  /**
    * Gets currrent task being monitored.
    *
    * @return the current task
    * @throws IllegalStateException if [[init]] was never called
    */
  def current: Task = currentTask.getOrElse(
    throw new IllegalStateException("Current task not initialized")
  )

  /**
    * Executes the supplied function only if task was initialized, and with a
    * valid SNS topic as parameter.
    * Avoids expensive computations if no notification is needed.
    *
    * @param f function to be executed, receiving the task itself
    */
  def ifMonitoring[_T](f: Task => _T): Unit = {
    currentTask.foreach(_.ifMonitoring(f))
  }

  /**
    * Class representing a notification
    *
    * @param id unique ID representing the task
    * @param status status identifying the current state of the task
    * @param system system being processed (SICC, PLANIT, etc)
    * @param partitions partition specification for the task being processed
    * @param ok true if notification is for a OK state, false otherwise
    * @param payload extra data to be added to the notification
    */
  case class Event(
    id: String, status: String, system: String,
    partitions: PartitioningSpecification,
    ok: Boolean, payload: EventPayload
  ) {
    val timestamp: Instant = Goodies.now

    // Implements serializer to JSON for class (uses Play-JSON)
    implicit private object eventWriter extends Writes[Event] {
      implicit private val partitionWriter = Json.writes[PartitioningSpecification]

      override def writes(e: Event): JsValue = {
        Json.obj(
          "id" -> e.id,
          "status" -> e.status,
          "system" -> e.system,
          "ok" -> e.ok,
          "timestamp" -> e.timestamp,
          "partitions" -> Json.toJson(e.partitions),
          "payload" -> Option(e.payload)
        )
      }
    }

    /**
      * Converts event to a JSON string
      */
    def toJson: String = Json.toJson(this).toString()
  }
}
