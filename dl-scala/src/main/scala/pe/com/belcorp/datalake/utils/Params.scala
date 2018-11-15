package pe.com.belcorp.datalake.utils

import Goodies.logIt
import org.rogach.scallop._
import play.api.libs.json.{JsObject, Json}

class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
  // Trigger processing for all tables
  val processAll = toggle(default = Some(false))

  // Select system and interface to process for raw ingestions
  val system = opt[String]()
  val interface = opt[String]()

  // Select analytic model to process, and mode to execute
  val model = opt[String]()
  val create = toggle(default = Some(false))
  val update = toggle(default = Some(false))
  val skipFailures = toggle(default = Some(false))

  // Redshift options
  val jdbc = opt[String]()
  val tempS3dir = opt[String]("tempS3Dir")
  val redshiftSchema = opt[String]()

  // Glue database
  val glueLandingDatabase = opt[String]()
  val glueStagingDatabase = opt[String]()
  val glueFunctionalDatabase = opt[String]()

  // Partitioning options
  val country = opt[String]()
  val year = opt[String]()
  val month = opt[String]()
  val day = opt[String]()
  val secs = opt[String]()

  // Export options
  val campaigns = opt[List[String]]()
  val reloadOnly = opt[List[String]](default = Some(List.empty))

  // Monitoring options
  val monitoringTopic = opt[String]()
  val executionId = opt[String]()
  val lambdaCallback = opt[String]()
  val stepId = opt[String]()
  val attempt = opt[Int]()

  // Other options
  val lockTable = opt[String]()

  // Run argument extractions
  verify()

  // Log ingested params
  logIt(s"Ingested params: $toString")

  def partitioningSpecification: PartitioningSpecification = {
    PartitioningSpecification(
      country.getOrElse(null),
      year.getOrElse(null),
      month.getOrElse(null),
      day.getOrElse(null),
      secs.getOrElse(null)
    )
  }

  def asTaskJson: JsObject = {
    Json.obj(
      "country" -> country(),
      "year" -> year(),
      "month" -> month(),
      "day" -> day(),
      "secs" -> secs(),
      "execution_id" -> executionId.getOrElse(""),
      "attempt" -> attempt(),
      "system" -> system()
    )
  }

  override def toString: String = s"Params(${arguments.mkString(" ")})"
}
