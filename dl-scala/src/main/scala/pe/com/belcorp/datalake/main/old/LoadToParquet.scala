package pe.com.belcorp.datalake.main.old

import org.apache.spark.sql.SparkSession
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{Params, SparkUtils}
import play.api.libs.json.Json

object LoadToParquet {
  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val spark = SparkUtils.getSparkSession()

    try {
      reportOn(params) {
        Task.init(params.system.getOrElse("all"), params)
        Task.ifMonitoring { t => t.success("LOADING_TO_PARQUET") }

        if (params.processAll() && params.system.isDefined) {
          val system = System.fetch(params.system(), params)
          for (interface <- system.interfaces) {
            loadFor(interface, spark, params)
          }
          Task.ifMonitoring { t => t.success("PARQUET_LOADED") }
        } else if (params.processAll()) {
          for (system <- System.fetchAll(params)) {
            for (interface <- system.interfaces) {
              loadFor(interface, spark, params)
            }
          }
          Task.ifMonitoring { t => t.success("PARQUET_LOADED") }
        }
      }
    } catch {
      case e: Exception =>
        Task.ifMonitoring { t =>
          t.failure("PARQUET_FAILED", e)
        }

        throw e
    } finally {
      spark.stop()
    }
  }

  private def loadFor(interface: Interface, spark: SparkSession, params: Params): Unit = {
    logIt(s"Running ${interface.qualifiedSrcTableName}")

    val ellapsed = timeIt { interface.writeToParquet(spark, params) }

    logIt(s"${interface.qualifiedSrcTableName} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("PARQUET_INTERFACE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "system" -> interface.system.name,
        "interface" -> interface.name
      ))
    }
  }
}
