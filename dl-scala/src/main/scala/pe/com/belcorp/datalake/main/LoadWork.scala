package pe.com.belcorp.datalake.main

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jooq.conf.ParamType
import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.main.old.LoadModelToParquet.{generateQuery, reorderColumns}
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{DB, Params, SparkUtils}
import play.api.libs.json.Json
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn

object LoadWork {
  def main(args: Array[String]): Unit = try {
    val params = new Params(args)
    val spark = SparkUtils.getSparkSession()

    reportOn(params) {
      Task.init(params.system.getOrElse("all"), params)
      Task.ifMonitoring { t => t.success("WORK_LOADING") }

      if (params.processAll() && params.system.isDefined) {
        val system = System.fetch(params.system(), params)
        for (interface <- system.interfaces) {
          loadForStaging(interface, spark, params)
        }
        Task.ifMonitoring { t => t.success("WORK_LOADED") }
      }
    }

  } catch {
    case e: Exception =>
      Task.ifMonitoring { t =>
        t.failure("WORK_FAILED", e)
      }

      throw e
  }

  private def loadForStaging(interface: Interface, spark: SparkSession, params: Params): Unit = {
    logIt(s"Running ${interface.qualifiedSrcTableName}")

    val ellapsed = timeIt { interface.updateStagingParquet(spark, params) }

    logIt(s"${interface.qualifiedSrcTableName} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("WORK_INTERFACE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "system" -> interface.system.name,
        "interface" -> interface.name
      ))
    }
  }
}
