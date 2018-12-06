package pe.com.belcorp.datalake.main

import org.apache.spark.sql.SparkSession
import pe.com.belcorp.datalake.raw.datasets.{Interface, InterfaceFunctional, System, SystemFunctional}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{Params, SparkUtils}
import play.api.libs.json.Json

object LoadFunctional {
  def main(args: Array[String]): Unit = try {
    val params = new Params(args)
    val spark = SparkUtils.getSparkSession()

    reportOn(params) {
      Task.init(params.system.getOrElse("all"), params)
      Task.ifMonitoring { t => t.success("FUNCTIONAL_LOADING") }

      if (params.processAll() && params.system.isDefined) {
        val system = SystemFunctional.fetch(params.system(), params)
        for (interface <- system.interfaces) {
          loadForFunctional(interface, spark, params)
        }
        Task.ifMonitoring { t => t.success("FUNCTIONAL_LOADED") }
      }
    }

  } catch {
    case e: Exception =>
      Task.ifMonitoring { t =>
        t.failure("FUNCIONAL_FAILED", e)
      }

      throw e
  }

  private def loadForFunctional(interface: InterfaceFunctional, spark: SparkSession, params: Params): Unit = {
    logIt(s"Running ${interface.qualifiedSrcTableNames}")

    val ellapsed = timeIt { interface.updateFunctionalParquet(spark, params) }

    logIt(s"${interface.qualifiedSrcTableNames} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("FUNCTIONAL_INTERFACE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "system" -> interface.system.name,
        "interface" -> interface.name,
        "target" -> interface.targetTable
      ))
    }
  }
}
