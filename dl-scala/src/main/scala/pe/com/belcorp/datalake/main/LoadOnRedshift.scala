package pe.com.belcorp.datalake.main

import org.apache.spark.sql.SparkSession
import pe.com.belcorp.datalake.raw.datasets.{InterfaceFunctional, SystemFunctional}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{DB, Lock, Params, SparkUtils}
import play.api.libs.json.Json

object LoadOnRedshift {
  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val spark = SparkUtils.getSparkSession()
    val manager = new DB(params.jdbc())

    Lock.init(params.lockTable())

    try {
      reportOn(params) {
        Task.init(params.system.getOrElse("all"), params)
        Task.ifMonitoring { t => t.success("LOADING_TO_REDSHIFT") }

        if (params.processAll() && params.system.isDefined) {
          val system = SystemFunctional.fetch(params.system(), params)
          manageSchema(system.redshiftSchema, manager)

          for (interface <- system.interfaces) {
            loadFor(interface, manager, spark, params)
          }

          Task.ifMonitoring { t => t.success("REDSHIFT_LOADED") }
        }
      }
    } catch {
      case e: Exception =>
        Task.ifMonitoring { t =>
          t.failure("REDSHIFT_FAILED", e)
        }

        throw e
    } finally {
      spark.stop()
      manager.close()
    }
  }

  private def loadFor(interface: InterfaceFunctional, db: DB, spark: SparkSession, params: Params): Unit = {
    logIt(s"Running ${interface.qualifiedRedshiftTableName}")
    val ellapsed = timeIt {
      interface.updateFunctionalRedshift(spark, db, params)
    }
    logIt(s"${interface.qualifiedRedshiftTableName} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("REDSHIFT_INTERFACE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "system" -> interface.system.name,
        "interface" -> interface.name,
        "table" -> interface.qualifiedRedshiftTableName
      ))
    }
  }

  private def manageSchema(schema: String, db: DB): Unit = {
    db.checkout { conn =>
      DB.dsl(conn).createSchemaIfNotExists(schema).execute()
    }
  }
}
