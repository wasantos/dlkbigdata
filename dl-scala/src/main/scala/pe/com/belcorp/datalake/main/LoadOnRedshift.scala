package pe.com.belcorp.datalake.main

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{DB, Lock, Params, SparkUtils}
import play.api.libs.json.Json
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn

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
          val system = System.fetch(params.system(), params)
          manageSchema(system.redshiftSchema, manager)
          val campaignDfs = for (interface <- system.interfaces) yield {
            loadFor(interface, spark, params)
          }

          saveTracking(campaignDfs.flatten, manager, params)
          Task.ifMonitoring { t => t.success("REDSHIFT_LOADED") }
        } else if (params.processAll()) {
          val campaignDfs = for {
            system <- System.fetchAll(params)
            interface <- system.interfaces
          } yield {
            loadFor(interface, spark, params)
          }

          saveTracking(campaignDfs.flatten, manager, params)
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

  private def loadFor(interface: Interface, spark: SparkSession, params: Params): Option[DataFrame] = {
    logIt(s"Running ${interface.qualifiedSrcTableName}")
    val ellapsed = timeIt {
      interface.writeToRedshift(spark, params)
    }
    logIt(s"${interface.qualifiedSrcTableName} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("REDSHIFT_INTERFACE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "system" -> interface.system.name,
        "interface" -> interface.name
      ))
    }

    interface.getCampaigns(spark, params)
  }

  private def saveTracking(campaignDfs: Seq[DataFrame], db: DB, params: Params): Unit = {
    if (campaignDfs.isEmpty) return

    val ellapsed = timeIt {
      // Aglutinate campaign data from all tables
      val union = campaignDfs.reduce((a, b) => a.union(b)).distinct()

      // Create campaign table
      Lock.acquire("emrCampaignTrackingTable") { _ =>
        db.checkout { conn => CampaignTracking.create(conn, params) }
      }

      // Save new campaign data into tracking table
      CampaignTracking.append(union, params)
    }

    logIt(s"Saving campaigns took ${ellapsed.toLong} ms")
  }

  private def manageSchema(schema: String, db: DB): Unit = {
    db.checkout { conn =>
      DB.dsl(conn).createSchemaIfNotExists(schema).execute()
    }
  }
}
