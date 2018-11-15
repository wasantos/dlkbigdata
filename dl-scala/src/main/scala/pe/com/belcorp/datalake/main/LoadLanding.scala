package pe.com.belcorp.datalake.main

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jooq.conf.ParamType
import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.main.LoadModelToParquet.{generateQuery, reorderColumns}
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Task
import pe.com.belcorp.datalake.utils.{DB, Params, SparkUtils}
import play.api.libs.json.Json
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn

object LoadLanding {
  def main(args: Array[String]): Unit = try {
    val params = new Params(args)
    val spark = SparkUtils.getSparkSession()

    reportOn(params) {
      Task.init(params.system.getOrElse("all"), params)
      Task.ifMonitoring { t => t.success("LOADING_LANDING") }

      if (params.processAll() && params.system.isDefined) {
        val system = System.fetch(params.system(), params)
        for (interface <- system.interfaces) {
          loadLandingFor(interface, spark, params)
        }
        Task.ifMonitoring { t => t.success("LANDING_LOADED") }
      }
    }

  } catch {
    case e: Exception =>
      Task.ifMonitoring { t =>
        t.failure("PARQUET_FAILED", e)
      }

      throw e
  }

  private def loadLandingFor(interface: Interface, spark: SparkSession, params: Params): Unit = {
    logIt(s"Running ${interface.qualifiedSrcTableName}")

    val ellapsed = timeIt { interface.updateLandingParquet(spark, params) }

    logIt(s"${interface.qualifiedSrcTableName} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("LANDING_INTERFACE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "system" -> interface.system.name,
        "interface" -> interface.name
      ))
    }
  }

  private def generateQuery(db: DB, table: Table, campaigns: Seq[String], country: String): String = {

    db.checkout { conn =>
      import org.jooq.impl.DSL.{field, name}

      val dsl = DB.dsl(conn)
      val baseQuery = dsl.select()
        .from(name(table.schema, table.name)).where("1=1")

      table.partitions.foldLeft(baseQuery) { (query, column) =>
        if(column == "CODPAIS") {
          query.and(s"${field(column)} = $$$$$country$$$$")
        } else if(column.startsWith("ANIOCAMPANA")) {
          if(campaigns.nonEmpty) {
            val inClause = campaigns.map(c => s"$$$$$c$$$$").mkString(", ")
            query.and(s"${field(column)} IN ($inClause)")
          } else {
            query
          }
        } else {
          throw new IllegalArgumentException(s"Invalid column ${column}")
        }
      }.getSQL(ParamType.INLINED)
    }
  }

}
