package pe.com.belcorp.datalake.main.old

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jooq.conf.ParamType
import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils._
import pe.com.belcorp.datalake.utils.monitoring.Callback.reportOn
import pe.com.belcorp.datalake.utils.monitoring.Task
import play.api.libs.json.Json

object LoadModelToParquet {
  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val credentials = AWSCredentials.fetch()
    val jdbc = params.jdbc()
    val tempS3dir = params.tempS3dir()
    val manager = new DB(jdbc)
    val model = LoadModel.modelsAvailable(params.model())(manager, params)
    val country = params.country()
    val campaigns = params.campaigns.getOrElse {
      manager.transaction(conn => CampaignTracking.fetch(conn, params).toList)
    }

    val spark = SparkUtils.getSparkSession()
    spark.catalog.setCurrentDatabase(params.glueFunctionalDatabase())
    Lock.init(params.lockTable())

    try {
      reportOn(params) {
        Task.init(params.system.getOrElse("all"), params)
        Task.ifMonitoring { t => t.success("LOADING_FUNCTIONAL_TO_PARQUET") }

        for (table <- model.tables) {
          val parquet = s"${table.name}"
          logIt(s"Processing ${parquet}")

          val ellapsed = timeIt {
            val query = generateQuery(manager, table, campaigns, country)
            logIt(s"Redshift query is ${query}")
            val inputDf = getRedshiftDataFrame(spark, jdbc, query, tempS3dir, credentials)

            Lock.acquire(s"loadFunctionalParquet:${table.name}") { _ =>
              if (table.partitions.isEmpty) {
                // Save as is, ignore partitions
                inputDf.write.mode(SaveMode.Overwrite).saveAsTable(parquet)
              } else {
                if (spark.catalog.tableExists(parquet)) {
                  // If table exists, use insertInto to not delete it
                  // Also reorder columns because Spark ducking sucks
                  val columns = inputDf.columns
                  val reordered = reorderColumns(columns, table.partitions)

                  inputDf.selectExpr(reordered: _*).write
                    .mode(SaveMode.Overwrite)
                    .insertInto(parquet)
                } else {
                  // Otherwise, use saveAsTable to create it,
                  // specifying partitions
                  inputDf.write
                    .mode(SaveMode.Overwrite)
                    .partitionBy(table.partitions: _*)
                    .saveAsTable(parquet)
                }
              }
            }
          }

          Task.ifMonitoring { t =>
            t.success("FUNCTIONAL_TO_PARQUET_TABLE_LOADED", Json.obj(
              "ellapsed_ms" -> ellapsed,
              "table" -> table.name
            ))
          }
        }

        Task.ifMonitoring { t => t.success("FUNCTIONAL_TO_PARQUET_LOADED") }
      }
    } catch {
      case e: Exception =>
        Task.ifMonitoring { t =>
          t.failure("FUNCTIONAL_TO_PARQUET_FAILED", e)
        }

        throw e
    } finally {
      spark.stop()
      manager.close()
    }
  }

  private def reorderColumns(columns: Seq[String], partitions: Seq[String]): Seq[String] = {
    val lowercase = partitions.map(_.toLowerCase)
    val filtered = columns.filterNot(lowercase.contains)

    filtered ++ lowercase
  }

  private def generateQuery(
     db: DB, table: Table,
     campaigns: Seq[String], country: String): String = {

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

  private def getRedshiftDataFrame(spark: SparkSession, jdbc: String, query: String,
                                   tempS3dir: String, credentials: AWSCredentials): DataFrame = {
    val inputDf = spark.read
      .format("com.databricks.spark.redshift")
      .option("url", jdbc)
      .option("query", query)
      .option("tempdir", tempS3dir)
      .option("temporary_aws_access_key_id", credentials.accessKey)
      .option("temporary_aws_secret_access_key", credentials.secretKey)
      .option("temporary_aws_session_token", credentials.token)
      .option("tempformat", "CSV GZIP")
      .load() // Load DataFrame schema

    val nullableSchema = setNullableStateForAllStringColumns(inputDf, true)

    spark.read.schema(nullableSchema)
      .format("com.databricks.spark.redshift")
      .option("url", jdbc)
      .option("query", query)
      .option("tempdir", tempS3dir)
      .option("temporary_aws_access_key_id", credentials.accessKey)
      .option("temporary_aws_secret_access_key", credentials.secretKey)
      .option("temporary_aws_session_token", credentials.token)
      .option("tempformat", "CSV GZIP")
      .load() // Table is loaded
  }

  private def setNullableStateForAllStringColumns(df: DataFrame, nullable: Boolean): StructType = {
    StructType(df.schema.map {
      case StructField( c, StringType, _, m) => StructField( c, StringType, nullable = nullable, m)
      case StructField( c, t, n, m) => StructField( c, t, n, m)
    })
  }
}
