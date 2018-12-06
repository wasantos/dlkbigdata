package pe.com.belcorp.datalake.common

import java.sql.Connection

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.jooq.impl.DSL._
import pe.com.belcorp.datalake.utils.Goodies.logIt
import pe.com.belcorp.datalake.utils.{AWSCredentials, DB, Params, PartitioningSpecification}

import scala.collection.JavaConverters._

object CampaignTracking {
  val TRACKING_TABLE: String = "int_campaign_tracking"
  val TRACKING_COLUMN: String = "aniocampana"

  def create(conn: Connection, params: Params): Unit = {
    val columns = PartitioningSpecification.COLUMNS.map { c =>
      s"$c VARCHAR(10)"
    }.mkString(",\n")

    conn.createStatement().execute(
      s"""
         |CREATE TABLE IF NOT EXISTS public.${TRACKING_TABLE} (
         |$columns,
         |${TRACKING_COLUMN} VARCHAR(6),
         |system_name VARCHAR(10),
         |interface_name VARCHAR(30))
       """.stripMargin)
  }

  def select(df: DataFrame, params: Params, campaignColumn: Option[String],
             system: String, name: String): (Option[DataFrame], Boolean) = {
    import org.apache.spark.sql.functions._

    if (campaignColumn.isEmpty) {
      return (None, false)
    }

    // campaignColumn is guaranteed to be not empty here
    val wrongColumn = !df.columns.contains(campaignColumn.get.toLowerCase)

    if (wrongColumn) {
      return (None, true)
    }

    val partitionCols = params.partitioningSpecification.columns

    // Return filled dataframe, properly deduplicated
    val columns = partitionCols.map(col) ++ Seq(col(campaignColumn.get).as(TRACKING_COLUMN))
    val finalDf = df.select(columns: _*).distinct()
      .withColumn("system_name", lit(system))
      .withColumn("interface_name", lit(name))

    (Option(finalDf), false)
  }

  def selectCampaign(df: DataFrame, params: Params, campaignColumn: String,
             system: String, name: String): (Seq[String], Boolean) = {
    import org.apache.spark.sql.functions._

    val wrongColumn = !df.columns.contains(campaignColumn.toLowerCase)

    if (wrongColumn) {
      return (Seq.empty, true)
    }

    val campaigns = df.select(col(campaignColumn))
      .distinct()
      .collect()
      .map(_(0).toString)

    (campaigns, false)
  }

  def append(df: DataFrame, params: Params): Unit = {
    val credentials = AWSCredentials.fetch()

    df.write.format("com.databricks.spark.redshift")
      .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
      .option("url", params.jdbc())
      .option("tempdir", params.tempS3dir())
      .option("dbtable", s"public.$TRACKING_TABLE")
      .option("temporary_aws_access_key_id", credentials.accessKey)
      .option("temporary_aws_secret_access_key", credentials.secretKey)
      .option("temporary_aws_session_token", credentials.token)
      .option("tempformat", "CSV GZIP")
      .mode(SaveMode.Append)
      .save()
  }

  def fetch(conn: Connection, params: Params): Seq[String] = {
    val dsl = DB.dsl(conn)
    val campaignField = field("ANIOCAMPANA", classOf[String])

    val select = dsl.selectDistinct(campaignField)
      .from(name("public", TRACKING_TABLE))
      .where("1=1")

    val result = params.partitioningSpecification.dailyMappings.foldLeft(select) {
      (query, mapping) =>
        val (column, part) = mapping
        val lit = value(part, classOf[String])

        query.and(lit.isNull or field(column, classOf[String]).equal(lit))
    }.fetch(campaignField).asScala

    logIt(s"campaigns are $result")
    result
  }
}
