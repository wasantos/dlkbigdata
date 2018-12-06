package pe.com.belcorp.datalake.raw.datasets

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jooq.conf.ParamType
import pe.com.belcorp.datalake.utils.DB._
import pe.com.belcorp.datalake.utils._
import pe.com.belcorp.datalake.utils.Goodies.logIt

/**
  * Trait representing a specific interface to be processed
  */
trait InterfaceFunctional {

  /**
    * Parent system
    */
  val system: SystemFunctional

  /**
    * Interface name
    */
  val name: String

  /**
    * Source Glue/Hive schema
    */
  val glueSchemaLanding: String

  /**
    * Source Glue/Hive schema
    */
  val glueSchemaSource: String

  /**
    * Target Glue/Hive schema
    */
  val glueSchemaTarget: String

  /**
    * Redshift schema
    */
  val redshiftSchema: String

  /**
    * Redshift SQL create script path
    */
  val redshiftCreateScriptPath: String

  /**
    * Marks if table should be populated only once
    */
  val redshiftPopulateOnce: Boolean

  /**
    * Loads creation script from resources
    *
    * @return SQL string
    */
  def redshiftCreateScript: String = Resources.load(redshiftCreateScriptPath)

  /**
    * @return partition columns for interface
    */
  val partitionColumns: Seq[String]

  /**
    * @return key columns for interface
    */
  val keyColumns: Seq[String]

  /**
    * @return which column should be used to track campaigns in this interface
    */
  val campaignColumn: Option[String]

  /**
    * Source tables
    */
  def sourceTables: Seq[String]

  /**
    * Target table
    */
  def targetTable: String

  /**
    * @return all fully qualified system names to be fetched in the raw Source datalake
    */
  def qualifiedSrcTableNames: Seq[String] = sourceTables.map(qualifiedSrcTableName)

  /**
    * @return a fully qualified system name to be fetched in the raw Source datalake
    */
  def qualifiedSrcTableName(name: String): String = s"$glueSchemaSource.$name"

  /**
    * @return a fully qualified system name to be fetched in the raw Target datalake
    */
  def qualifiedTgtTableName: String = s"$glueSchemaTarget.$targetTable"

  /**
    * @return a fully qualified system name to be fetched in the raw Target datalake
    */
  def qualifiedRedshiftTableName: String = s"$redshiftSchema.$targetTable"


  /**
    * Process the interface, returning the campaigns included in the dataset
    * for the given parameters, if present
    *
    * @param spark a correctly configured SparkSession
    * @param params the parameters for the ingestion
    * @return a possible DataFrame with the needed query
    */
  def getCampaigns(spark: SparkSession, params: Params): Seq[String]


  /**
    * Process the interface, putting source and target together and writing it to Parquet storage
    *
    * @param spark a correctly configured SparkSession
    * @param params the parameters for the ingestion
    */
  def updateFunctionalParquet(spark: SparkSession, params: Params): Unit

  /**
    * Process the interface, reading from Parquet and writing it to Redshift
    *
    * @param spark a correctly configured SparkSession
    * @params db a correctly configured connection manager
    * @param params the parameters for the ingestion
    */
  def updateFunctionalRedshift(spark: SparkSession, db: DB, params: Params): Unit = {
    import spark.implicits._

    if(!spark.catalog.tableExists(qualifiedTgtTableName)) {
      logIt(s"$qualifiedTgtTableName does not exist yet, skipping")
      return
    }

    val columnQuery = db.transaction { conn =>
      import org.jooq.impl.DSL.{name => sqlName}

      val dsl = DB.dsl(conn)

      // Ensure existence check works
      dsl.execute(s"SET search_path TO $redshiftSchema, '$$user', public")

      val tableMissing = !dsl.fetchExists(
        dsl.selectOne().from("pg_table_def")
          .where("schemaname = ?", redshiftSchema)
          .and("tablename = ?", targetTable))

      // Quit if table already exists and should be updated only once
      if (!tableMissing && redshiftPopulateOnce) {
        return
      }

      // Create table if needed
      if (tableMissing) {
        dsl.executeNamed(redshiftCreateScript, inlined = Map(
          "tableName" -> qualifiedRedshiftTableName
        ))
      }

      // create query to recover column names from Redshift
      dsl.select()
        .from(sqlName(redshiftSchema, targetTable))
        .getSQL(ParamType.INLINED)
    }

    val campaigns = getCampaigns(spark, params)
    val credentials = AWSCredentials.fetch()
    val columns = spark.read
      .format("com.databricks.spark.redshift")
      .option("url", params.jdbc())
      .option("tempdir", params.tempS3dir())
      .option("query", columnQuery)
      .option("temporary_aws_access_key_id", credentials.accessKey)
      .option("temporary_aws_secret_access_key", credentials.secretKey)
      .option("temporary_aws_session_token", credentials.token)
      .option("tempformat", "CSV GZIP")
      .load() // Load DataFrame schema
      .columns

    val redshiftPartitions = partitionColumns.map {
      case "aniocampana" =>
        if(columns.exists(_.equalsIgnoreCase("aniocampanaweb"))) "aniocampanaweb"
        else "aniocampana"
      case any: String => any
    }

    // Delete old data
    db.transaction { conn =>
      import org.jooq.impl.DSL.{field, table}

      val dsl = DB.dsl(conn)

      if (redshiftPartitions.isEmpty) {
        dsl.truncate(table(qualifiedRedshiftTableName)).execute()
      } else {
        val baseDelete = dsl.deleteFrom(table(qualifiedRedshiftTableName))
          .where("1=1")

        val fullDelete = redshiftPartitions.foldLeft(baseDelete) {
          (delete, partition) =>
            partition match {
              case "pt_country" =>
                delete.and("codpais = ?", params.country())
              case "codpais" =>
                delete.and("codpais = ?", params.country())
              case "aniocampana" =>
                delete.and(
                  field("aniocampana", classOf[String]).in(campaigns: _*))
              case "aniocampanaweb" =>
                delete.and(
                  field("aniocampanaweb", classOf[String]).in(campaigns: _*))
            }
        }

        fullDelete.execute()
      }
    }

    val df = partitionColumns.foldLeft(spark.table(qualifiedTgtTableName)) {
      (df, partition) =>
        partition match {
          case "pt_country" => df.where($"pt_country" === params.country())
          case "codpais" => df.where($"codpais" === params.country())
          case "aniocampana" => df.where($"aniocampana".isin(campaigns: _*))
          case "aniocampanaweb" => df.where($"aniocampanaweb".isin(campaigns: _*))
        }
    }.selectExpr(columns: _*)

    df.write.format("com.databricks.spark.redshift")
      .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
      .option("url", params.jdbc())
      .option("tempdir", params.tempS3dir())
      .option("dbtable", qualifiedRedshiftTableName)
      .option("temporary_aws_access_key_id", credentials.accessKey)
      .option("temporary_aws_secret_access_key", credentials.secretKey)
      .option("temporary_aws_session_token", credentials.token)
      .option("tempformat", "CSV GZIP")
      .mode(SaveMode.Append)
      .save()
  }
}
