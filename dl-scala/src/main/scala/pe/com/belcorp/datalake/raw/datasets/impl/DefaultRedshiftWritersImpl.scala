package pe.com.belcorp.datalake.raw.datasets.impl

import java.sql.Connection
import java.util.UUID

import org.apache.spark.sql.{SaveMode, SparkSession}
import pe.com.belcorp.datalake.raw.datasets.Interface
import pe.com.belcorp.datalake.utils.Goodies.logIt
import pe.com.belcorp.datalake.utils._

object DefaultRedshiftWritersImpl {
  type Writer = (Interface, SparkSession, Params) => Unit

  // Creates writer for given Spark save mode
  private val writerFactory = (mode: SaveMode) => {
    val writer: Writer = (interface, spark, params) => {
      val sourceTable = interface.qualifiedSrcTableName

      if(spark.catalog.tableExists(sourceTable)) {
        val credentials = AWSCredentials.fetch()
        val df = params.partitioningSpecification.apply(spark.table(sourceTable))
        val manager = new DB(params.jdbc())

        // Truncates table if needed
        createBaseTableIfNotExists(manager, interface, spark, params)

        df.write.format("com.databricks.spark.redshift")
          .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
          .option("url", params.jdbc())
          .option("tempdir", params.tempS3dir())
          .option("dbtable", interface.qualifiedRedshiftTableName)
          .option("temporary_aws_access_key_id", credentials.accessKey)
          .option("temporary_aws_secret_access_key", credentials.secretKey)
          .option("temporary_aws_session_token", credentials.token)
          .option("tempformat", "CSV GZIP")
          .mode(mode)
          .save()
      } else {
        logIt(s"$sourceTable doesn't exists yet, skipping operation")
      }
    }

    writer
  }

  val AppendOnly: Writer = writerFactory(SaveMode.Append)
  val OverwriteOnly: Writer = writerFactory(SaveMode.Overwrite)

  // Dedicated Writer for merge scenarios
  val MergeRecent: Writer = (interface, spark, params) => {
    val table = interface.qualifiedSrcTableName

    if(spark.catalog.tableExists(table))
      mergeTo(interface, spark, params)
    else
      logIt(s"$table doesn't exists yet, skipping merge")
  }

  // Returns true if table was created right now
  private def createBaseTableIfNotExists(manager: DB, interface: Interface,
                                         spark: SparkSession, params: Params): Boolean = {
    val lockName = s"landingRedshiftTableCreate: ${interface.qualifiedRedshiftTableName}"

    Lock.acquire(lockName) { _ =>
      manager.transaction { conn =>
        val dsl = DB.dsl(conn)

        logIt(s"schemaname = ${interface.redshiftSchema}")
        logIt(s"tablename = ${interface.redshiftTableName}")

        // Ensure existence check works
        dsl.execute(s"SET search_path TO ${interface.redshiftSchema}, '$$user', public")

        val tableMissing = !dsl.fetchExists(
          dsl.selectOne().from("pg_table_def")
            .where("schemaname = ?", interface.redshiftSchema)
            .and("tablename = ?", interface.redshiftTableName))

        if (tableMissing) {
          createBaseTable(conn, interface, spark, params, interface.qualifiedRedshiftTableName)
        }

        tableMissing
      }
    }
  }

  // Base function to create Redshift table
  private def createBaseTable(conn: Connection, interface: Interface,
                              spark: SparkSession, params: Params,
                              tableName: String): Unit = {
    // Set sortkeys
    val sortKeys = interface.keyColumns
    val countryCol = PartitioningSpecification.COUNTRY_COLUMN

    val (actualKeys, distByKey) = if (sortKeys.contains(countryCol)) {
      (sortKeys.filterNot(_ == countryCol), true)
    } else {
      (sortKeys, false)
    }

    val sortKeysSpec =
      if (actualKeys.isEmpty) ""
      else s"COMPOUND SORTKEY(${actualKeys.mkString(",")})"

    // Set columns
    val columns = spark.table(interface.qualifiedSrcTableName).columns
    val columnsSpec = columns.map { c => s"$c VARCHAR(MAX)" }.mkString(",\n")

    // Set distkey
    val distKeySpec =
      if (distByKey) s"DISTKEY($countryCol)"
      else "DISTSTYLE EVEN"

    val dml =
      s"""
         |CREATE TABLE $tableName (
         |  $columnsSpec)
         |$distKeySpec
         |$sortKeysSpec
       """.stripMargin

    execute(conn, dml)
  }

  private def mergeTo(interface: Interface, spark: SparkSession, params: Params): Unit = {
    if (interface.keyColumns.isEmpty) {
      throw new IllegalArgumentException(
        "This writer needs a list of key columns to be executed")
    }

    val tempTableName = generateTempTableName()
    val manager = new DB(params.jdbc())

    try {
      saveRawTempTable(interface, spark, params, tempTableName, manager)
      processTempTable(interface, spark, params, tempTableName, manager)
    } finally {
      dropTempTable(manager, tempTableName)
      manager.close()
    }
  }

  private def saveRawTempTable(interface: Interface, spark: SparkSession,
                               params: Params, tempTableName: String,
                               manager: DB): Unit = {
    manager.checkout { conn =>
      createBaseTable(conn, interface, spark, params, tempTableName)

      val credentials = AWSCredentials.fetch()
      val df = params.partitioningSpecification.apply(
        spark.table(interface.qualifiedSrcTableName))

      df.write.format("com.databricks.spark.redshift")
        .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
        .option("url", params.jdbc())
        .option("tempdir", params.tempS3dir())
        .option("dbtable", tempTableName)
        .option("temporary_aws_access_key_id", credentials.accessKey)
        .option("temporary_aws_secret_access_key", credentials.secretKey)
        .option("temporary_aws_session_token", credentials.token)
        .option("tempformat", "CSV GZIP")
        .mode(SaveMode.Append)
        .save()
    }
  }

  private def processTempTable(interface: Interface, spark: SparkSession,
                               params: Params, rawTempTable: String,
                               manager: DB): Unit = {
    val source = rawTempTable
    val target = interface.qualifiedRedshiftTableName
    val created = createBaseTableIfNotExists(manager, interface, spark, params)

    manager.transaction { conn =>
      if (!created) {
        val conditions = if (interface.keyColumns.nonEmpty) {
          val leftCols = interface.keyColumns
            .map { c => s"NVL($target.${c}, '')" }.mkString(", ")
          val rightCols = interface.keyColumns
            .map { c => s"NVL($source.${c}, '')" }.mkString(", ")

          s"($leftCols) IN (SELECT $rightCols FROM $source)"
        } else {
          "1=1"
        }

        execute(conn, s"DELETE FROM $target WHERE $conditions")
      }

      execute(conn, s"INSERT INTO $target SELECT * FROM $source")
    }
  }

  private def dropTempTable(manager: DB, tempTableName: String): Unit = {
    manager.checkout { conn =>
      val dsl = DB.dsl(conn)
      dsl.dropTableIfExists(tempTableName).execute()
    }
  }

  private def generateTempTableName(): String = {
    s"tmp_${UUID.randomUUID().toString.replace('-', '_')}"
  }

  private def execute(conn: Connection, sql: String): Unit = {
    logIt(s"[DEBUG][SQL] Running SQL: $sql")
    conn.createStatement().execute(sql)
  }
}
