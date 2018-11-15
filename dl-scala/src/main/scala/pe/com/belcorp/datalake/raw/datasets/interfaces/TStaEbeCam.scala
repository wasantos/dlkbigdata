package pe.com.belcorp.datalake.raw.datasets.interfaces

import org.apache.spark.sql.SparkSession
import pe.com.belcorp.datalake.raw.datasets.System
import pe.com.belcorp.datalake.raw.datasets.impl.{DefaultInterfaceImpl, DefaultRedshiftWritersImpl}
import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.utils.Goodies.logIt

/**
  * Implements custom logic for TStaEbeCam interface
  * @param system parent system
  * @param glueSchema Glue/Hive schema for landing tables
  * @param redshiftSchema Redshift schema for functional tables
  * @param csvPrefix Prefix for CSV tables
  * @param parquetPrefix Prefix for Parquet tables
  */
class TStaEbeCam(
  override val system: System,
  override val glueSchemaSource: String, override val glueSchemaTarget: String, override val redshiftSchema: String,
  csvPrefix: String = "", parquetPrefix: String = "tbpq_") extends DefaultInterfaceImpl(
  system,
  "tstaebecam",
  Seq("pt_country", "aniocampana"),
  Seq("pt_country", "aniocampana"),
  Some("aniocampana"),
  glueSchemaSource, glueSchemaTarget, redshiftSchema, DefaultRedshiftWritersImpl.MergeRecent,
  csvPrefix, parquetPrefix
) {
  /**
    * Name of true FStaEbeCam
    */
  val targetName = "fstaebecam"

  // Override table names
  override def redshiftTableName: String = s"${parquetPrefix}${system.name}_${targetName}"

  override def writeToRedshift(spark: SparkSession, params: Params): Unit = {
    val table = qualifiedSrcTableName

    // Quit if table doesn't exist yet
    if(!spark.catalog.tableExists(table)) {
      logIt(s"$table doesn't exists yet, skipping operation")
      return
    }

    val df = params.partitioningSpecification.apply(spark.table(table))

    // Quit if TStaEbeCam is empty
    if (df.head(1).isEmpty) {
      logIt(s"$table is empty, skipping operation")
      return
    }

    // Otherwise, if TStaEbeCam exists for the given ingestion,
    // replace whole campaign of FStaEbeCam from it using merge writer
    redshiftWriter(this, spark, params)
  }
}
