package pe.com.belcorp.datalake.raw.datasets.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.utils.Goodies.logIt

/**
  * Default implementation for interfaces
  */
class DefaultInterfaceImpl(
  override val system: System,
  override val name: String,
  override val keyColumns: Seq[String],
  override val partitionColumns: Seq[String],
  override val campaignColumn: Option[String],
  override val glueSchemaSource: String,
  override val glueSchemaTarget: String,
  override val redshiftSchema: String,
  val redshiftWriter: (Interface, SparkSession, Params) => Unit,
  private val csvPrefix: String,
  private val parquetPrefix: String) extends Interface {

  override def parquetTableName: String = s"${parquetPrefix}${system.name}_${name}"
  override def tableName: String = s"${csvPrefix}${system.name}_${name}"
  override def redshiftTableName: String = s"${parquetPrefix}${system.name}_${name}"

  override def updateLandingParquet(spark: SparkSession, params: Params): Unit = {
    if(!spark.catalog.tableExists(qualifiedTgtTableName)) {
      logIt(s"$tableName doesn't exists yet, skipping write")
      return
    }

    //select no df_target com os campos keyColumns utilizando os valores de cada coluna no df_source
    //    val df_target = params.partitioningSpecification.apply(spark.table(qualifiedTgtTableName))
    val df_target = spark.table(qualifiedTgtTableName)

    val df_source = params.partitioningSpecification.apply(spark.table(qualifiedSrcTableName))

    val df_total = df_source.union(df_target)
      .select(col("*"),
        row_number().over(
          Window.partitionBy(df_target.columns.head, df_target.columns.tail:_*)
            .orderBy("pt_year","pt_month", "pt_day", "pt_secs")).as("ORD"))
      .filter("ORD = 1").drop(col("ORD"))

    logIt(s"SOURCE: ${qualifiedSrcTableName} [UNION] TARGET: ${qualifiedTgtTableName} => ${df_total.count()}")

    df_total.write.mode(SaveMode.Overwrite).insertInto(qualifiedTgtTableName)
  }

  override def writeToParquet(spark: SparkSession, params: Params): Unit = {
    if(!spark.catalog.tableExists(qualifiedSrcTableName)) {
      logIt(s"$tableName doesn't exists yet, skipping write")
      return
    }

    val df = params.partitioningSpecification.apply(spark.table(qualifiedSrcTableName))
    df.write.insertInto(qualifiedTgtTableName)
  }

  override def getCampaigns(spark: SparkSession, params: Params): Option[DataFrame] = {
    if(!spark.catalog.tableExists(qualifiedSrcTableName)) {
      logIt(s"$tableName doesn't exists yet, skipping scan")
      return None
    }

    val df = params.partitioningSpecification.apply(spark.table(qualifiedSrcTableName))
    val (campaignsDf, wrongColumn) = CampaignTracking.select(df, params, campaignColumn,
      system.name, name)

    if(wrongColumn) {
      logIt(
        s"column '${campaignColumn.get}' does not exists in $tableName, skip, fix registry settings"
      )
    }

    campaignsDf
  }

  override def writeToRedshift(spark: SparkSession, params: Params): Unit = {
    redshiftWriter(this, spark, params)
  }
}
