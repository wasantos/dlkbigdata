package pe.com.belcorp.datalake.raw.datasets.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.utils.Goodies.logIt
import org.apache.spark.sql.catalyst.TableIdentifier
import pe.com.belcorp.datalake.utils.SparkUtils.getStagingPath

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
  private val parquetPrefix: String,
  val circuitBreaker: (SparkSession, Params, DataFrame) => Boolean) extends Interface {

  //parquetPrefix e csvPrefix definidos em glueutils.py
  override def parquetTableName: String = s"${parquetPrefix}${system.name}_${name}"
  override def tableName: String = s"${csvPrefix}${system.name}_${name}"
  override def redshiftTableName: String = s"${parquetPrefix}${system.name}_${name}"

  override def updateStagingParquet(spark: SparkSession, params: Params): Unit = {
    if(!spark.catalog.tableExists(qualifiedSrcTableName)) {
      logIt(s"$tableName doesn't exists yet, skipping write")
      return
    }

    val df_source_partition = params.partitioningSpecification.apply(spark.table(qualifiedSrcTableName))

    // Quit early if necessary
    val breakEarly = Option(circuitBreaker)
      .map(_(spark, params, df_source_partition)).getOrElse(false)

    if(breakEarly) {
      logIt("circuitBreaker returned true")
      return
    }

    // Reorder columns because Spark ducking sucks
    val columns = df_source_partition.columns
    val reordered = reorderColumns(columns, this.partitionColumns)

    val df_source_reordered = df_source_partition.selectExpr(reordered: _*)

    //verifica se a tabela destino existe
    if(!spark.catalog.tableExists(qualifiedTgtTableName)) {
      //crea la tabla en glue
      df_source_reordered.coalesce(4)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy(this.partitionColumns: _*)
        .saveAsTable(qualifiedTgtTableName)

      return
    }

    // select distinct partitioncolumns FROM df_source
    val df_source_distinct = df_source_reordered.selectExpr(this.partitionColumns :_*)
      .distinct()
      .repartition(200)

    val df_target_raw = spark.table(qualifiedTgtTableName)
      .repartition(200)

    //select no df_target com os campos keyColumns pelos valores de cada coluna no df_source
    val df_target_reordered = df_target_raw.join(df_source_distinct, partitionColumns, "leftsemi")
      .selectExpr(reordered: _*)

    val df_total = df_source_reordered.union(df_target_reordered)
      .select(col("*"),
        row_number().over(
          Window.partitionBy(this.keyColumns.head, this.keyColumns.tail:_*)
            .orderBy(col("pt_year").desc,
              col("pt_month").desc,
              col("pt_day").desc,
              col("pt_secs").desc)).as("ORD"))
      .where("ORD = 1")
      .drop(col("ORD"))

    logIt(s"SOURCE: ${qualifiedSrcTableName} [UNION] TARGET: ${qualifiedTgtTableName}") // => ${df_total.count()}

    val numPartitions = 4
    val tmpPath = getStagingPath("temp", qualifiedTgtTableName)

    df_total.coalesce(numPartitions)
      .write
      .mode(SaveMode.ErrorIfExists)
      .parquet(tmpPath)

    // If table exists, use insertInto to not delete it
    spark.read.parquet(tmpPath)
      .repartition(numPartitions * 32)
      .coalesce(numPartitions)
      .selectExpr(reordered: _*)
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(qualifiedTgtTableName)
  }

  private def reorderColumns(columns: Seq[String], partitions: Seq[String]): Seq[String] = {
    val lowercase = partitions.map(_.toLowerCase)
    val filtered = columns.filterNot(lowercase.contains)

    if (partitions.nonEmpty) {
      filtered ++ lowercase
    } else {
      filtered
    }
  }


  @Deprecated
  override def writeToParquet(spark: SparkSession, params: Params): Unit = {
    if(!spark.catalog.tableExists(qualifiedSrcTableName)) {
      logIt(s"$tableName doesn't exists yet, skipping write")
      return
    }

    val df = params.partitioningSpecification.apply(spark.table(qualifiedSrcTableName))
    df.write.insertInto(qualifiedTgtTableName)
  }

  @Deprecated
  override def getCampaigns(spark: SparkSession, params: Params): Option[DataFrame] = {
    if(!spark.catalog.tableExists(qualifiedSrcTableName)) {
      logIt(s"$tableName doesn't exists yet, skipping scan")
      return None
    }

    val df = params.partitioningSpecification.apply(spark.table(qualifiedSrcTableName))
    val (campaignsDf, wrongColumn) = CampaignTracking.select(df, params, campaignColumn, system.name, name)

    if(wrongColumn) {
      logIt(s"column '${campaignColumn.get}' does not exists in $tableName, skip, fix registry settings")
    }

    campaignsDf
  }

  @Deprecated
  override def writeToRedshift(spark: SparkSession, params: Params): Unit = {
    redshiftWriter(this, spark, params)
  }
}
