package pe.com.belcorp.datalake.raw.datasets

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.datalake.utils.Params

/**
  * Trait representing a specific interface to be processed
  */
trait Interface {

  /**
    * Parent system
    */
  val system: System

  /**
    * Interface name
    */
  val name: String

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
    * @return a system name to be fetched in the raw Parquet datalake
    */
  def parquetTableName: String

  /**
    * @return a system name to be fetched in the raw CSV datalake
    */
  def tableName: String

  /**
    * @return a fully qualified system name to be fetched in the raw Source datalake
    */
  def qualifiedSrcTableName: String = s"$glueSchemaSource.$tableName"

  /**
    * @return a fully qualified system name to be fetched in the raw Target datalake
    */
  def qualifiedTgtTableName: String = s"$glueSchemaTarget.$parquetTableName"

  /**
    * @return a system name to be used as output for the processing
    */
  def redshiftTableName: String

  /**
    * @return a fully qualified system name to be used as output for the processing
    */
  def qualifiedRedshiftTableName: String = s"$redshiftSchema.$redshiftTableName"


  /**
    * @return key columns for interface
    */
  val keyColumns: Seq[String]

  /**
    * @return key columns for interface
    */
  val partitionColumns: Seq[String]


  /**
    * @return which column should be used to track campaigns in this interface
    */
  val campaignColumn: Option[String]

  /**
    * Process the interface, putting source and target together and writing it to Parquet storage
    *
    * @param spark a correctly configured SparkSession
    * @param params the parameters for the ingestion
    */
  def updateStagingParquet(spark: SparkSession, params: Params): Unit

  /**
    * Process the interface, writing it to Parquet storage
    *
    * @param spark a correctly configured SparkSession
    * @param params the parameters for the ingestion
    */
  def writeToParquet(spark: SparkSession, params: Params): Unit

  /**
    * Process the interface, returning the campaigns included in the dataset
    * for the given parameters, if present
    *
    * @param spark a correctly configured SparkSession
    * @param params the parameters for the ingestion
    * @return a possible DataFrame with the needed query
    */
  def getCampaigns(spark: SparkSession, params: Params): Option[DataFrame]

  /**
    * Process the interface, writing it to Redshift storage
    *
    * @param spark a correctly configured SparkSession
    * @param params the parameters for the ingestion
    */
  def writeToRedshift(spark: SparkSession, params: Params): Unit
}
