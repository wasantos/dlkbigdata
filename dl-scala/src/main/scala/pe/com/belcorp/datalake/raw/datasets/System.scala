package pe.com.belcorp.datalake.raw.datasets

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.datalake.raw.datasets.impl.{DefaultInterfaceImpl, DefaultRedshiftWritersImpl}
import pe.com.belcorp.datalake.raw.datasets.systems._
import pe.com.belcorp.datalake.utils.Params

/**
  * Trait representing a full system to be processed
  */
trait System {

  /**
    * System name
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
    * Interfaces to be processed, in order
    */
  def interfaces: Seq[Interface]

  /**
    * Interface map for easier access
    */
  def interfaceMap: Map[String, Interface] = {
    interfaces.map(itf => itf.name -> itf).toMap
  }

  /**
    * Convenience method for creating a new child interface for the system
    * @param name name for the interface
    * @param redshiftWriter a function which writes the interface into Redshift,
    *                       using the given ingestion parameters
    * @param glueSchemaSource the name of the Source Glue/Hive schema to be used
    * @param glueSchemaTarget the name of the Target Glue/Hive schema to be used
    * @param redshiftSchema the name of the Redshift schema to be used
    * @param csvPrefix prefix for all CSV tables
    * @param parquetPrefix prefix for all Parquet tables
    * @return a fully configured interface
    */
  final protected def interface(
    name: String,
    redshiftWriter: (Interface, SparkSession, Params) => Unit,
    keyColumns: Seq[String] = Seq.empty,
    partitionColumns: Seq[String] = Seq.empty,
    campaignColumn: String = null,
    glueSchemaSource: String = this.glueSchemaSource,
    glueSchemaTarget: String = this.glueSchemaTarget,
    redshiftSchema: String = this.redshiftSchema,
    csvPrefix: String = "",
    parquetPrefix: String = "",
    circuitBreaker: (SparkSession, Params, DataFrame) => Boolean = null): Interface = {

    new DefaultInterfaceImpl(
      this, name, keyColumns, partitionColumns, Option(campaignColumn), glueSchemaSource, glueSchemaTarget,
      redshiftSchema, redshiftWriter, csvPrefix, parquetPrefix, circuitBreaker)
  }
}

/**
  * Convenience properties
  */
object System {
  import DefaultRedshiftWritersImpl._
  import scala.collection.mutable.LinkedHashMap

  val APPEND: Writer = AppendOnly
  val OVERWRITE: Writer = OverwriteOnly
  val MERGE: Writer = MergeRecent

  type SystemFactory = Params => System

  // Use LinkedHashMap to preserve insertion order
  val REGISTRY = LinkedHashMap[String, SystemFactory](
    "sicc" -> (p => new Sicc(p)),
    "planit" -> (p => new PlanIt(p)),
    "sap" -> (p => new Sap(p)),
    "bi" -> (p => new BI(p)),
    "digital" -> (p => new Digital(p))
  )

  /**
    * Fetch a single system
    * @param name system name
    * @param params ingestion parameters
    * @return fully configured system instance
    */
  def fetch(name: String, params: Params): System =
    REGISTRY(name)(params)

  /**
    * Fetch all systems available
    * @param params ingestion parameters
    * @return fully configured system instances
    */
  def fetchAll(params: Params): Seq[System] =
    REGISTRY.values.map(_.apply(params)).toSeq
}
