package pe.com.belcorp.datalake.raw.datasets

import pe.com.belcorp.datalake.raw.datasets.impl.FunctionalInterfaceImpl
import pe.com.belcorp.datalake.raw.datasets.systems._
import pe.com.belcorp.datalake.utils.Params

/**
  * Trait representing a full system to be processed
  */
trait SystemFunctional {

  /**
    * System name
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
    * Interfaces to be processed, in order
    */
  def interfaces: Seq[InterfaceFunctional]

  /**
    * Convenience method for creating a new child interface for the system
    * @param name name for the interface
    * @param glueSchemaSource the name of the Source Glue/Hive schema to be used
    * @param glueSchemaTarget the name of the Target Glue/Hive schema to be used
    * @return a fully configured interface
    */
  final def interfacefunc(name: String,
                          sourceTables: Seq[String] = Seq.empty,
                          targetTable: String,
                          partitionColumns: Seq[String] = Seq.empty,
                          keyColumns: Seq[String] = Seq.empty,
                          campaignColumn: String = null,
                          glueSchemaLanding: String = this.glueSchemaLanding,
                          glueSchemaSource: String = this.glueSchemaSource,
                          glueSchemaTarget: String = this.glueSchemaTarget,
                          redshiftPopulateOnce: Boolean = false,
                          redshiftCreateScriptPath: String): InterfaceFunctional = {

    new FunctionalInterfaceImpl(this,
      name, sourceTables, targetTable, partitionColumns, keyColumns,
      Option(campaignColumn), glueSchemaLanding, glueSchemaSource, glueSchemaTarget,
      redshiftSchema, redshiftCreateScriptPath, redshiftPopulateOnce)
  }
}


/**
  * Convenience properties
  */
object SystemFunctional {
  import pe.com.belcorp.datalake.raw.datasets.impl.DefaultRedshiftWritersImpl._
  import scala.collection.mutable.LinkedHashMap

  val APPEND: Writer = AppendOnly
  val OVERWRITE: Writer = OverwriteOnly
  val MERGE: Writer = MergeRecent

  type SystemFactory = Params => SystemFunctional

  // Use LinkedHashMap to preserve insertion order
  val REGISTRY = LinkedHashMap[String, SystemFactory](
    "sicc" -> (p => new SiccFunctional(p)),
    "planit" -> (p => new PlanitFunctional(p)),
    "sap" -> (p => new SapFunctional(p)),
    "bi" -> (p => new BiFunctional(p)),
    "digital" -> (p => new DigitalFunctional(p))
  )

  /**
    * Fetch a single system
    * @param name system name
    * @param params ingestion parameters
    * @return fully configured system instance
    */
  def fetch(name: String, params: Params): SystemFunctional =
    REGISTRY(name)(params)

  /**
    * Fetch all systems available
    * @param params ingestion parameters
    * @return fully configured system instances
    */
  def fetchAll(params: Params): Seq[SystemFunctional] =
    REGISTRY.values.map(_.apply(params)).toSeq
}

