package pe.com.belcorp.datalake.analytics

import java.sql.Connection

import org.jooq.Name
import org.jooq.impl.DSL.{name => sqlName}
import pe.com.belcorp.datalake.utils.Params

/**
  * Trait representing a output table for analytic modeling processing.
  */
trait Table {
  /**
    * Sets in which schema the table should be created.
    */
  val schema: String

  /**
    * Sets the source schema for the data from which the table should be created
    */
  val sourceSchema: String

  /**
    * Name for table
    */
  val name: String

  /**
    * Partitioning columns (for documentation/export)
    */
  val partitions: Seq[String] = Seq.empty

  /**
    * Key columns (for updating)
    */
  val primaryKeys: Seq[String] = Seq.empty

  /**
    * Creates the table
    */
  def create(conn: Connection, params: Params): Unit

  /**
    * Updates the table
    */
  def update(conn: Connection, params: Params): Unit

  /**
    * Qualified name
    */
  def qualifiedName: Name = sqlName(schema, name)
}
