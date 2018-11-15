package pe.com.belcorp.datalake.analytics.addons

import java.sql.Connection

import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.utils.DB._
import pe.com.belcorp.datalake.utils.{DB, Params, Resources}

trait UpdateFromResource {
  this: Table =>

  /**
    * Path to resource containing table update script
    */
  val updatePath: String

  /**
    * Returns SQL to run for updating the table. May be parameterized.
    *
    * @return SQL string
    */
  def updateSQL: String = Resources.load(updatePath)

  /**
    * Parameters to apply in the update statement
    *
    * @param params the parameter object given to the ingestion
    * @return a sequence of name-value tuples to apply to the update
    */
  def updateParameters(params: Params): Map[String, Any] = Map.empty

  override def update(conn: Connection, params: Params): Unit = {
    val dsl = DB.dsl(conn)

    dsl.executeNamed(updateSQL, updateParameters(params))
  }
}
