package pe.com.belcorp.datalake.analytics.addons

import java.sql.Connection

import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.utils.{DB, Params, Resources}
import pe.com.belcorp.datalake.utils.DB.ExtendedDSLContext

trait CreateFromResource {
  this: Table =>

  /**
    * Path to resource containing table creation script
    */
  val createPath: String

  /**
    * Loads creation script from resources
    *
    * @return SQL string
    */
  def createSQL: String = Resources.load(createPath)

  override def create(conn: Connection, params: Params): Unit = {
    DB.dsl(conn).executeNamed(createSQL, inlined = Map(
      "tableName" -> qualifiedName.toString
    ))
  }
}
