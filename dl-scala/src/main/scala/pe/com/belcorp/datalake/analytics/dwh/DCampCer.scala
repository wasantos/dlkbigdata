package pe.com.belcorp.datalake.analytics.dwh

import java.sql.Connection

import org.jooq.Name
import pe.com.belcorp.datalake.analytics.OldTable
import pe.com.belcorp.datalake.analytics.addons.{ByCountry, OverwriteUpdate}
import pe.com.belcorp.datalake.utils.DB._
import pe.com.belcorp.datalake.utils.{DB, Params}

class DCampCer(val schema: String, val sourceSchema: String) extends OldTable
  with ByCountry with OverwriteUpdate {
  override val createPath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DCampCer/create.sql"
  override val updatePath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DCampCer/update.sql"
  override val name: String = "ctr_cierre_sicc"

  override def populateTempTable(conn: Connection, params: Params, tempName: Name): Unit = {
    DB.dsl(conn).executeNamed(updateSQL, updateParameters(params),
      Map("tableName" -> tempName.toString, "landingSchema" -> sourceSchema))
  }

  override def deleteFromPermanentTable(conn: Connection, params: Params, tempName: Name): Unit = {
    import org.jooq.impl.DSL._

    DB.dsl(conn).deleteFrom(table(qualifiedName))
      .where(field("CODPAIS", classOf[String]).equal(params.country()))
      .execute()
  }
}
