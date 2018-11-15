package pe.com.belcorp.datalake.analytics.dwh

import java.sql.Connection

import org.jooq.Name
import pe.com.belcorp.datalake.analytics.OldTable
import pe.com.belcorp.datalake.analytics.addons.{ByCountry, OverwriteUpdate}
import pe.com.belcorp.datalake.utils.DB._
import pe.com.belcorp.datalake.utils.{DB, Params}

class DComportamientoRolling(val schema: String, val sourceSchema: String)
  extends OldTable with ByCountry with OverwriteUpdate {
  override val createPath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DComportamientoRolling/create.sql"
  override val updatePath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DComportamientoRolling/update.sql"
  override val name: String = "dwh_dcomportamientorolling"
  override val primaryKeys: Seq[String] = Seq("CODPAIS", "CODCOMPORTAMIENTO")

  override def deleteFromPermanentTable(conn: Connection, params: Params, tempName: Name): Unit = {
    deleteUsingKeys(conn, params, tempName)
  }

  override def populateTempTable(conn: Connection, params: Params, tempName: Name): Unit = {
    DB.dsl(conn).executeNamed(updateSQL, updateParameters(params),
      Map("tableName" -> tempName.toString, "landingSchema" -> sourceSchema))
  }
}
