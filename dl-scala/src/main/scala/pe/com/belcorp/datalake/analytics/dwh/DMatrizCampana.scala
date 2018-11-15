package pe.com.belcorp.datalake.analytics.dwh

import java.sql.Connection

import org.jooq.Name
import pe.com.belcorp.datalake.analytics.OldTable
import pe.com.belcorp.datalake.analytics.addons.{ByCountryAndCampaign, OverwriteUpdate}
import pe.com.belcorp.datalake.utils.DB._
import pe.com.belcorp.datalake.utils.{DB, Params}

class DMatrizCampana(val schema: String, val sourceSchema: String, override val campaigns: Seq[String])
  extends OldTable with ByCountryAndCampaign with OverwriteUpdate {
  override val createPath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DMatrizCampana/create.sql"
  override val updatePath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DMatrizCampana/update.sql"
  override val name: String = "dwh_dmatrizcampana"
  override val primaryKeys: Seq[String] = Seq(
    "CODPAIS", "ANIOCAMPANA", "CODCANALVENTA",
    "CODTIPOOFERTA", "CODSAP", "CODVENTA")

  override def deleteFromPermanentTable(conn: Connection, params: Params, tempName: Name): Unit = {
    deleteUsingKeys(conn, params, tempName)
  }

  override def populateTempTable(conn: Connection, params: Params, tempName: Name): Unit = {
    DB.dsl(conn).executeNamed(updateSQL, updateParameters(params),
      Map("tableName" -> tempName.toString, "landingSchema" -> sourceSchema))
  }

  override def skipUpdates: Boolean = campaigns.isEmpty
}
