package pe.com.belcorp.datalake.analytics.dwh

import java.sql.Connection

import org.jooq.Name
import org.jooq.impl.DSL.{selectFrom, table}
import pe.com.belcorp.datalake.analytics.OldTable
import pe.com.belcorp.datalake.analytics.addons.{ByCountry, OverwriteUpdate}
import pe.com.belcorp.datalake.utils.DB._
import pe.com.belcorp.datalake.utils.Goodies.logIt
import pe.com.belcorp.datalake.utils.{DB, Params, Resources}

class DEbelista(val schema: String, val sourceSchema: String)
  extends OldTable with ByCountry with OverwriteUpdate {
  override val createPath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DEbelista/create.sql"
  override val updatePath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DEbelista/update.sql"
  override val name: String = "dwh_debelista"
  override val primaryKeys: Seq[String] = Seq("CODPAIS", "CODEBELISTA")

  override def deleteFromPermanentTable(conn: Connection, params: Params, tempName: Name): Unit = {
    deleteUsingKeys(conn, params, tempName)
  }

  override def populateTempTable(conn: Connection, params: Params, tempName: Name): Unit = {
    DB.dsl(conn).executeNamed(updateSQL, updateParameters(params),
      Map("tableName" -> tempName.toString, "landingSchema" -> sourceSchema))
  }

  def updateNew: String = Resources.load(
    "pe/com/belcorp/datalake/resources/analytics/dwh/DEbelista/update_new.sql")

  def updateOld: String = Resources.load(
    "pe/com/belcorp/datalake/resources/analytics/dwh/DEbelista/update_old.sql")

  override protected def executeUpdateTransaction(conn: Connection, params: Params, newData: Name): Unit = {
    var exc: Exception = null

    for (i <- 0 until 5) {
      try {
        DB.transaction(conn) { conn =>
          val dsl = DB.dsl(conn)

          // Update temporary table with new data
          dsl.executeNamed(updateNew, updateParameters(params), Map(
            "tableName" -> newData.toString,
            "landingSchema" -> sourceSchema,
            "functionalSchema" -> schema))

          // Update temporary table with data from old data
          dsl.executeNamed(updateOld, updateParameters(params), Map(
            "tableName" -> newData.toString,
            "landingSchema" -> sourceSchema,
            "functionalSchema" -> schema))

          // Delete old data from original table
          deleteFromPermanentTable(conn, params, newData)

          // Insert on original table
          DB.dsl(conn).insertInto(table(qualifiedName))
            .select(selectFrom(table(newData))).execute()
        }

        // Quit function if successful
        return
      } catch {
        case e: Exception =>
          logIt(s"[ERROR] Update transaction failed (attempt ${i + 1})")
          // Save exception for rethrowing
          exc = e
          // Retry after a minute
          Thread.sleep(60 * 1000)
      }
    }

    throw exc
  }
}
