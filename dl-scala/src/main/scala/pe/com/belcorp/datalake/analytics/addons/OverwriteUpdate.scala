package pe.com.belcorp.datalake.analytics.addons

import java.sql.Connection
import java.util.UUID

import org.jooq.Name
import org.jooq.impl.DSL.{name => sqlName, _}
import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.utils.{DB, Params}
import pe.com.belcorp.datalake.utils.Goodies.logIt

trait OverwriteUpdate {
  this: Table =>

  /**
    * Base name for table (used as name for permanent table and as
    * suffix for temp table)
    */
  def baseName: String = name

  /**
    * Populates temp table with new data
    */
  def populateTempTable(conn: Connection, params: Params, tempName: Name): Unit

  /**
    * Deletes stale data from permanent table
    */
  def deleteFromPermanentTable(conn: Connection, params: Params, tempName: Name): Unit

  /**
    * Skip update if true
    */
  def skipUpdates: Boolean = false

  override def update(conn: Connection, params: Params): Unit = {
    if(skipUpdates) return

    val dsl = DB.dsl(conn)

    // Set temporary table name
    val newData = sqlName(schema, generateTempTableName())

    // Drop temp table if exists
    dsl.dropTableIfExists(newData).execute()

    // Create temp table as same to original table
    dsl.execute(s"CREATE TABLE $newData (LIKE $qualifiedName)")

    try {
      // Populate temp table
      populateTempTable(conn, params, newData)

      executeUpdateTransaction(conn, params, newData)
    } finally {
      // Drop temp table again
      dsl.dropTableIfExists(newData).execute()
    }

  }

  protected def deleteUsingKeys(conn: Connection, params: Params, tempName: Name): Unit = {
    import org.jooq.impl.DSL._

    val rightQuery = primaryKeys.map { key =>
      field(s"$tempName.$key").eq(field(s"$qualifiedName.$key"))
    }.reduceLeft((cond, comp) => cond.and(comp))

    val renderedQuery = DB.dsl(conn).renderInlined(rightQuery)

    val delete =
      s"""
         |DELETE FROM $qualifiedName
         |USING $tempName
         |WHERE ($renderedQuery)
       """.stripMargin

    DB.dsl(conn).execute(delete)
  }

  protected def generateTempTableName(): String = {
    s"tmp_${UUID.randomUUID().toString.replace('-', '_')}"
  }

  protected def executeUpdateTransaction(conn: Connection, params: Params, newData: Name): Unit = {
    var exc: Exception = null

    for(i <- 0 until 5) {
      try {
        DB.transaction(conn) { conn =>
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
