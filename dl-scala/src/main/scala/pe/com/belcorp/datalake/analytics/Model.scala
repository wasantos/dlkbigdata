package pe.com.belcorp.datalake.analytics

import java.sql.Connection

import pe.com.belcorp.datalake.utils.{DB, Params}
import pe.com.belcorp.datalake.utils.Goodies.{logIt, timeIt}
import pe.com.belcorp.datalake.utils.monitoring.Task
import play.api.libs.json.Json

/**
  * Describes a full analytic model
  */
trait Model {
  /**
    * Schema (namespace) for the tables from the model.
    */
  val schema: String

  /**
    * Source schema for the tables from the model to be built upon.
    */
  val sourceSchema: String

  /**
    * Database entrypoint for managing tables
    */
  val manager: DB

  /**
    * Parameters for the model generation
    */
  val params: Params

  /**
    * Tables composing the model, in order in which should be created/updated
    */
  def tables: Seq[Table]

  /**
    * Create all needed tables
    */
  def create(): Unit = {
    manager.transaction { conn =>
      manageSchema(conn)
      tables.foreach(_.create(conn, params))
    }
  }

  /**
    * Update all tables for the model
    */
  def update(): Unit = {
    if (params.skipFailures()) {
      for (table <- tables) {
        try {
          manager.checkout { conn =>
            manageSchema(conn)
            runFor(table, conn)
          }
        } catch {
          case e: Exception =>
            System.err.println(s"Error updating ${table.name}")
            e.printStackTrace()
            System.err.println(s"Continuing execution...")
        }
      }
    } else {
      manager.transaction { conn =>
        manageSchema(conn)
        tables.foreach(runFor(_, conn))
      }
    }
  }

  private def runFor(table: Table, conn: Connection) = {
    logIt(s"Running ${table.name}")
    val ellapsed = timeIt { table.update(conn, params) }
    logIt(s"${table.name} took ${ellapsed.toLong} ms")

    Task.ifMonitoring { t =>
      t.success("FUNCTIONAL_TABLE_LOADED", Json.obj(
        "ellapsed_ms" -> ellapsed,
        "table" -> table.name
      ))
    }
  }

  private def manageSchema(conn: Connection) = {
    val dsl = DB.dsl(conn)

    dsl.createSchemaIfNotExists(schema).execute()
    dsl.execute(s"SET search_path TO $schema, '$$user', public")
  }
}
