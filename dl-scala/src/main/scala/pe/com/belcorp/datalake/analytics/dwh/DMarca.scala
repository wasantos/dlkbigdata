package pe.com.belcorp.datalake.analytics.dwh

import java.sql.Connection

import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.analytics.addons.CreateFromResource
import pe.com.belcorp.datalake.utils.{DB, Params}

class DMarca(val schema: String, val sourceSchema: String) extends Table with CreateFromResource {
  override val createPath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DMarca/create.sql"
  override val name: String = "dwh_dmarca"

  override def update(conn: Connection, params: Params): Unit = {
    import org.jooq.impl.{DSL => d}

    val dsl = DB.dsl(conn)
    val mkTable = d.table(d.name(schema, name))

    // Skip if table already populated
    if(dsl.fetchExists(mkTable)) return

    val codMarca = d.field("codmarca", classOf[String])
    val desMarca = d.field("desmarca", classOf[String])

    dsl.insertInto(mkTable)
      .set(codMarca, "01").set(desMarca, "L'BEL").newRecord()
      .set(codMarca, "02").set(desMarca, "ESIKA").newRecord()
      .set(codMarca, "03").set(desMarca, "CYZONE").newRecord()
      .set(codMarca, "06").set(desMarca, "FINART").newRecord()
      .set(codMarca, "09").set(desMarca, "SKINEXPERT").newRecord()
      .set(codMarca, "14").set(desMarca, "BELCORP").newRecord()
      .set(codMarca, "99").set(desMarca, "GENERICA")
      .execute()
  }
}
