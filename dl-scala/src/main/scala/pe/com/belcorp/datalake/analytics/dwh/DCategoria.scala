package pe.com.belcorp.datalake.analytics.dwh

import java.sql.Connection

import pe.com.belcorp.datalake.analytics.addons.CreateFromResource
import pe.com.belcorp.datalake.analytics.Table
import pe.com.belcorp.datalake.utils.{DB, Params}

class DCategoria(val schema: String, val sourceSchema: String) extends Table with CreateFromResource {
  override val createPath: String =
    "pe/com/belcorp/datalake/resources/analytics/dwh/DCategoria/create.sql"
  override val name: String = "dwh_dcategoria"
  override val primaryKeys: Seq[String] = Seq("CODCATEGORIA")

  override def update(conn: Connection, params: Params): Unit = {
    import org.jooq.impl.{DSL => d}

    val dsl = DB.dsl(conn)
    val catTable = d.table(d.name(schema, name))

    // Skip if table already populated
    if(dsl.fetchExists(catTable)) return

    val codCategoria = d.field("codcategoria", classOf[String])
    val desCategoria = d.field("descategoria", classOf[String])

    dsl.insertInto(catTable)
      .set(codCategoria, "MQ").set(desCategoria, "MAQUILLAJE").newRecord()
      .set(codCategoria, "FG").set(desCategoria, "FRAGANCIA").newRecord()
      .set(codCategoria, "CP").set(desCategoria, "CUIDADO PERSONAL").newRecord()
      .set(codCategoria, "TC").set(desCategoria, "TRATAMIENTO CORPORAL").newRecord()
      .set(codCategoria, "TF").set(desCategoria, "TRATAMIENTO FACIAL")
      .execute()
  }
}
