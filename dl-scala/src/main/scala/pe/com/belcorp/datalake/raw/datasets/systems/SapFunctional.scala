package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.raw.datasets.{InterfaceFunctional, SystemFunctional}
import pe.com.belcorp.datalake.utils.Params

/**
  * Class representing the SAP system
  */
final class SapFunctional(val params: Params) extends SystemFunctional {

  override val name = "sap"
  override val glueSchemaLanding: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaSource: String = params.glueStagingDatabase.getOrElse("work")
  override val glueSchemaTarget: String = params.glueFunctionalDatabase.getOrElse("functional")
  override val redshiftSchema: String = params.redshiftSchema.getOrElse("fnc_analitico")

  override def interfaces: Seq[InterfaceFunctional] = Seq(
    interfacefunc("sap_dproducto",
      Seq("sap_dproducto"),
      targetTable = "dwh_dproducto",
      redshiftCreateScriptPath =
        "pe/com/belcorp/datalake/resources/analytics/dwh/DProducto/create.sql"
    )
  )

}
