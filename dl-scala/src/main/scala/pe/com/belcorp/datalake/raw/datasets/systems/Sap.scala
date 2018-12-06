package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}

/**
  * Class representing the SAP system
  */
final class Sap(val params: Params) extends System {
  import System._

  override val name = "sap"
  override val glueSchemaSource: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaTarget: String = params.glueStagingDatabase.getOrElse("work")
  override val redshiftSchema: String = "lan_analitico"

  override def interfaces: Seq[Interface] = Seq(
    interface("dproducto", MERGE,
      keyColumns = Seq("codsap")
    )
  )
}
