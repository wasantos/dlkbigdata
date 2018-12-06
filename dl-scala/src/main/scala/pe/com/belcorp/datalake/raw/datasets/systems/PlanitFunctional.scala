package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.raw.datasets.interfaces.Registry
import pe.com.belcorp.datalake.raw.datasets.{InterfaceFunctional, SystemFunctional}
import pe.com.belcorp.datalake.utils.Params

/**
  * Class representing the PLANIT system
  */
final class PlanitFunctional(val params: Params) extends SystemFunctional with Registry {
  override val name = "planit"
  override val glueSchemaLanding: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaSource: String = params.glueStagingDatabase.getOrElse("work")
  override val glueSchemaTarget: String = params.glueFunctionalDatabase.getOrElse("functional")
  override val redshiftSchema: String = params.redshiftSchema.getOrElse("fnc_analitico")

  override def interfaces: Seq[InterfaceFunctional] = Seq(
    dwh_dtipooferta,
    dwh_fvtaprocammes,
    ctr_cierre_planit,
    dwh_dmatrizcampana,
    dwh_fvtaproebecam
  )
}
