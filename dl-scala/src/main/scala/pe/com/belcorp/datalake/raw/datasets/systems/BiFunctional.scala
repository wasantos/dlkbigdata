package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.raw.datasets.interfaces.Registry
import pe.com.belcorp.datalake.raw.datasets.{InterfaceFunctional, SystemFunctional}
import pe.com.belcorp.datalake.utils.Params

/**
  * Class representing the BI system
  */
final class BiFunctional(val params: Params) extends SystemFunctional with Registry {

  override val name = "bi"
  override val glueSchemaLanding: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaSource: String = params.glueStagingDatabase.getOrElse("work")
  override val glueSchemaTarget: String = params.glueFunctionalDatabase.getOrElse("functional")
  override val redshiftSchema: String = params.redshiftSchema.getOrElse("fnc_analitico")

  override def interfaces: Seq[InterfaceFunctional] = Seq(
    dwh_dstatus,
    dwh_dpais,
    dwh_dcomportamientorolling,
    dwh_fstaebecam,
    dwh_debelista,
    dwh_dgeografiacampana,
    dwh_dmatrizcampana
  )

}
