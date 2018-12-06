package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.raw.datasets.interfaces.Registry
import pe.com.belcorp.datalake.raw.datasets.{InterfaceFunctional, SystemFunctional}
import pe.com.belcorp.datalake.utils.Params

/**
  * Class representing the SICC system
  */
final class SiccFunctional(val params: Params) extends SystemFunctional with Registry {

  override val name = "sicc"
  override val glueSchemaLanding: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaSource: String = params.glueStagingDatabase.getOrElse("work")
  override val glueSchemaTarget: String = params.glueFunctionalDatabase.getOrElse("functional")
  override val redshiftSchema: String = params.redshiftSchema.getOrElse("fnc_analitico")

  override def interfaces: Seq[InterfaceFunctional] = Seq(
    dwh_dcategoria,
    dwh_dmarca,
    dwh_dgeografiacampana,
    dwh_dapoyoproducto,
    dwh_dletsrangoscomision,
    dwh_dnrofactura,
    dwh_dstatusfacturacion,
    dwh_dtiempoactividadzona,
    dwh_fnumpedcam,
    dwh_dmatrizcampana,
    dwh_fvtaproebecam,
    dwh_fstaebecam,
    dwh_debelista,
    ctr_cierre_sicc
  )

}
