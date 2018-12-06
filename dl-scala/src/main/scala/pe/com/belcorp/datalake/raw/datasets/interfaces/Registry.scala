package pe.com.belcorp.datalake.raw.datasets.interfaces

import pe.com.belcorp.datalake.raw.datasets.{InterfaceFunctional, SystemFunctional}

trait Registry {
  this: SystemFunctional =>

  type IF = InterfaceFunctional

  def dwh_dcategoria: IF = interfacefunc("dwh_dcategoria",
    targetTable = "dwh_dcategoria",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DCategoria/create.sql",
    redshiftPopulateOnce = true
  )

  def dwh_dmarca: IF = interfacefunc("dwh_dmarca",
    targetTable = "dwh_dmarca",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DMarca/create.sql",
    redshiftPopulateOnce = true
  )

  def dwh_dgeografiacampana: IF = interfacefunc("sicc_dgeografiacampana",
    Seq("sicc_dgeografia", "sicc_dgeografiacampana", "bi_findsociaemp", "bi_dpais"),
    targetTable = "dwh_dgeografiacampana",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq("codpais", "aniocampana", "codterritorio"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DGeografiaCampana/create.sql"
  )

  def dwh_dapoyoproducto: IF = interfacefunc("sicc_dapoyoproducto",
    Seq("sicc_dapoyoproducto"),
    targetTable = "dwh_dapoyoproducto",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq(
      "codpais","aniocampana","codventaapoyador","codtipoofertaapoyador",
      "codsapapoyador","codcanalventaapoyador","codventaapoyado","codtipoofertaapoyado",
      "codsapapoyado","codcanalventaapoyado"
    ),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DApoyoProducto/create.sql"
  )

  def dwh_dletsrangoscomision: IF = interfacefunc("sicc_dletsrangoscomision",
    Seq("sicc_dletsrangoscomision"),
    targetTable = "dwh_dletsrangoscomision",
    partitionColumns = Seq("codpais"),
    keyColumns = Seq("codpais", "codprograma"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DLetsRangosComision/create.sql"
  )

  def dwh_dnrofactura: IF = interfacefunc("sicc_dnrodocumento",
    Seq("sicc_dnrodocumento"),
    targetTable = "dwh_dnrofactura",
    partitionColumns = Seq("codpais", "aniocampana"),
    campaignColumn = "aniocampana",
    keyColumns = Seq("codpais", "aniocampana", "nrofactura", "codebelista"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DNroFactura/create.sql"
  )

  def dwh_dstatusfacturacion: IF = interfacefunc("sicc_dstatusf",
    Seq("sicc_dstatusf"),
    targetTable = "dwh_dstatusfacturacion",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq("codpais", "aniocampana", "codregion", "codzona"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DStatusFacturacion/create.sql"
  )

  def dwh_dtiempoactividadzona: IF = interfacefunc("sicc_dtiempoactividadzona",
    Seq("sicc_dtiempoactividadzona"),
    targetTable = "dwh_dtiempoactividadzona",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq("codpais", "aniocampana", "codregion", "codzona", "codactividad"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DTiempoActividadZona/create.sql"
  )

  def dwh_fnumpedcam: IF = interfacefunc("sicc_fnumpedcam",
    Seq("sicc_fnumpedcam", "planit_fnumpedcam"),
    targetTable = "dwh_fnumpedcam",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq("codpais", "aniocampana", "codcanalventa"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FNumPedCam/create.sql"
  )

  def dwh_dmatrizcampana: IF = interfacefunc("sicc_dmatrizcampana",
    Seq("sicc_dmatrizcampana", "planit_dmatrizcampana", "bi_dcatalogovehiculo"),
    targetTable = "dwh_dmatrizcampana",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq(
      "codpais", "aniocampana", "codcanalventa",
      "codtipooferta", "codsap", "codventa"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DMatrizCampana/create.sql"
  )

  def dwh_fvtaproebecam: IF = interfacefunc("sicc_fvtaproebecam",
    Seq("sicc_fvtaproebecam", "planit_dcostoproductocampana", "planit_fnumpedcam", "digital_dorigenpedidoweb", "sicc_dmatrizcampana"),
    targetTable = "dwh_fvtaproebecam",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq(
      "codpais", "aniocampana", "codcanalventa", "codebelista", "codventa",
      "codsap", "codtipooferta", "aniocampanaref", "nrofactura", "fechaproceso"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FVtaProEbeCam/create.sql"
  )

  def dwh_fstaebecam: IF = interfacefunc("sicc_fstaebecam",
    Seq("sicc_fstaebecam", "bi_fstaebecam", "sicc_fstaebeadic", "digital_fcompdigcon", "digital_fingresosconsultoraportal"),
    targetTable = "dwh_fstaebecam",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq("codpais", "aniocampana", "codcanalventa", "codebelista"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FStaEbeCam/create.sql"
  )

  def dwh_debelista: IF = interfacefunc("sicc_debelista",
    Seq("sicc_debelista", "sicc_debelistadatosadic", "bi_debelista", "sicc_dnrodocumento", "digital_flogingresoportal"),
    targetTable = "dwh_debelista",
    partitionColumns = Seq("codpais"),
    keyColumns = Seq("codpais", "codebelista"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DEbelista/create.sql"
  )

  def ctr_cierre_sicc: IF = interfacefunc("sicc_dcampcer",
    Seq("sicc_dcampcer"),
    targetTable = "ctr_cierre_sicc",
    partitionColumns = Seq("codpais"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DCampCer/create.sql"
  )

  def dwh_dtipooferta: IF = interfacefunc("planit_dtipooferta",
    Seq("planit_dtipooferta"),
    targetTable = "dwh_dtipooferta",
    partitionColumns = Seq("codpais"),
    keyColumns = Seq("codpais", "codcanalventa", "codtipooferta"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DTipoOferta/create.sql"
  )

  def dwh_fvtaprocammes: IF = interfacefunc("planit_fvtaprocammes",
    Seq("planit_fvtaprocammes"),
    targetTable = "dwh_fvtaprocammes",
    partitionColumns = Seq("codpais", "aniocampana"),
    keyColumns = Seq(
      "codpais", "aniocampana", "codcanalventa", "codsap", "codtipooferta"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FVtaProCamMes/create.sql"
  )

  def ctr_cierre_planit: IF = interfacefunc("planit_dcontrol",
    Seq("planit_dcontrol", "planit_fnumpedcam"),
    targetTable = "ctr_cierre_planit",
    partitionColumns = Seq("codpais"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DControl/create.sql"
  )

  def dwh_dorigenpedidoweb: IF = interfacefunc("digital_dorigenpedidoweb",
    Seq("digital_dorigenpedidoweb"),
    targetTable = "dwh_dorigenpedidoweb",
    partitionColumns = Seq("codpais"),
    keyColumns = Seq("codpais", "origenpedidoweb"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DOrigenPedidoWeb/create.sql"
  )

  def dwh_flogingresoportal: IF = interfacefunc("digital_flogingresoportal",
    Seq("digital_flogingresoportal"),
    targetTable = "dwh_flogingresoportal",
    partitionColumns = Seq("codpais", "aniocampanaweb"),
    keyColumns = Seq("codpais", "aniocampanaweb", "codebelista"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FLogingResoPortal/create.sql"
  )

  def dwh_fofertafinalconsultora: IF = interfacefunc("digital_fofertafinalconsultora",
    Seq("digital_fofertafinalconsultora"),
    targetTable = "dwh_fofertafinalconsultora",
    partitionColumns = Seq("codpais", "aniocampanaweb"),
    keyColumns = Seq("codpais", "aniocampanaweb", "codebelista", "codventa"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FOfertaFinalConsultora/create.sql"
  )

  def dwh_fpedidowebdetalle: IF = interfacefunc("digital_fpedidowebdetalle",
    Seq("digital_fpedidowebdetalle"),
    targetTable = "dwh_fpedidowebdetalle",
    partitionColumns = Seq("codpais", "aniocampanaweb"),
    keyColumns = Seq(
      "codpais", "aniocampanaweb", "codebelista", "pedidoid", "pedidodetalleid"),
    campaignColumn = "aniocampana",
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/FPedidoWebDetalle/create.sql"
  )

  def dwh_dstatus: IF = interfacefunc("bi_dstatus",
    Seq("bi_dstatus"),
    targetTable = "dwh_dstatus",
    partitionColumns = Seq("codpais"),
    keyColumns = Seq("codpais", "codstatus"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DStatus/create.sql"
  )

  def dwh_dpais: IF = interfacefunc("bi_dpais",
    Seq("bi_dpais"),
    targetTable = "dwh_dpais",
    keyColumns = Seq("codpais"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DPais/create.sql"
  )

  def dwh_dcomportamientorolling: IF = interfacefunc("bi_dcomportamientorolling",
    Seq("bi_dcomportamientorolling"),
    targetTable = "dwh_dcomportamientorolling",
    partitionColumns = Seq("codpais"),
    keyColumns = Seq("codpais", "codcomportamiento"),
    redshiftCreateScriptPath =
      "pe/com/belcorp/datalake/resources/analytics/dwh/DComportamientoRolling/create.sql"
  )
}
