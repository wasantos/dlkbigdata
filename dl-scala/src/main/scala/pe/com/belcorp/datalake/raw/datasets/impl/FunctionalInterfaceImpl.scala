package pe.com.belcorp.datalake.raw.datasets.impl

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.raw.datasets.{Interface, InterfaceFunctional, SystemFunctional}
import pe.com.belcorp.datalake.utils.Goodies.logIt
import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.utils.SparkUtils.getStagingPath

/**
  * Default implementation for interfaces
  */
class FunctionalInterfaceImpl(
  override val system: SystemFunctional,
  override val name: String,
  override val sourceTables: Seq[String],
  override val targetTable: String,
  override val partitionColumns: Seq[String],
  override val keyColumns: Seq[String],
  override val campaignColumn: Option[String],
  override val glueSchemaLanding: String,
  override val glueSchemaSource: String,
  override val glueSchemaTarget: String,
  override val redshiftSchema: String,
  override val redshiftCreateScriptPath: String,
  override val redshiftPopulateOnce: Boolean) extends InterfaceFunctional {

  override def updateFunctionalParquet(spark: SparkSession, params: Params): Unit = {
    if (name == "dwh_dcategoria") {
      logIt("DWH_DCATEGORIA")
      updateDCategoria(spark, params)
    }

    if (name == "dwh_dmarca") {
      logIt("DWH_DMARCA")
      updateDMarca(spark, params)
    }

    if (name == "sicc_dcampcer") {
      logIt("SICC_DCAMPCER")
      updateDCampCer(spark, params)
    }

    if (name == "sicc_dgeografiacampana") {
      logIt("SICC_DGEOGRAFIACAMPANA")
      updateDGeografiaCampana(spark, params)
    }

    if (name == "sicc_dapoyoproducto") {
      logIt("SICC_DAPOYOPRODUCTO")
      updateDApoyoProducto(spark, params)
    }

    if (name == "sicc_dletsrangoscomision") {
      logIt("SICC_DLETSRANGOSCOMISION")
      updateDLetsRangosComision(spark, params)
    }

    if (name == "sicc_dnrodocumento") {
      logIt("SICC_DNRODOCUMENTO")
      updateDNroFactura(spark, params)
    }

    if (name == "sicc_dstatusf") {
      logIt("SICC_DSTATUSF")
      updateDStatusFacturacion(spark, params)
    }

    if (name == "sicc_dtiempoactividadzona") {
      logIt("SICC_DTIEMPOACTIVIDADZONA")
      updateDTiempoActividadZona(spark, params)
    }

    if (name == "sicc_fnumpedcam") {
      logIt("SICC_FNUMPEDCAM")
      updateFNumPedCam(spark, params)
    }

    if (name == "sicc_dmatrizcampana") {
      logIt("SICC_FMATRIZCAMPANA")
      updateDMatrizCampana(spark, params)
    }


    if (name == "sap_dproducto") {
      logIt("SAP_DPRODUCTO")
      updateDProducto(spark, params)
    }


    if (name == "bi_dstatus") {
      logIt("BI_DSTATUS")
      updateDStatus(spark, params)
    }

    if (name == "bi_dpais") {
      logIt("BI_DPAIS")
      updateDPais(spark, params)
    }

    if (name == "bi_dcomportamientorolling") {
      logIt("BI_DCOMPORTAMIENTOROLLING")
      updateDComportamientoRolling(spark, params)
    }


    if (name == "planit_dtipooferta") {
      logIt("PLANIT_DTIPOOFERTA")
      updateDTipoOferta(spark, params)
    }

    if (name == "planit_dcontrol") {
      logIt("PLANIT_DCONTROL")
      updateDControl(spark, params)
    }

    if (name == "planit_fvtaprocammes") {
      logIt("PLANIT_FVTAPROCAMMES")
      updateFVtaProCamMes(spark, params)
    }


    if (name == "digital_dorigenpedidoweb") {
      logIt("DIGITAL_DORIGENPEDIDOWEB")
      updateDOrigenPedidoWeb(spark, params)
    }

    if (name == "digital_flogingresoportal") {
      logIt("DIGITAL_FLOGINGRESOPORTAL")
      updateFLogingResoPortal(spark, params)
    }

    if (name == "digital_fofertafinalconsultora") {
      logIt("DIGITAL_FOFERTAFINALCONSULTORA")
      updateFOfertaFinalConsultora(spark, params)
    }

    if (name == "digital_fpedidowebdetalle") {
      logIt("DIGITAL_FPEDIDOWEBDETALLE")
      updateFPedidoWebDetalle(spark, params)
    }


    if (name == "sicc_fvtaproebecam") {
      logIt("SICC_FVTAPROEBECAM")
      updateFVtaProEbeCam(spark, params)
    }

    if (name == "sicc_fstaebecam") {
      logIt("SICC_FSTAEBECAM")
      updateFStaEbeCam(spark, params)
    }

    if (name == "sicc_debelista") {
      logIt("SICC_DEBELISTA")
      updateDEbelista(spark, params)
    }

  }


  //********************************************************************************************************************
  //DWH - carga de table DCATEGORIA en FUNCTIONAL
  private def updateDCategoria(spark: SparkSession, params: Params): Unit = {
    if(spark.catalog.tableExists(qualifiedTgtTableName)) {
      // no necesita inserir a tabla multiplas vezes
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codcategoria" -> StringType,
      "descategoria" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    import spark.implicits._
    val df_source = Seq(
      ("MQ", "MAQUILLAJE"),
      ("FG", "FRAGANCIA"),
      ("CP", "CUIDADO PERSONAL"),
      ("TC", "TRATAMIENTO CORPORAL"),
      ("TF", "TRATAMIENTO FACIAL")).toDF("codcategoria", "descategoria")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 1)
  }


  //********************************************************************************************************************
  //DWH - carga de table DMARCA en FUNCTIONAL
  private def updateDMarca(spark: SparkSession, params: Params): Unit = {
    if(spark.catalog.tableExists(qualifiedTgtTableName)) {
      // no necesita inserir a tabla multiplas vezes
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codmarca" -> StringType,
      "desmarca" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    import spark.implicits._
    val df_source = Seq(
      ("01", "L'BEL"),
      ("02", "ESIKA"),
      ("03", "CYZONE"),
      ("06", "FINART"),
      ("09", "SKINEXPERT"),
      ("14", "BELCORP"),
      ("99", "GENERICA")).toDF("codmarca", "desmarca")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 1)
  }


  //********************************************************************************************************************
  //SICC - carga de table DCAMPCER en FUNCTIONAL
  private def updateDCampCer(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "aniocampana" -> StringType,
      "fecha" -> TimestampType,
      "codpais" -> StringType,
      "fecha_proceso" -> TimestampType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.sicc_dcampcer")
        .withColumn("fecha_fmt",
          concat(from_unixtime(unix_timestamp(col("fecha"), "yyyyMMdd"), "yyyy-MM-dd"),lit(" 00:00:00")).cast(TimestampType).alias("fecha_fmt"))
        .withColumn("fecha_hoy", current_timestamp())
        .selectExpr("aniocampana", "fecha_fmt as fecha", "pt_country as codpais", "fecha_hoy as fecha_proceso")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 1)//crea los archivos en S3
  }


  //********************************************************************************************************************
  //SICC - carga de table DGEOGRAFIACAMPANA en FUNCTIONAL
  private def updateDGeografiaCampana(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codgerenteregional" -> StringType,
      "codgerentezona" -> StringType,
      "codlider" -> StringType,
      "codregion" -> StringType,
      "codseccion" -> StringType,
      "codterritorio" -> StringType,
      "codzona" -> StringType,
      "desregion" -> StringType,
      "deszona" -> StringType,
      "codpais" -> StringType,
      "despais" -> StringType,
      "desdepartamento" -> StringType,
      "desciudad" -> StringType,
      "desdistrito" -> StringType,
      "desnivelsocia" -> StringType,
      "desrendimientosocia" -> StringType,
      "deslider" -> StringType,
      "desgerenteregional" -> StringType,
      "desgerentezona" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //tratamiento especifico de geografia
    val df_geocamp = spark.table(s"$glueSchemaSource.sicc_dgeografiacampana")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("aniocampana", "codgerenteregional", "codgerentezona", "codlider", "codregion", "codseccion",
        "codzona", "desregion", "deszona", "pt_country", "codterritorio")

    val df_geo = spark.table(s"$glueSchemaSource.sicc_dgeografia")
      .where(col("pt_country") === lit(params.country()))
      .select("desdepartamento", "desciudad", "desdistrito", "deslider", "desgerenteregional", "desgerentezona",
        "pt_country", "codterritorio")

    val df_geo2 = df_geocamp.join(df_geo, Seq("pt_country", "codterritorio"), "left")

    val df_findsocia = spark.table(s"$glueSchemaSource.bi_findsociaemp")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("desnivel", "rendimientoetapa", "pt_country", "aniocampana", "trim(codseccion) as codseccion")

    val df_geo2socia = df_geo2
      .withColumn("codseccion", concat(trim(col("codzona")), trim(col("codseccion"))))
      .join(df_findsocia, Seq("pt_country", "aniocampana", "codseccion"), "left")

    val df_dpais = spark.table(s"$glueSchemaSource.bi_dpais")
      .where(col("pt_country") === lit(params.country()))
      .select("despais", "codpais")

    val df_source = df_geo2socia.join(df_dpais, col("codpais") === col("pt_country"), "left")
        .selectExpr("codgerenteregional", "codgerentezona", "codlider", "codregion", "codseccion",
          "codterritorio", "codzona", "desregion", "deszona", "pt_country as codpais", "despais", "desdepartamento", "desciudad",
          "desdistrito", "desnivel AS desnivelsocia", "rendimientoetapa AS desrendimientosocia",
          "deslider", "desgerenteregional", "desgerentezona", "aniocampana", "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 1, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table DMATRIZCAMPANA en FUNCTIONAL
  private def updateDMatrizCampana(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codcanalventa" -> StringType,
      "codcatalogo" -> StringType,
      "codestrategia" -> StringType,
      "codtipooferta" -> StringType,
      "codventa" -> StringType,
      "descatalogo" -> StringType,
      "destipooferta" -> StringType,
      "nropagina" -> IntegerType,
      "numoferta" -> IntegerType,
      "precionormalmn" -> DecimalType,
      "preciooferta" -> DecimalType,
      "preciovtapropuestomn" -> DecimalType,
      "codtipocatalogo" -> StringType,
      "desargventa" -> StringType,
      "desexposicion" -> StringType,
      "desladopag" -> StringType,
      "destipocatalogo" -> StringType,
      "desubicacioncatalogo" -> StringType,
      "fotomodelo" -> StringType,
      "fotoproducto" -> StringType,
      "nropaginas" -> IntegerType,
      "paginacatalogo" -> IntegerType,
      "desobservaciones" -> StringType,
      "vehiculoventa" -> StringType,
      "codpais" -> StringType,
      "codsap" -> StringType,
      "codtipomedioventa" -> StringType,
      "demandaanormalplan" -> DecimalType,
      "desestrategia" -> StringType,
      "destipodiagramacion" -> StringType,
      "factorcuadre" -> StringType,
      "factorrepeticion" -> StringType,
      "flagdiscover" -> StringType,
      "flagestadisticable" -> StringType,
      "flagproductosebe" -> StringType,
      "indcuadre" -> StringType,
      "indpadre" -> StringType,
      "precionormaldolplan" -> DecimalType,
      "precionormalmnplan" -> DecimalType,
      "precioofertadolplan" -> DecimalType,
      "precioofertamnplan" -> DecimalType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)


    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //tratamiento especifico de matrizcampana
    val df_sicc_mc = spark.table(s"$glueSchemaSource.sicc_dmatrizcampana")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("codcanalventa", "codcatalogo", "codestrategia", "codventa", "descatalogo", "destipooferta",
        "nropagina", "numoferta", "precionormalmn", "preciooferta", "preciovtapropuesto", "codtipomedioventa",
        "desestrategia", "factorcuadre", "factorrepeticion", "flagestadisticable", "flagproductosebe", "indcuadre",
        "indpadre", "aniocampana", "codtipooferta", "codproducto", "pt_country")

    val df_plan_mc = spark.table(s"$glueSchemaSource.planit_dmatrizcampana")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("codtipocatalogo", "desargventa", "desexposicion", "desladopag", "destipocatalogo",
        "desubicacioncatalogo", "fotomodelo", "fotoproducto", "nropaginas", "paginacatalogo", "desobservaciones",
        "demandaanormalnplan", "destipodiagramacion", "flagdiscover", "precionormaldolplan", "precionormalmnplan",
        "precioofertadolplan", "precioofertamnplan", "aniocampana", "codtipooferta", "codproducto", "pt_country")

    val df_mc = df_sicc_mc.join(df_plan_mc, Seq("aniocampana", "codtipooferta", "codproducto", "pt_country"), "left")

    val df_catvehiculo = spark.table(s"$glueSchemaSource.bi_dcatalogovehiculo")
      .selectExpr("vehiculoventa", "codcatalogo", "pt_country")

    val df_source = df_mc.join(df_catvehiculo, Seq("codcatalogo", "pt_country"), "left")
      .selectExpr("aniocampana", "codcanalventa", "codcatalogo", "codestrategia", "codtipooferta", "codventa",
        "descatalogo", "destipooferta", "cast(nropagina as int)", "cast(numoferta as int)",
        "cast(precionormalmn as decimal(15,5))", "cast(preciooferta as decimal(15,5))",
        "cast(preciovtapropuesto as decimal(15,5)) as preciovtapropuestomn",
        "codtipocatalogo", "desargventa", "desexposicion", "desladopag", "destipocatalogo", "desubicacioncatalogo",
        "coalesce(fotomodelo,'') as fotomodelo", "coalesce(fotoproducto,'') as fotoproducto",
        "cast(nropaginas as int)", "cast(paginacatalogo as int)", "desobservaciones", "vehiculoventa",
        "codproducto as codsap", "codtipomedioventa",
        "cast(demandaanormalnplan as decimal(15,5)) AS demandaanormalplan", "desestrategia",
        "destipodiagramacion", "factorcuadre", "factorrepeticion", "flagdiscover", "flagestadisticable",
        "flagproductosebe", "indcuadre", "indpadre", "cast(precionormaldolplan as decimal(15,5)) as precionormaldolplan",
        "cast(precionormalmnplan as decimal(15,5)) as precionormalmnplan",
        "cast(precioofertadolplan as decimal(15,5)) as precioofertadolplan",
        "cast(precioofertamnplan as decimal(15,5)) as precioofertamnplan", "pt_country", "pt_country as codpais")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 1, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table FVTAPROEBECAM en FUNCTIONAL
  private def updateFVtaProEbeCam(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codcanalventa" -> StringType,
      "codebelista" -> StringType,
      "codterritorio" -> StringType,
      "codtipodocumento" -> StringType,
      "codtipooferta" -> StringType,
      "codventa" -> StringType,
      "descuento" -> DecimalType,
      "oportunidadahorromn" -> DecimalType,
      "nrofactura" -> StringType,
      "fechaproceso" -> TimestampType,
      "realanulmnneto" -> DecimalType,
      "realdevmnneto" -> DecimalType,
      "realuuanuladas" -> IntegerType,
      "realuudevueltas" -> IntegerType,
      "realuufaltantes" -> IntegerType,
      "realuuvendidas" -> IntegerType,
      "realvtamnfactura" -> DecimalType,
      "realvtamnfaltneto" -> DecimalType,
      "realvtamnneto" -> DecimalType,
      "realanulmncatalogo" -> DecimalType,
      "realdevmncatalogo" -> DecimalType,
      "realvtamncatalogo" -> DecimalType,
      "realvtamnfaltcatalogo" -> DecimalType,
      "costoreposicionmn" -> DecimalType,
      "realtcpromedio" -> DecimalType,
      "esttcpromedio" -> DecimalType,
      "canalingreso" -> StringType,
      "codpais" -> StringType,
      "codsap" -> StringType,
      "codpalancapersonalizacion" -> StringType,
      "despalancapersonalizacion" -> StringType,
      "aniocampanaref" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //tratamiento especifico de matrizcampana
    val df_ebecam = spark.table(s"$glueSchemaSource.sicc_fvtaproebecam")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("codcanalventa", "codebelista", "codterritorio", "codtipodocumento", "codtipooferta",
        "codventa", "descuento", "oportunidadahorro", "nrodocumento", "fechaproceso", "realanulmnneto", "realdevmnneto",
        "realuuanuladas", "realuudevueltas", "realuufaltantes", "realuuvendidas", "realvtamnfactura", "realvtamnfaltneto",
        "realvtamnneto", "realuuanuladas", "realuudevueltas", "realuuvendidas", "realuufaltantes", "canalingreso",
        "pt_country", "codproducto", "codigopalanca", "aniocampanaref", "aniocampana")

    val df_plan_cpc = spark.table(s"$glueSchemaSource.planit_dcostoproductocampana")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("costoreposicionmn", "aniocampana", "codproducto", "pt_country")

    val df_ebecam_costo = df_ebecam.join(df_plan_cpc, Seq("aniocampana", "codproducto", "pt_country"), "left")


    val df_numpedcam = spark.table(s"$glueSchemaSource.planit_fnumpedcam")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("realtc", "esttc", "aniocampana", "pt_country")

    val df_ebecam_costo_npcam = df_ebecam_costo.join(df_numpedcam, Seq("aniocampana", "pt_country"), "left")


    val df_origenpweb = spark.table(s"$glueSchemaSource.digital_dorigenpedidoweb")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("desorigenpedidoweb", "codorigenpedidoweb as CODIGOPALANCA", "pt_country")

    val df_ebecam_costo_origen = df_ebecam_costo_npcam.join(df_origenpweb, Seq("codigopalanca", "pt_country"), "left")
      .withColumn("aniocampanaori",
        when(col("aniocampanaref").isNull, col("aniocampana"))
          .when(trim(col("aniocampanaref")) === "", col("aniocampana"))
          .otherwise(col("aniocampanaref")))

    val df_sicc_mc = spark.table(s"$glueSchemaSource.sicc_dmatrizcampana")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("preciooferta", "aniocampana as aniocampanaori", "codtipooferta", "codproducto", "codventa", "pt_country")

    val df_source = df_ebecam_costo_origen.join(df_sicc_mc, Seq("aniocampanaori", "codtipooferta", "codproducto", "codventa", "pt_country"), "left")
      .selectExpr("aniocampana", "codcanalventa", "codebelista", "codterritorio", "codtipodocumento", "codtipooferta",
        "codventa", "cast(descuento as decimal(15,5)) as descuento", "cast(oportunidadahorro as decimal(15,5)) as oportunidadahorromn",
        "nrodocumento as nrofactura",
        "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fechaproceso, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fechaproceso",
        "cast(realanulmnneto as decimal(15,5)) as realanulmnneto", "cast(realdevmnneto as decimal(15,5)) as realdevmnneto",
        "cast(realuuanuladas as int) as realuuanuladas", "cast(realuudevueltas as int) as realuudevueltas",
        "cast(realuufaltantes as int) as realuufaltantes", "cast(realuuvendidas as int) as realuuvendidas",
        "cast(realvtamnfactura as decimal(15,5)) as realvtamnfactura", "cast(realvtamnfaltneto as decimal(15,5)) as realvtamnfaltneto",
        "cast(realvtamnneto as decimal(15,5)) as realvtamnneto",
        "cast(realuuanuladas as decimal(15,5)) * cast(preciooferta as decimal(15,5)) as realanulmncatalogo", //pprealuuanuladas",
        "cast(realuudevueltas as decimal(15,5)) * cast(preciooferta as decimal(15,5)) as realdevmncatalogo",  //pprealuudevueltas",
        "cast(realuuvendidas as decimal(15,5)) * cast(preciooferta as decimal(15,5)) as realvtamncatalogo",   //pprealuuvendidas",
        "cast(realuufaltantes as decimal(15,5)) * cast(preciooferta as decimal(15,5)) as realvtamnfaltcatalogo",  //pprealuufaltantes",
        "cast(costoreposicionmn as decimal(15,5)) as costoreposicionmn", "cast(realtc as decimal(15,5)) as realtcpromedio",
        "cast(esttc as decimal(15,5)) as esttcpromedio", "canalingreso", "pt_country as codpais", "codproducto as codsap",
        "codigopalanca as codpalancapersonalizacion", "desorigenpedidoweb as despalancapersonalizacion",
        "case when aniocampanaref is null then aniocampana " +
          "when trim(aniocampanaref) = '' then aniocampana " +
          "else aniocampanaref " +
          "end as aniocampanaref",
        "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 8, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table FSTAEBECAM en FUNCTIONAL
  private def updateFStaEbeCam(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codcanalventa" -> StringType,
      "codebelista" -> StringType,
      "codstatus" -> IntegerType,
      "codterritorio" -> StringType,
      "flagpasopedido" -> IntegerType,
      "realnroordenes" -> IntegerType,
      "flagactiva" -> IntegerType,
      "flagpasopedidocuidadopersonal" -> IntegerType,
      "flagpasopedidomaquillaje" -> IntegerType,
      "flagpasopedidotratamientocorporal" -> IntegerType,
      "flagpasopedidotratamientofacial" -> IntegerType,
      "flagpedidoanulado" -> IntegerType,
      "flagpasopedidofragancias" -> IntegerType,
      "codpais" -> StringType,
      "codigofacturainternet" -> StringType,
      "codcanalorigen" -> StringType,
      "flagmultimarca" -> IntegerType,
      "constancia" -> StringType,
      "frecuenciacompra" -> IntegerType,
      "codcomportamientorolling" -> IntegerType,
      "descripcionrolling" -> StringType,
      "flagpasopedidoweb" -> IntegerType,
      "nrologueos" -> IntegerType,
      "flagipunicozona" -> IntegerType,
      "flagexpuestaodd" -> IntegerType,
      "flagexpuestaof" -> IntegerType,
      "flagexpuestafdc" -> IntegerType,
      "flagexpuestasr" -> IntegerType,
      "flagcompraopt" -> IntegerType,
      "flagcompraodd" -> IntegerType,
      "flagcompraof" -> IntegerType,
      "flagcomprafdc" -> IntegerType,
      "flagcomprasr" -> IntegerType,
      "flagexpuestaopt" -> IntegerType,
      "flagdigital" -> IntegerType,
      "flagofertadigital" -> IntegerType,
      "flagrevistadigitalsuscripcion" -> IntegerType,
      "flagexperienciaganamas" -> IntegerType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //tratamiento especifico de fstaebecam
    val df_sicc_ebecam = spark.table(s"$glueSchemaSource.sicc_fstaebecam")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("codcanalventa", "codterritorio", "flagpasopedido", "realnroordenes", "flagdigital",
        "codstatus as codstatus_eb", "codebelista", "aniocampana", "pt_country")

    val df_bi_ebecam = spark.table(s"$glueSchemaSource.bi_fstaebecam")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("constancia", "frecuenciacompra", "codcomportamientorollingrep", "descomportamientorolling",
        "codebelista", "aniocampana", "pt_country")

    val df_ebecam = df_sicc_ebecam.join(df_bi_ebecam, Seq("codebelista", "aniocampana", "pt_country"), "left")


    val df_ebeadic = spark.table(s"$glueSchemaSource.sicc_fstaebeadic")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("codcanalorigen", "codebelista", "aniocampana", "pt_country")

    val df_ebe = df_ebecam.join(df_ebeadic, Seq("codebelista", "aniocampana", "pt_country"), "left")


    val df_digcon = spark.table(s"$glueSchemaSource.digital_fcompdigcon")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("flagipunicozona", "estadorevistadigitalsuscripcion", "codebelista", "aniocampana", "pt_country")

    val df_ebedig = df_ebe.join(df_digcon, Seq("codebelista", "aniocampana", "pt_country"), "left")


    val df_ingreso = spark.table(s"$glueSchemaSource.digital_fingresosconsultoraportal")
      .where(col("pt_country") === lit(params.country()) && col("aniocampanaweb").isin(campaigns:_*))
      .selectExpr("ingresostotales", "consultora as codebelista", "aniocampanaweb as aniocampana", "pt_country")

    val df_ebedig_ing = df_ebedig.join(df_ingreso, Seq("codebelista", "aniocampana", "pt_country"), "left")


    //****************************************************************************************************************** agg_flags_palancas
    val df_proebecam = spark.table(s"$glueSchemaSource.sicc_fvtaproebecam")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("trim(codproducto) as codproducto", "codtipodocumento", "realuuvendidas", "realuufaltantes",
        "aniocampanaref", "codebelista", "aniocampana", "pt_country")

    val df_palancas = spark.table(s"$glueSchemaSource.bi_fresultadopalancas")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("expuestafdc", "expuestaodd", "expuestaof", "expuestaopt", "expuestasr",
        "codebelista", "aniocampana", "pt_country", "codpalanca AS codigopalanca")

    val df_agg_flags_palancas = df_palancas
      .groupBy("codebelista", "aniocampana", "pt_country")
      .agg(
        max(when(col("codigopalanca") === lit("FDC"), lit(1)).otherwise(lit(0))).as("flagcomprafdc"),
        max(when(col("codigopalanca") === lit("ODD"), lit(1)).otherwise(lit(0))).as("flagcompraodd"),
        max(when(col("codigopalanca") === lit("OF"), lit(1)).otherwise(lit(0))).as("flagcompraof"),
        max(when(col("codigopalanca") === lit("OPT"), lit(1)).otherwise(lit(0))).as("flagcompraopt"),
        max(when(col("codigopalanca") === lit("SR"), lit(1)).otherwise(lit(0))).as("flagcomprasr"),
        max(when(col("codigopalanca").isin("FDC", "ODD", "OF", "OPT", "SR"), lit(1))
        .otherwise(lit(0))).as("flagofertadigital"),
        max(when(col("expuestafdc") === lit("1"), lit(1)).otherwise(lit(0))).as("flagexpuestafdc"),
        max(when(col("expuestaodd") === lit("1"), lit(1)).otherwise(lit(0))).as("flagexpuestaodd"),
        max(when(col("expuestaof") === lit("1"), lit(1)).otherwise(lit(0))).as("flagexpuestaof"),
        max(when(col("expuestaopt") === lit("1"), lit(1)).otherwise(lit(0))).as("flagexpuestaopt"),
        max(when(col("expuestasr") === lit("1"), lit(1)).otherwise(lit(0))).as("flagexpuestasr"))
      .selectExpr("flagcomprafdc", "flagcompraodd", "flagcompraof",
        "flagcompraopt", "flagcomprasr", "flagofertadigital", "flagexpuestafdc", "flagexpuestaodd", "flagexpuestaof",
        "flagexpuestaopt", "flagexpuestasr", "codebelista", "aniocampana", "pt_country")

    val df_ebe_pal = df_ebedig_ing.join(df_agg_flags_palancas, Seq("codebelista", "aniocampana", "pt_country"), "left")

    //****************************************************************************************************************** agg_flags_factura
    val df_nrodoc = spark.table(s"$glueSchemaSource.sicc_dnrodocumento")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .select("codebelista", "aniocampana", "pt_country", "flagordenanulado", "canalingreso")

    val df_agg_flags_factura = df_nrodoc.groupBy("codebelista", "aniocampana", "pt_country")
        .agg(col("codebelista"), col("aniocampana"), col("pt_country"),
          max("flagordenanulado").alias("flagpedidoanulado"),
          max("canalingreso").alias("canalingreso"))
        .withColumn("flagpasopedidoweb", when(col("canalingreso").isin("WEB", "WMX", "APP", "APM", "APW"), lit(1))
          .otherwise(lit(0)))
      .select("flagpedidoanulado", "canalingreso", "flagpasopedidoweb", "codebelista", "aniocampana", "pt_country")

    val df_ebeb_fac = df_ebe_pal.join(df_agg_flags_factura, Seq("codebelista", "aniocampana", "pt_country"), "left")


    //****************************************************************************************************************** agg_flags_indicadores
    //df_proebecam
    val df_proebecam2 = df_proebecam.where(col("codtipodocumento") === lit("N") &&
        (col("realuuvendidas") + col("realuufaltantes") > lit(0)) &&
        col("aniocampanaref").isNull)

    val df_producto = spark.table(s"$glueSchemaSource.sap_dproducto")
      .selectExpr("trim(codsap) as codsap", "trim(codmarca) as codmarca", "trim(codclase) as codclase")

    val df_proebecam_prod = df_proebecam2.join(df_producto, col("codsap") === col("codproducto"), "left")
//      .groupBy("codebelista", "aniocampana", "pt_country")
//      .agg(col("codmarca"), col("codclase"))

    val df_agg_flags_indicadores = df_proebecam_prod
      .withColumn("flagmultimarca", when(col("codmarca").isin("01", "02", "03", "06"), lit(1)).otherwise(lit(0)))
      .withColumn("flagpasopedidofragancia", when(col("codclase") === lit("11"), lit(1)).otherwise(lit(0)))
      .withColumn("flagpasopedidocuidadopersonal", when(col("codclase") === lit("13"), lit(1)).otherwise(lit(0)))
      .withColumn("flagpasopedidomaquillaje", when(col("codclase") === lit("12"), lit(1)).otherwise(lit(0)))
      .withColumn("flagpasopedidotratamientocorporal", when(col("codclase") === lit("15"), lit(1)).otherwise(lit(0)))
      .withColumn("flagpasopedidotratamientofacial", when(col("codclase") === lit("14"), lit(1)).otherwise(lit(0)))
      .selectExpr("flagmultimarca", "flagpasopedidofragancia", "flagpasopedidocuidadopersonal",
        "flagpasopedidomaquillaje", "flagpasopedidotratamientocorporal", "flagpasopedidotratamientofacial",
        "codebelista", "aniocampana", "pt_country")

    val df_ebe_ind = df_ebeb_fac.join(df_agg_flags_indicadores, Seq("codebelista", "aniocampana", "pt_country"), "left")


    //****************************************************************************************************************** agg_bi_dstatus
    val df_bidstatus = spark.table(s"$glueSchemaSource.bi_dstatus")
      .where(col("pt_country") === lit(params.country()) && col("codstatus_sicc").isNotNull)
      .selectExpr("pt_country as pt_country_bi", "codstatus_sicc", "codstatus", "desstatus", "codstatuscorp", "desstatuscorp")
      .groupBy("pt_country_bi", "codstatus_sicc")
      .agg(min("codstatus").alias("codstatus"), min("desstatus").alias("desstatus"),
        min("codstatuscorp").alias("codstatuscorp"), min("desstatuscorp").alias("desstatuscorp"))

    val df_ebe_stat = df_ebe_ind.join(df_bidstatus, (col("codstatus_eb") === col("codstatus_sicc")) &&
      (col("pt_country") === col("pt_country_bi")), "left")

    val df_source = df_ebe_stat.where(!coalesce(col("desstatuscorp"), lit("")).isin("Retiradas", "Registradas"))
        .withColumn("flagrevistadigitalsuscripcion",
          when(col("estadorevistadigitalsuscripcion") === lit("1"), lit(1)).otherwise(lit(0)))
        .withColumn("flagexperienciaganamas",
          when(col("estadorevistadigitalsuscripcion").isin("1", "2"), lit(1)).otherwise(lit(0)))
        .withColumn("flagactiva",
          when(col("codstatuscorp").isin(1, 2, 3), lit(1)).otherwise(lit(0)))
        .selectExpr("codcanalventa", "codebelista", "cast(codstatus as int) as codstatus" ,
          "codterritorio", "cast(flagpasopedido as int) as flagpasopedido", "cast(realnroordenes as int) as realnroordenes",
          "cast(flagactiva as int)", "cast(flagpasopedidocuidadopersonal as int)", "cast(flagpasopedidomaquillaje as int)",
          "cast(flagpasopedidotratamientocorporal as int)", "cast(flagpasopedidotratamientofacial as int)",
          "cast(flagpedidoanulado as int)", "cast(flagpasopedidofragancia as int) as flagpasopedidofragancias",
          "pt_country as codpais",
          "canalingreso as codigofacturainternet", "codcanalorigen", "cast(flagmultimarca as int)", "constancia",
          "cast(frecuenciacompra as int) as frecuenciacompra",
          "cast(codcomportamientorollingrep as int) as codcomportamientorolling", "descomportamientorolling as descripcionrolling",
          "cast(flagpasopedidoweb as int)", "cast(coalesce(ingresostotales, 0) as int) as nrologueos",
          "cast(flagipunicozona as int) as flagipunicozona", "cast(flagexpuestaodd as int)", "cast(flagexpuestaof as int)",
          "cast(flagexpuestafdc as int)", "cast(flagexpuestasr as int)", "cast(flagcompraopt as int)", "cast(flagcompraodd as int)",
          "cast(flagcompraof as int)", "cast(flagcomprafdc as int)", "cast(flagcomprasr as int)", "cast(flagexpuestaopt as int)",
          "cast(flagdigital as int) as flagdigital", "cast(flagofertadigital as int)", "cast(flagrevistadigitalsuscripcion as int)",
          "cast(flagexperienciaganamas as int)", "aniocampana", "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table DEBELISTA en FUNCTIONAL
  private def updateDEbelista(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "aniocampanaingreso" -> StringType,
      "aniocampanaprimerpedidoweb" -> StringType,
      "aniocampanaultimopedido" -> StringType,
      "codebelista" -> StringType,
      "desestadocivil" -> StringType,
      "desnse" -> StringType,
      "desapematerno" -> StringType,
      "desapepaterno" -> StringType,
      "desnombre" -> StringType,
      "fechanacimiento" -> TimestampType,
      "flaggerentezona" -> IntegerType,
      "aniocampanaprimerpedido" -> StringType,
      "codpais" -> StringType,
      "desapenom" -> StringType,
      "deslider" -> StringType,
      "telefonomovil" -> StringType,
      "flagdigital" -> IntegerType,
      "tipodocidentidad" -> StringType,
      "docidentidad" -> StringType,
      "flagcorreovalidado" -> IntegerType,
      "desdireccion" -> StringType,
      "correoelectronico" -> StringType,
      "edad" -> IntegerType,
      "flagcelular" -> IntegerType,
      "aniocampanaregistro" -> StringType,
      "fecharegistro" -> TimestampType,
      "aniocampanacorreovalidado" -> StringType,
      "pt_country" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //tratamiento especifico de fstaebecam
    val df_deb = spark.table(s"$glueSchemaSource.sicc_debelista")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("aniocampanaingreso", "aniocampanaultimopedido", "desestadocivil", "desnse", "desapematerno",
        "desapepaterno", "desnombre", "fechanacimiento", "flaggerentezona", "desapenom", "deslider", "flagdigital",
        "tipodocidentidad", "docidentidad", "desdireccion", "aniocampanaregistro", "fecharegistro", "codebelista", "pt_country")

    val df_deb_adic = spark.table(s"$glueSchemaSource.sicc_debelistadatosadic")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("telefonomovil", "correoelectronico", "codebelista", "pt_country")

    val db_deb2 = df_deb.join(df_deb_adic, Seq("codebelista", "pt_country"), "left")


    val db_deb_bi = spark.table(s"$glueSchemaSource.bi_debelista")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("aniocampanaprimerpedido", "codebelista", "pt_country")

    val db_deb3 = db_deb2.join(db_deb_bi, Seq("codebelista", "pt_country"), "left")   //DEBELISTAS SICC, ADIC e BI

    // LEITURA DA TABELA FUNCTIONAL - FINALIDADE DE NAO RECALCULAR OS CAMPOS NOVAMENTE (ANIOCAMPANAPRIMERPEDIDOWEB)
    val db_deb_dwh = spark.table(s"$glueSchemaTarget.dwh_debelista")
      .alias('dwh)
      .join(db_deb3.alias('db),
        col("db.codebelista") === col("dwh.codebelista") &&
          col("db.pt_country") === col("dwh.codpais"), "inner")
      .where(col("db.pt_country") === lit(params.country()))
      .selectExpr(
        "db.pt_country", "db.codebelista",
        "dwh.aniocampanaprimerpedidoweb as aniocampanaold")

    val db_deb4_old = db_deb3.join(db_deb_dwh, Seq("codebelista", "pt_country"), "inner")
      .withColumn("aniocampanaprimerpedidoweb", expr("aniocampanaold"))
      .drop("aniocampanaold")

    val db_deb4_new = db_deb3.join(db_deb_dwh, Seq("codebelista", "pt_country"), "left_anti")

    //VERIFICACAO DO PRIMEIRO PEDIDO WEB DA EBELISTA
    val df_primerpedido = spark.table(s"$glueSchemaSource.sicc_dnrodocumento")
      .join(db_deb4_new, Seq("codebelista", "pt_country"))
      .where(col("canalingreso").isin("WEB", "WMK"))
      .selectExpr("aniocampana", "codebelista", "pt_country")

    val df_agg_primerpedido = df_primerpedido
      .groupBy("codebelista", "pt_country")
      .agg(min("aniocampana").as("aniocampanaprimerpedidoweb"))

    val db_deb_primerped = db_deb3
      .join(df_agg_primerpedido, Seq("codebelista", "pt_country"), "inner")
      .unionByName(db_deb4_old)

    //VERIFICACAO DA FLAG DE CORREO VALIDADO DA EBELISTA
    val df_correo = spark.table(s"$glueSchemaSource.digital_flogingresoportal")
      .where(col("pt_country") === lit(params.country()))
      .select(
        col("codebelista"), col("pt_country"),
        col("correovalidado"), col("aniocampana"),
        row_number().over(
          Window.partitionBy(col("codebelista"), col("pt_country"))
            .orderBy(col("correovalidado").desc_nulls_last, col("aniocampana").asc)
        ).as("posvalidado"))

    val df_agg_flagcorreo = df_correo
      .where("posvalidado = 1")
      .select(
        col("codebelista"), col("pt_country"),
        when(col("correovalidado") =!= lit("1"), lit(0))
          .otherwise(lit(1)).as("flagcorreovalidado"),
        when(col("correovalidado") =!= lit("1"), col("aniocampana"))
          .otherwise(lit(null)).as("aniocampanacorreovalidado"))

    val db_deb_final = db_deb_primerped.join(df_agg_flagcorreo, Seq("codebelista", "pt_country"), "left")
      .withColumn("fecha_nac",
        concat(from_unixtime(unix_timestamp(col("fechanacimiento"), "yyyyMMdd"), "yyyy-MM-dd"),lit(" 00:00:00")).cast(TimestampType))
      .withColumn("fecha_hoy", current_timestamp())
      .withColumn("edad", datediff(col("fecha_hoy"), col("fecha_nac")) / 365)

    val df_source = db_deb_final.selectExpr("aniocampanaingreso", "aniocampanaprimerpedidoweb",
      "aniocampanaultimopedido", "codebelista", "desestadocivil", "desnse", "desapematerno", "desapepaterno", "desnombre",
      "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fechanacimiento, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fechanacimiento",
      "cast(flaggerentezona as int)", "aniocampanaprimerpedido", "pt_country as codpais", "desapenom", "deslider", "telefonomovil",
      "cast(flagdigital as int)", "tipodocidentidad", "docidentidad", "cast(flagcorreovalidado as int)", "desdireccion",
      "correoelectronico",
      "cast(edad as int)",
      "cast(coalesce(telefonomovil,'') != '' as int) as flagcelular", "aniocampanaregistro",
      "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fecharegistro, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fecharegistro",
      "aniocampanacorreovalidado", "pt_country")

    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 2, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table DAPOYOPRODUCTO en FUNCTIONAL
  private def updateDApoyoProducto(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codpais" -> StringType,
      "codventaapoyador" -> StringType,
      "codtipoofertaapoyador" -> StringType,
      "codsapapoyador" -> StringType,
      "codcanalventaapoyador" -> StringType,
      "codventaapoyado" -> StringType,
      "codtipoofertaapoyado" -> StringType,
      "codsapapoyado" -> StringType,
      "codcanalventaapoyado" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.sicc_dapoyoproducto")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr(
        "pt_country as codpais", "aniocampana", "codventaapoyador",
        "codtipoofertaapoyador", "codproductoapoyador AS codsapapoyador",
        "codcanalventaapoyador", "codventaapoyado", "codtipoofertaapoyado",
        "codproductoapoyado AS codsapapoyado", "codcanalventaapoyado", "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table DLETSRANGOSCOMISION en FUNCTIONAL
  private def updateDLetsRangosComision(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "codpais" -> StringType,
      "codprograma" -> StringType,
      "codnivel" -> StringType,
      "tipovalor" -> StringType,
      "codrango" -> IntegerType,
      "porcomision" -> DecimalType,
      "montoini" -> DecimalType,
      "montofin" -> DecimalType,
      "pt_country" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.sicc_dletsrangoscomision")
      .selectExpr("pt_country as codpais", "codprograma", "codnivel", "tipovalor", "cast(codrango as int) as codrango",
        "cast(porccomision as decimal(15,5)) as porcomision", "cast(montoini as decimal(15,5)) as montoini",
        "cast(montofin as decimal(15,5)) as montofin", "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 1)
  }

  //********************************************************************************************************************
  //SICC - carga de table DNROFACTURA en FUNCTIONAL
  private def updateDNroFactura(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codacceso" -> StringType,
      "codcanalventa" -> StringType,
      "codebelista" -> StringType,
      "codterritorio" -> StringType,
      "codtipodocumento" -> StringType,
      "flagordenanulado" -> IntegerType,
      "flagprol" -> IntegerType,
      "realvtamnfactura" -> DecimalType,
      "saldobanco" -> DecimalType,
      "canalingreso" -> StringType,
      "fechaemisionfactura" -> TimestampType,
      "nrofactura" -> StringType,
      "codpais" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.sicc_dnrodocumento")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("aniocampana", "codacceso", "codcanalventa", "codebelista", "codterritorio", "codtipodocumento",
        "cast(flagordenanulado as int) as flagordenanulado", "cast(flagprol as int) as flagprol",
        "cast(realvtamnfactura as decimal(15,5)) as realvtamnfactura", "cast(saldobanco as decimal(15,5)) as saldobanco",
        "canalingreso",
        "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fechaemisiondocumento, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fechaemisionfactura",
        "nrodocumento as nrofactura", "pt_country as codpais", "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 1, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table DSTATUSFACTURACION en FUNCTIONAL
  private def updateDStatusFacturacion(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codregion" -> StringType,
      "codzona" -> StringType,
      "flagstatusfactsc" -> IntegerType,
      "fecha" -> TimestampType,
      "codpais" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.sicc_dstatusf")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .withColumn("fecha_hoy", current_timestamp())
      .selectExpr("aniocampana", "codregion", "codzona", "cast(flagstatusfactsc as int) as flagstatusfactsc",
        "fecha_hoy as fecha", "pt_country as codpais", "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 1, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table DTIEMPOACTIVIDADZONA en FUNCTIONAL
  private def updateDTiempoActividadZona(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codregion" -> StringType,
      "codzona" -> StringType,
      "codactividad" -> StringType,
      "desactividad" -> StringType,
      "fecha" -> TimestampType,
      "numdia" -> IntegerType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType,
      "codpais" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.sicc_dtiempoactividadzona")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("pt_country", "aniocampana", "codregion", "codzona", "codactividad", "desactividad",
        "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fecha, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fecha",
        "cast(numdia as int) as numdia", "pt_country AS codpais")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //DIGITAL - carga de table FLOGINGRESOPORTAL en FUNCTIONAL
  private def updateFLogingResoPortal(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "aniocampanaweb" -> StringType,
      "codebelista" -> StringType,
      "iporigen" -> StringType,
      "fechahora" -> TimestampType,
      "correovalidado" -> IntegerType,
      "codpais" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.digital_flogingresoportal")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*) &&
        trim(coalesce(col("codebelista"), lit(""))) =!= "")
      .selectExpr("aniocampana as aniocampanaweb", "codebelista", "iporigen",
        "cast(FROM_UNIXTIME(UNIX_TIMESTAMP(fechahora, 'yyyyMMdd HH:mm:SS'), 'yyyy-MM-dd HH:mm:SS') as timestamp) as fechahora",
        "cast(correovalidado as int) as correovalidado", "pt_country as codpais", "pt_country", "aniocampana")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //DIGITAL - carga de table FOFERTAFINALCONSULTORA en FUNCTIONAL
  private def updateFOfertaFinalConsultora(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "aniocampanaweb" -> StringType,
      "cantidad" -> IntegerType,
      "codebelista" -> StringType,
      "codventa" -> StringType,
      "tipoofertafinal" -> StringType,
      "fechacreacion" -> TimestampType,
      "realmngap" -> DecimalType,
      "tipoevento" -> StringType,
      "codpais" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.digital_fofertafinalconsultora")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*) &&
        col("codebelista") =!= lit("") && col("codventa") =!= lit(""))
      .selectExpr("aniocampana as aniocampanaweb", "cast(cantidad as int) as cantidad", "codebelista", "codventa",
        "tipoofertafinal",
        "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fecha, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fechacreacion",
        "cast(gap as decimal(15,5)) as realmngap",
        "tipoevento", "pt_country as codpais", "pt_country", "aniocampana")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //DIGITAL - carga de table FPEDIDOWEBDETALLE en FUNCTIONAL
  private def updateFPedidoWebDetalle(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "aniocampanaweb" -> StringType,
      "cantidad" -> IntegerType,
      "codebelista" -> StringType,
      "codventa" -> StringType,
      "fechacreacion" -> TimestampType,
      "flagofertaweb" -> IntegerType,
      "flagprocesado" -> IntegerType,
      "importetotal" -> DecimalType,
      "ordenpedidowd" -> IntegerType,
      "origenpedidoweb" -> IntegerType,
      "pedidodetalleid" -> IntegerType,
      "pedidoid" -> IntegerType,
      "codpais" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.digital_fpedidowebdetalle")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("aniocampana as aniocampanaweb", "cast(cantidad as int) as cantidad", "codebelista", "codventa",
        "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fechacreacion, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fechacreacion",
        "cast(flagofertaweb as int) as flagofertaweb",
        "cast(flagprocesado as int) as flagprocesado", "cast(importetotal as decimal(15,5)) as importetotal",
        "cast(ordenpedidowd as int) as ordenpedidowd", "cast(origenpedidoweb as int) as origenpedidoweb",
        "cast(pedidodetalleid as int) as pedidodetalleid", "cast(pedidoid as int) as pedidoid",
        "pt_country as codpais", "pt_country", "aniocampana")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //SICC - carga de table FNUMPEDCAM en FUNCTIONAL
  private def updateFNumPedCam(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codpais" -> StringType,
      "codcanalventa" -> StringType,
      "realtcpromedio" -> DecimalType,
      "pedidominmn" -> IntegerType,
      "realnroclientes" -> IntegerType,
      "realnropedidos" -> IntegerType,
      "realnroordencompra" -> IntegerType,
      "realvtamnnetototal" -> DecimalType,
      "realvtamncatalogototal" -> DecimalType,
      "realnroactivastotal" -> IntegerType,
      "estnropedidos" -> IntegerType,
      "esttcpromedio" -> DecimalType,
      "tccosto" -> DecimalType,
      "promordenesxped" -> DecimalType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_sicc_fnump = spark.table(s"$glueSchemaSource.sicc_fnumpedcam")
      .where(col("pt_country") === lit(params.country())
        && col("aniocampana").isin(campaigns:_*)).alias('sf)

    val df_planit_fnump = spark.table(s"$glueSchemaSource.planit_fnumpedcam")
      .where(col("pt_country") === lit(params.country())
        && col("aniocampana").isin(campaigns:_*)).alias('pf)

    val df_joined = df_sicc_fnump
      .join(df_planit_fnump,
        expr("sf.aniocampana = pf.aniocampana AND sf.pt_country = pf.pt_country"),
        "left")
      .selectExpr(
        "sf.*", "pf.estnropedidos", "pf.estnropedidos",
        "pf.esttc", "pf.promordenesxped", "pf.tccosto")

    val df_source = df_joined
      .selectExpr(
        "pt_country as codpais", "codcanalventa", "aniocampana",
        "cast(realtcpromedio as decimal(15,5)) as realtcpromedio",
        "cast(pedidominmn as int) as pedidominmn",
        "cast(realnroclientes as int) as realnroclientes",
        "cast(realnropedidos as int) as realnropedidos",
        "cast(realnroordencompra as int) as realnroordencompra",
        "cast(realvtamnnetototal as decimal(15,5)) as realvtamnnetototal",
        "cast(realvtamncatalogototal as decimal(15,5)) as realvtamncatalogototal",
        "cast(realnroactivastotal as int) as realnroactivastotal",
        "cast(estnropedidos as int) as estnropedidos",
        "cast(esttc as decimal(15,5)) as esttcpromedio",
        "cast(tccosto as decimal(15,5)) as tccosto",
        "cast(promordenesxped as decimal(15,5)) as promordenesxped",
        "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 4, campaigns = campaigns)
  }


  //********************************************************************************************************************
  //BI - carga de table DSTATUS en FUNCTIONAL
  private def updateDStatus(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "codpais" -> StringType,
      "codstatus" -> IntegerType,
      "desstatus" -> StringType,
      "codstatuscorp" -> IntegerType,
      "desstatuscorp" -> StringType,
      "codstatus_sicc" -> IntegerType,
      "desstatus_sicc" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.bi_dstatus")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("codpais", "cast(codstatus as int) as codstatus", "desstatus",
        "cast(codstatuscorp as int) as codstatuscorp", "desstatuscorp",
        "cast(codstatus_sicc as int) as codstatus_sicc", "desstatus_sicc")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 4)
  }

  //********************************************************************************************************************
  //BI - carga de table DPAIS en FUNCTIONAL
  private def updateDPais(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "codpais" -> StringType,
      "descripcionpais" -> StringType,
      "codpaisdm" -> StringType,
      "codcentro" -> StringType,
      "nrocampanas" -> IntegerType,
      "despais" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.bi_dpais")
      .where(col("pt_country") === lit(params.country()))
      .withColumn("descripcionpais", lit(""))
      .withColumn("codpaisdm", lit(""))
      .withColumn("codcentro", lit(""))
      .withColumn("nrocampanas", lit(0))
      .selectExpr("codpais", "descripcionpais", "codpaisdm", "codcentro", "nrocampanas", "despais")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 4)
  }


  //********************************************************************************************************************
  //BI - carga de table DCOMPORTAMIENTOROLLING en FUNCTIONAL
  private def updateDComportamientoRolling(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "codpais" -> StringType,
      "codcomportamiento" -> IntegerType,
      "desnivelcomportamiento" -> StringType,
      "descomportamiento" -> StringType,
      "desabrcomportamiento" -> StringType,
      "flagperiodo" -> IntegerType,
      "pt_country" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.bi_dcomportamientorolling")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("codpais", "cast(codcomportamiento as int) as codcomportamiento",
        "desnivelcomportamiento", "descomportamiento", "desabrcomportamiento",
        "cast(flagperiodo as smallint) as flagperiodo", "codpais as pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 4)
  }

  //********************************************************************************************************************
  //SAP - carga de table DPRODUCTO en FUNCTIONAL
  private def updateDProducto(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "codsap" -> StringType,
      "codproducto" -> StringType,
      "desproducto" -> StringType,
      "cuc" -> StringType,
      "descripcuc" -> StringType,
      "codunidadnegocio" -> StringType,
      "desunidadnegocio" -> StringType,
      "codmarca" -> StringType,
      "desmarca" -> StringType,
      "codcategoria" -> StringType,
      "descategoria" -> StringType,
      "codclase" -> StringType,
      "desclase" -> StringType,
      "desnegocio" -> StringType,
      "codigonegocio" -> StringType,
      "codsubcategoria" -> StringType,
      "codtipo" -> StringType,
      "destipo" -> StringType,
      "dessubtipo" -> StringType,
      "codsubtipo" -> StringType,
      "dessubcategoria" -> StringType,
      "destiposolo" -> StringType,
      "dessubtiposolo" -> StringType,
      "dessubcategoriasolo" -> StringType,
      "codlinea" -> StringType,
      "deslinea" -> StringType,
      "desproductosupergenerico" -> StringType,
      "codproductosupergenerico" -> StringType,
      "desproductogenerico" -> StringType,
      "codproductogenerico" -> StringType,
      "descrippeq" -> StringType,
      "peq" -> StringType,
      "codgrupoarticulo" -> StringType,
      "desgrupoarticulo" -> StringType,
      "mercadoacccosmetico" -> StringType,
      "nombregenbij" -> StringType,
      "adicional" -> StringType,
      "aplicacionlugaruso" -> StringType,
      "detallebrazo" -> StringType,
      "grosorbrazo" -> StringType,
      "negociacionuso" -> StringType,
      "beneficio" -> StringType,
      "detallescaja" -> StringType,
      "formacaja" -> StringType,
      "detallebot" -> StringType,
      "tipoestuche" -> StringType,
      "detalleproducto" -> StringType,
      "ropadetalle" -> StringType,
      "colortono" -> StringType,
      "colorluna" -> StringType,
      "teoriacolor" -> StringType,
      "insumoscomplent" -> StringType,
      "diseno" -> StringType,
      "detalletop" -> StringType,
      "origenaccesorios" -> StringType,
      "colormarco" -> StringType,
      "reportajemas" -> StringType,
      "caracteristicasbij" -> StringType,
      "acabadoaccesorios" -> StringType,
      "acabadoluna" -> StringType,
      "materialcaja" -> StringType,
      "familiafragancia" -> StringType,
      "datoslente" -> StringType,
      "tipofabricacion" -> StringType,
      "desmercado" -> StringType,
      "tipomaterialcalzado" -> StringType,
      "detallesuela" -> StringType,
      "presentacionenvase" -> StringType,
      "tipoplaneacion" -> StringType,
      "desposicionamiento" -> StringType,
      "presentacion" -> StringType,
      "presentacionforma" -> StringType,
      "tipoestampado" -> StringType,
      "temporada" -> StringType,
      "tamanomanga" -> StringType,
      "detalleespecifi" -> StringType,
      "colorcorrea" -> StringType,
      "tirasasa" -> StringType,
      "desestilo" -> StringType,
      "edadobjetivo" -> StringType,
      "tema" -> StringType,
      "tipolente" -> StringType,
      "tipopielcabello" -> StringType,
      "contneto" -> StringType,
      "um_contenido" -> StringType,
      "unidaddemedidabase" -> StringType,
      "versatilidad" -> StringType,
      "tallatamano" -> StringType,
      "codtipomaterial" -> StringType,
      "destipomaterial" -> StringType,
      "tipoproducto" -> StringType,
      "gama" -> StringType,
      "rotulosfila" -> StringType,
      "insumosmatprim" -> StringType,
      "tematico" -> StringType,
      "dial" -> StringType,
      "dialacabado" -> StringType,
      "tipologia" -> StringType,
      "sexo" -> StringType,
      "descaracteristicabulk" -> StringType,
      "nombrereloj" -> StringType,
      "subtipoprodcomplementos" -> StringType,
      "codjerq02" -> StringType,
      "descjerq02" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.sap_dproducto")
      .selectExpr("codsap", "codproducto", "desproducto", "cuc", "descripcuc", "codunidadnegocio", "desunidadnegocio",
        "codmarca", "desmarca", "codcategoria", "descategoria", "codclase", "desclase", "desnegocio", "codigonegocio",
        "codsubcategoria", "codtipo", "destipo", "dessubtipo", "codsubtipo", "dessubcategoria", "destiposolo", "dessubtiposolo",
        "dessubcategoriasolo", "codlinea", "deslinea", "desproductosupergenerico", "codproductosupergenerico",
        "desproductogenerico", "codproductogenerico", "descrippeq", "peq", "codgrupoarticulo", "desgrupoarticulo",
        "mercadoacccosmetico", "nombregenbij", "adicional", "aplicacionlugaruso", "detallebrazo", "grosorbrazo",
        "negociacionuso", "beneficio", "detallescaja", "formacaja", "detallebot", "tipoestuche", "detalleproducto",
        "ropadetalle", "colortono", "colorluna", "teoriacolor", "insumoscomplent", "diseno", "detalletop",
        "origenaccesorios", "colormarco", "reportajemas", "caracteristicasbij", "acabadoaccesorios", "acabadoluna",
        "materialcaja", "familiafragancia", "datoslente", "tipofabricacion", "desmercado", "tipomaterialcalzado",
        "detallesuela", "presentacionenvase", "tipoplaneacion", "desposicionamiento", "presentacion", "presentacionforma",
        "tipoestampado", "temporada", "tamanomanga", "detalleespecifi", "colorcorrea", "tirasasa", "desestilo", "edadobjetivo",
        "tema", "tipolente", "tipopielcabello", "contneto", "um_contenido", "unidaddemedidabase", "versatilidad", "tallatamano",
        "codtipomaterial", "destipomaterial", "tipoproducto", "gama", "rotulosfila", "insumosmatprim", "tematico", "dial",
        "dialacabado", "tipologia", "sexo", "descaracteristicabulk", "nombrereloj", "subtipoprodcomplementos", "codjerq02", "descjerq02" )

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 1)
  }


  //********************************************************************************************************************
  //PLANIT - carga de table DTIPOOFERTA en FUNCTIONAL
  private def updateDTipoOferta(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "codcanalventa" -> StringType,
      "codtipooferta" -> StringType,
      "abrtipooferta" -> StringType,
      "destipooferta" -> StringType,
      "codsubgrupoto1" -> StringType,
      "dessubgrupoto1" -> StringType,
      "codsubgrupoto2" -> StringType,
      "dessubgrupoto2" -> StringType,
      "destipoprofit" -> StringType,
      "codtipoprofit" -> StringType,
      "codpais" -> StringType,
      "pt_country" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.planit_dtipooferta")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("codcanalventa", "codtipooferta", "abrtipooferta", "destipooferta", "codsubgrupoto1",
        "dessubgrupoto1", "codsubgrupoto2", "dessubgrupoto2", "destipoprofit", "codtipoprofit", "pt_country as codpais",
        "pt_country")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 4)
  }

  //********************************************************************************************************************
  //PLANIT - carga de table DCONTROL en FUNCTIONAL
  private def updateDControl(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "aniocampana" -> StringType,
      "control" -> StringType,
      "codpais" -> StringType,
      "fecha_proceso" -> TimestampType,
      "realtcpromedio" -> DecimalType,
      "pt_country" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_ctrl = spark.table(s"$glueSchemaSource.planit_dcontrol")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("codcontrol", "pt_country")

    val df_agg_maxaniocampana = spark.table(s"$glueSchemaSource.planit_fnumpedcam")
      .where(col("pt_country") === lit(params.country()))
      .select("aniocampana", "pt_country")
      .groupBy("pt_country")
      .agg(max(col("aniocampana")).alias("maxaniocampana"))

    val df_agg_realtc_planit = spark.table(s"$glueSchemaSource.planit_fnumpedcam")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr(
        "realtc as realtcpromedio_planit",
        "row_number() OVER (ORDER BY aniocampana DESC NULLS LAST) as pos")
      .where("pos = 1")
      .drop("pos")
      .limit(1)

    val df_agg_realtc_sicc = spark.table(s"$glueSchemaSource.sicc_fnumpedcam")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr(
        "realtcpromedio as realtcpromedio_sicc",
        "row_number() OVER (ORDER BY aniocampana DESC NULLS LAST) as pos")
      .where("pos = 1")
      .drop("pos")
      .limit(1)

    val df_source = df_ctrl
      .join(df_agg_maxaniocampana, Seq("pt_country"), "inner")
      .crossJoin(df_agg_realtc_sicc)
      .crossJoin(df_agg_realtc_planit)
      .withColumn("fecha_hoy", current_timestamp())
      .selectExpr(
        "maxaniocampana as aniocampana", "codcontrol as control",
        "pt_country as codpais", "fecha_hoy as fecha_proceso", "pt_country",
        "CAST(CASE WHEN codcontrol = '2' THEN realtcpromedio_sicc " +
          "ELSE realtcpromedio_planit END AS DECIMAL(15,5)) AS realtcpromedio")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 4)
  }

  //********************************************************************************************************************
  //PLANIT - carga de table FVTAPROCAMMES en FUNCTIONAL
  private def updateFVtaProCamMes(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val campaigns = getCampaigns(spark, params)
    if (campaigns.isEmpty) {
      logIt("no campaigns found for ingestion, skipping write")
      return
    }

    val (schemaTyped, columns) = createStructType(
      "codversion" -> StringType,
      "desversion" -> StringType,
      "fechaversionamiento" -> TimestampType,
      "codcanalventa" -> StringType,
      "aniocontable" -> IntegerType,
      "mes" -> IntegerType,
      "codsap" -> StringType,
      "codtipooferta" -> StringType,
      "estuuvendidas" -> IntegerType,
      "estvtamnneto" -> DataTypes.createDecimalType(18,3),
      "estvtadolneto" -> DataTypes.createDecimalType(18,3),
      "estpup" -> DataTypes.createDecimalType(18,4),
      "estutilidad" -> DataTypes.createDecimalType(18,3),
      "precioofertamn" -> DataTypes.createDecimalType(18,3),
      "precioofertadol" -> DataTypes.createDecimalType(18,3),
      "precionormalmn" -> DataTypes.createDecimalType(18,3),
      "precionormaldol" -> DataTypes.createDecimalType(18,3),
      "demandaanormal" -> StringType,
      "codpais" -> StringType,
      "pt_country" -> StringType,
      "aniocampana" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    val df_source = spark.table(s"$glueSchemaSource.planit_fvtaprocammes")
      .where(col("pt_country") === lit(params.country()) && col("aniocampana").isin(campaigns:_*))
      .selectExpr("codversion", "desversion",
        "cast(concat(FROM_UNIXTIME(UNIX_TIMESTAMP(fechaversionamiento, 'yyyyMMdd'), 'yyyy-MM-dd'),' 00:00:00') as timestamp) as fechaversionamiento",
        "codcanalventa", "cast(aniocontable as int) as aniocontable", "cast(mes as int) as mes",
        "codproducto as codsap", "codtipooferta", "cast(estuuvendidas as int) as estuuvendidas",
        "cast(estvtamnneto as decimal(18,3)) as estvtamnneto", "cast(estvtadolneto as decimal(18,3)) as estvtadolneto",
        "cast(estpup as decimal(18,4)) as estpup", "cast(estutilidad as decimal(18,3)) as estutilidad",
        "cast(precioofertamn as decimal(18,3)) as precioofertamn", "cast(precioofertadol as decimal(18,3)) as precioofertadol",
        "cast(precionormalmn as decimal(18,3)) as precionormalmn", "cast(precionormaldol as decimal(18,3)) as precionormaldol",
        "demandaanormal", "pt_country as codpais", "pt_country", "aniocampana")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns,
      numFiles = 1, campaigns = campaigns)
  }

  //********************************************************************************************************************
  //DIGITAL - carga de table DORIGENPEDIDOWEB en FUNCTIONAL
  private def updateDOrigenPedidoWeb(spark: SparkSession, params: Params): Unit = {
    for (table <- this.qualifiedSrcTableNames) {
      if (!spark.catalog.tableExists(table)) {
        logIt(s"$table doesn't exists yet, skipping write")
        return
      }
    }

    val (schemaTyped, columns) = createStructType(
      "origenpedidoweb" -> IntegerType,
      "desorigenpedidoweb" -> StringType,
      "codpopup" -> IntegerType,
      "despopup" -> StringType,
      "flagpersonalizacion" -> IntegerType,
      "codzona" -> IntegerType,
      "deszona" -> StringType,
      "codmedio" -> IntegerType,
      "desmedio" -> StringType,
      "codseccion" -> IntegerType,
      "desseccion" -> StringType,
      "codpais" -> StringType)

    //verifica se a tabela destino existe
    createTableIfExists(spark, schemaTyped)

    //params.partitioningSpecification.apply(spark.table(qualifiedSrcTableNames))
    val df_source = spark.table(s"$glueSchemaSource.digital_dorigenpedidoweb")
      .where(col("pt_country") === lit(params.country()))
      .selectExpr("cast(codorigenpedidoweb as int) as origenpedidoweb", "desorigenpedidoweb",
        "cast(codpopup as int) as codpopup", "despopup",
        "cast(flagpersonalizacion as int) as flagpersonalizacion", "cast(codarea as int) as codzona",
        "desarea AS deszona", "cast(codmediotec as int) as codmedio", "desmediotec as desmedio",
        "cast(codespacio as int) as codseccion", "desespacio as desseccion", "pt_country AS codpais")

    //crea los archivos en S3
    insertData(spark, params, df_source, schemaTyped, columns, numFiles = 1)
  }

  private def reorderColumns(columns: Seq[String], partitions: Seq[String]): Seq[String] = {
    if (partitions.isEmpty) {
      return columns
    }

    val lowercase = partitions.map(_.toLowerCase)
    val filtered = columns.filterNot(lowercase.contains)

    filtered ++ lowercase
  }

  override def getCampaigns(spark: SparkSession, params: Params): Seq[String] = {
    val possibleDfs = for (tableName <- sourceTables) yield {
      val interface = getRawInterface(params, tableName)
      val fullName = qualifiedSrcTableName(tableName)
      if(spark.catalog.tableExists(fullName)) {
        interface.campaignColumn.map { key =>
          params.partitioningSpecification.apply(spark.table(fullName))
            .select(col(key).as("campaign"))
        }
      } else None
    }

    val finalDfs = possibleDfs.flatten

    if (finalDfs.isEmpty) return Seq.empty

    val finalDf = finalDfs.reduce((a, b) => a.union(b))
    val (campaignsList, wrongColumn) = CampaignTracking.selectCampaign(
      finalDf, params, "campaign", system.name, name)

    if (wrongColumn) {
      logIt(s"column '${campaignColumn.get}' does not exists in $qualifiedSrcTableNames, skip, fix registry settings")
    }

    campaignsList
  }

  private def getRawInterface(params: Params, tableName: String): Interface = {
    val Array(system, interface) = tableName.split("_")
    import pe.com.belcorp.datalake.raw.datasets.System
    System.fetch(system, params).interfaceMap(interface)
  }

  private def insertData(spark: SparkSession, params: Params,
                         df: DataFrame, schemaTyped: StructType,
                         columns: Seq[String], numFiles: Int = 4,
                         campaigns: Seq[String] = Seq.empty): Unit = {
    // checar columnas distintas
    val diff = columns.toSet.diff(df.columns.toSet)
    if(diff.nonEmpty) {
      throw new RuntimeException(s"Columns unnacounted for: $diff")
    }

    val nonDestructiveDF = if(this.keyColumns.nonEmpty) {
      // so necesita filtrar por particiones
      // cuando tiene que processar llaves
      val filteredDf = getOldData(spark, params, campaigns)

      filteredDf
        .join(df, this.keyColumns, "left_anti")
        .unionByName(df)
    } else df

    val tmpPath = getStagingPath("temp", qualifiedTgtTableName)

    nonDestructiveDF
      .coalesce(numFiles)
      .write
      .mode(SaveMode.ErrorIfExists)
      .parquet(tmpPath)
    
    var finalDf = spark.read.schema(schemaTyped).parquet(tmpPath)
      .selectExpr(reorderColumns(columns, this.partitionColumns): _*)

    if(this.partitionColumns.nonEmpty) {
      finalDf = finalDf.repartition(numFiles * 32, this.partitionColumns.map(col): _*)
    }

    finalDf
      .coalesce(numFiles)
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(qualifiedTgtTableName)
  }

  private val DecimalType = DataTypes.createDecimalType(15,5)

  private def getOldData(spark: SparkSession, params: Params, campaigns: Seq[String]) = {
    import scala.math.{min, pow}

    val rawDf = partitionColumns.foldLeft(spark.table(qualifiedTgtTableName)) {
      (df, partition) =>
        partition match {
          case "pt_country" => df.where(col("pt_country") === params.country())
          case "codpais" => df.where(col("codpais") === params.country())
          case "aniocampana" => df.where(col("aniocampana").isin(campaigns: _*))
          case "aniocampanaweb" => df.where(col("aniocampanaweb").isin(campaigns: _*))
        }
    }

    val numPartitions = min(256,
      pow(8, partitionColumns.size) * (campaigns.size + 1)).toInt

    rawDf.repartition(numPartitions, partitionColumns.map(col): _*)
  }

  private def createStructType(columns: (String, DataType)*): (StructType, Seq[String]) = {
    val columnNames = columns.map(_._1)
    val definitions = columns.map {
      case (name_, type_) => StructField(name_, type_)
    }
    (StructType(definitions), columnNames)
  }

  private def createTableIfExists(spark: SparkSession, schemaTyped: StructType): Unit = {
    //verifica se a tabela destino existe
    if(!spark.catalog.tableExists(qualifiedTgtTableName)) {
      //crea la tabla en glue
      val empty_df = spark
        .createDataFrame(spark.sparkContext.emptyRDD[Row], schemaTyped)

      if(this.partitionColumns.nonEmpty) {
        empty_df
          .write
          .partitionBy(this.partitionColumns: _*)
          .saveAsTable(qualifiedTgtTableName)
      } else {
        empty_df
          .write
          .saveAsTable(qualifiedTgtTableName)
      }
    }
  }
}
