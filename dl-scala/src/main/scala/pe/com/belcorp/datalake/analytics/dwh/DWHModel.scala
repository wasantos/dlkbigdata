package pe.com.belcorp.datalake.analytics.dwh

import pe.com.belcorp.datalake.analytics.{Model, Table}
import pe.com.belcorp.datalake.common.CampaignTracking
import pe.com.belcorp.datalake.utils.{DB, Params}

import scala.util.Random

class DWHModel(override val manager: DB, override val params: Params) extends Model {
  lazy val campaigns: Seq[String] =
    manager.transaction { conn => CampaignTracking.fetch(conn, params) }

  override val schema: String = "fnc_analitico"
  override val sourceSchema: String = "lan_analitico"

  override def tables: Seq[Table] =
    Random.shuffle(Seq(
      new DEbelista(schema, sourceSchema),
      new DProducto(schema, sourceSchema),
      new DGeografiaCampana(schema, sourceSchema, campaigns),
      new DApoyoProducto(schema, sourceSchema, campaigns),
      new DMatrizCampana(schema, sourceSchema, campaigns),
      new DLetsRangosComision(schema, sourceSchema),
      new DNroFactura(schema, sourceSchema, campaigns),
      new FStaEbeCam(schema, sourceSchema, campaigns),
      new FVtaProEbeCam(schema, sourceSchema, campaigns),
      new DStatusFacturacion(schema, sourceSchema, campaigns),
      new DTipoOferta(schema, sourceSchema),
      new DOrigenPedidoWeb(schema, sourceSchema),
      new FLogingResoPortal(schema, sourceSchema, campaigns),
      new FPedidoWebDetalle(schema, sourceSchema, campaigns),
      new FOfertaFinalConsultora(schema, sourceSchema, campaigns),
      new DComportamientoRolling(schema, sourceSchema),
      new DPais(schema, sourceSchema),
      new DMarca(schema, sourceSchema),
      new DCategoria(schema, sourceSchema),
      new DStatus(schema, sourceSchema),
      new DTiempoActividadZona(schema, sourceSchema, campaigns),
      new FNumPedCam(schema, sourceSchema, campaigns),
      new FVtaProCamMes(schema, sourceSchema, campaigns),
      new DCampCer(schema, sourceSchema),
      new DControl(schema, sourceSchema)
    ))
}

