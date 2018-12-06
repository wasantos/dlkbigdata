package pe.com.belcorp.datalake.analytics.addons

import pe.com.belcorp.datalake.analytics.OldTable
import pe.com.belcorp.datalake.utils.Params

trait ByCountry {
  this: OldTable =>

  override def updateParameters(params: Params): Map[String, Any] =
    Map("country" -> params.country())

  override val partitions: Seq[String] = Seq("CODPAIS")
}
