package pe.com.belcorp.datalake.analytics.addons

import pe.com.belcorp.datalake.analytics.OldTable
import pe.com.belcorp.datalake.utils.Params

trait ByCountryAndCampaign {
  this: OldTable =>

  /**
    * Campaigns to handle
    */
  val campaigns: Seq[String]

  override def updateParameters(params: Params): Map[String, Any] =
    Map(
      "country" -> params.country(),
      "campaign" -> campaigns
    )

  override val partitions: Seq[String] = Seq("CODPAIS", "ANIOCAMPANA")
}
