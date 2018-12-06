package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Params

/**
  * Class representing the DIGITAL system
  */
final class Digital(val params: Params) extends System {
  import System._

  override val name = "digital"
  override val glueSchemaSource: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaTarget: String = params.glueStagingDatabase.getOrElse("work")
  override val redshiftSchema: String = "lan_analitico"

  override def interfaces: Seq[Interface] = Seq(
    interface("flogingresoportal", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista", "fechahora"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fingresosconsultoraportal", MERGE,
      keyColumns = Seq("pt_country", "aniocampanaweb", "consultora"),
      partitionColumns = Seq("pt_country", "aniocampanaweb"),
      campaignColumn = "aniocampanaweb"
    ),

    interface("dorigenpedidoweb", MERGE,
      keyColumns = Seq("pt_country", "codorigenpedidoweb"),
      partitionColumns = Seq("pt_country")
    ),

    interface("fpedidowebdetalle", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista", "pedidoid", "pedidodetalleid"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fofertafinalconsultora", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista", "fecha", "codventa"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fcompdigcon", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista"),
      partitionColumns = Seq("pt_country", "aniocampana")
    )

  )
}
