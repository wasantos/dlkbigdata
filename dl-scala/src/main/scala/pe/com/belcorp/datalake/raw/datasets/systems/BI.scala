package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}

/**
  * Class representing the BI system
  */
final class BI(val params: Params) extends System {
  import System._

  override val name = "bi"
  override val glueSchemaSource: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaTarget: String = params.glueLandingDatabase.getOrElse("staging")
  override val redshiftSchema: String = "lan_analitico"

  override def interfaces: Seq[Interface] = Seq(
    interface("debelista", MERGE,
      keyColumns = Seq("pt_country", "codebelista"),
      partitionColumns = Seq("pt_country")
    ),

    interface("dpalancas", MERGE,
      keyColumns = Seq("pt_country", "codpalanca"),
      partitionColumns = Seq("pt_country")
    ),

     interface("findsociaemp", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codseccion"),
      partitionColumns = Seq("pt_country", "aniocampana")),

    interface("dcatalogovehiculo", MERGE,
      keyColumns = Seq("pt_country", "codcatalogo"),
      partitionColumns = Seq("pt_country")
    ),

    interface("fstaebecam", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dcomportamientorolling", MERGE,
      keyColumns = Seq("pt_country", "codcomportamiento"),
      partitionColumns = Seq("pt_country")
    ),

    interface("dstatus", MERGE,
      keyColumns = Seq("pt_country", "codstatus"),
      partitionColumns = Seq("pt_country")
    ),

    interface("fresultadopalancas", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista", "codpalanca"),
      partitionColumns = Seq("pt_country", "aniocampana")),

    interface("dpais", MERGE,
      keyColumns = Seq("codpais"))
  )
}
