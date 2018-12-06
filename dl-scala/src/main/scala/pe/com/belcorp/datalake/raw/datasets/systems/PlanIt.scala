package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}

/**
  * Class representing the PLANIT system
  */
final class PlanIt(val params: Params) extends System {
  import System._

  override val name = "planit"
  override val glueSchemaSource: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaTarget: String = params.glueStagingDatabase.getOrElse("work")
  override val redshiftSchema: String = "lan_analitico"

  override def interfaces: Seq[Interface] = Seq(
    interface("dcontrol", MERGE,
      keyColumns = Seq("pt_country", "codcontrol"),
      partitionColumns = Seq("pt_country")
    ),

    interface("dtipooferta", MERGE,
      keyColumns = Seq("pt_country", "codtipooferta"),
      partitionColumns = Seq("pt_country")
    ),

    interface("dmatrizcampana", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codproducto",
        "codtipooferta", "codventa"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dcostoproductocampana", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codproducto"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fnumpedcam", MERGE,
      keyColumns = Seq("pt_country", "aniocampana"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fvtaprocammes", MERGE,
      keyColumns = Seq("pt_country","aniocampana", "aniocontable",
        "mes", "codproducto", "codtipooferta"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana")
  )
}
