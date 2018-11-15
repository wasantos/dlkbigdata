package pe.com.belcorp.datalake.raw.datasets.systems

import pe.com.belcorp.datalake.utils.Params
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.raw.datasets.interfaces.TStaEbeCam

/**
  * Class representing the SICC system
  */
final class Sicc(val params: Params) extends System {
  import System._

  override val name = "sicc"
  override val glueSchemaSource: String = params.glueLandingDatabase.getOrElse("landing")
  override val glueSchemaTarget: String = params.glueLandingDatabase.getOrElse("staging")
  override val redshiftSchema: String = "lan_analitico"

  override def interfaces: Seq[Interface] = Seq(
    interface("dcampcer", MERGE,
      keyColumns = Seq("pt_country", "aniocampana"),
      partitionColumns = Seq("pt_country", "aniocampana")
    ),

    interface("dgeografia", MERGE,
      keyColumns = Seq("codpais", "codterritorio"),
      partitionColumns = Seq("codpais")
    ),

    interface("debelista", MERGE,
      keyColumns = Seq("pt_country", "codebelista"),
      partitionColumns = Seq("pt_country")
    ),

    interface("debelistadatosadic", MERGE,
      keyColumns = Seq("pt_country", "codebelista"),
      partitionColumns = Seq("pt_country")
    ),

    interface("dmatrizcampana", MERGE,
      keyColumns = Seq("pt_country", "codcanalventa", "aniocampana", "codproducto", "codtipooferta", "codventa"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dgeografiacampana", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codterritorio"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dletsrangoscomision", MERGE,
      keyColumns = Seq("pt_country", "codprograma", "codnivel", "tipovalor", "codrango"),
      partitionColumns = Seq("pt_country")
    ),

    interface("dnrodocumento", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "nrodocumento"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fvtaproebecam", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codcanalventa", "codproducto", "codterritorio", "codebelista",
        "codtipooferta", "codtipodocumento", "nrodocumento", "codventa", "aniocampanaref", "fechaproceso"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fstaebecam", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fstaebeadic", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("tstaebecam", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codebelista"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dstatusf", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codregion", "codzona"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dtiempoactividadzona", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codzona", "codregion", "codactividad"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("fnumpedcam", MERGE,
      keyColumns = Seq("pt_country", "aniocampana"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    ),

    interface("dapoyoproducto", MERGE,
      keyColumns = Seq("pt_country", "aniocampana", "codproductoapoyado",
        "codventaapoyado", "codtipoofertaapoyado",
        "codproductoapoyador", "codventaapoyador",
        "codtipoofertaapoyador"),
      partitionColumns = Seq("pt_country", "aniocampana"),
      campaignColumn = "aniocampana"
    )
  )
}
