package pe.com.belcorp.datalake.tests.utils

import org.scalatest.FunSuite
import pe.com.belcorp.datalake.raw.datasets.impl.{DefaultInterfaceImpl, DefaultRedshiftWritersImpl}
import pe.com.belcorp.datalake.raw.datasets.{Interface, System}
import pe.com.belcorp.datalake.utils.Params

class SanityTest extends FunSuite {
  test("system/interface datasets API") {
    val data = System.fetchAll(new Params(baseParams))
      .flatMap(_.interfaces)
      .map { i =>
        val keys = s""""${i.keyColumns.mkString(",")}""""
        s"""${i.system.name},${i.name},${strategyFor(i)},${keys}"""
      }.mkString("\n")

    println("system,interface,strategy,keys")
    println(data)
  }

  private val baseParams = Seq(
    "--country", "pe", "--year", "2018",
    "--month", "05", "--day", "05", "--secs", "1234"
  )

  private def strategyFor(i: Interface): String = {
    i match {
      case impl: DefaultInterfaceImpl =>
        if(impl.redshiftWriter == System.MERGE) "MERGE"
        else if(impl.redshiftWriter == System.OVERWRITE) "OVERWRITE"
        else if(impl.redshiftWriter == System.APPEND) "APPEND"
        else "UNKNOWN"
      case _ => "UNKNOWN"
    }
  }
}
