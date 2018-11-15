package pe.com.belcorp.datalake.utils

import org.apache.spark.sql.SparkSession

object Tables {

  /**
    * Get glue/hive table list using 'show tables'
    *
    * @param spark session correctly configured
    */
  def listTables(spark: SparkSession, database: String): Array[String] = {
    import spark.implicits._
    spark.catalog.setCurrentDatabase(database)

    spark.sql("show tables").map(x => x.getString(0) + '.' + x.getString(1)).collect()
  }

}
