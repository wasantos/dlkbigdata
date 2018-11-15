package pe.com.belcorp.datalake.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession(appName: String = "DatalakeBelcorp"): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("hive.optimize.s3.query", true)
      .config("hive.exec.parallel", true)
      .config("hive.exec.dynamic.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
  }
}
