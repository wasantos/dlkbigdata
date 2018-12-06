package pe.com.belcorp.datalake.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession(appName: String = "DatalakeBelcorp"): SparkSession = {

    val spark = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.kryo.unsafe", "true")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.adaptive.enabled", "true")
      .config("hive.optimize.s3.query", "true")
      .config("hive.exec.parallel", "true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val checkpointPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"),
      "checkpoints")

    spark.sparkContext.setCheckpointDir(checkpointPath.toString)

    spark
  }

  def getStagingPath(paths: String*): String = {
    val initial = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
    paths.foldLeft(initial) { (path, segment) =>
      new Path(path, segment)
    }.toString
  }
}
