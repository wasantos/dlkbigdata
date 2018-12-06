package pe.com.belcorp.datalake.utils

import org.apache.spark.sql.DataFrame

/**
  * Describes how is a dataset to be partitioned
  * @param country country from dataset
  * @param year ingestion year
  * @param month ingestion month
  * @param day ingestion day
  * @param secs ingestion seconds
  */
case class PartitioningSpecification(country: String = null,
                                     year: String = null,
                                     month: String = null,
                                     day: String = null,
                                     secs: String = null) {

  import PartitioningSpecification._

  /**
    * Mappings for the partitions
    * @return a list of tuples with each partition column and it's value
    */
  def mappings: Seq[(String, String)] = Seq(
    (COUNTRY_COLUMN, country),
    (YEAR_COLUMN, year),
    (MONTH_COLUMN, month),
    (DAY_COLUMN, day),
    (SECS_COLUMN, secs)
  )

  /**
    * Mappings for the partitions considering daily updates
    * @return a list of tuples with each partition column and it's value
    */
  def dailyMappings: Seq[(String, String)] = Seq(
    (COUNTRY_COLUMN, country),
    (YEAR_COLUMN, year),
    (MONTH_COLUMN, month),
    (DAY_COLUMN, day)
  )

  /**
    * Columns for the partitions
    * @return a list of partition columns
    */
  def columns: Seq[String] = COLUMNS

  /**
    * Applies filtering based on partitions specified by instance to a [[DataFrame]]
    * @param df the DataFrame to filter
    */
  def apply(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.col

    mappings
      .filterNot(_._2 == null) // Ignore null values
      .foldLeft(df)((df, tup) => df.where(col(tup._1) === tup._2)) // Apply conditions
  }

  /**
    * Returns a string key to be used for identification in monitoring tasks
    */
  def toKeyString: String = {
    val values = Seq(
      (COUNTRY_COLUMN, country),
      (YEAR_COLUMN, year),
      (MONTH_COLUMN, month),
      (DAY_COLUMN, day),
      (SECS_COLUMN, secs)
    )

    values.map(t => s"${t._1}=${t._2}").mkString("/")
  }
}

object PartitioningSpecification {
  val COUNTRY_COLUMN: String = "pt_country"
  val YEAR_COLUMN: String = "pt_year"
  val MONTH_COLUMN: String = "pt_month"
  val DAY_COLUMN: String = "pt_day"
  val SECS_COLUMN: String = "pt_secs"

  val COLUMNS: Seq[String] = Seq(
    COUNTRY_COLUMN,
    YEAR_COLUMN,
    MONTH_COLUMN,
    DAY_COLUMN,
    SECS_COLUMN
  )
}