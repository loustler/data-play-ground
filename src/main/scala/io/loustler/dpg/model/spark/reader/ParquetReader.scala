package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ DataFrame, SparkSession }

final class ParquetReader extends ReadFromFile[ParquetReader] {

  override def read(
    spark: SparkSession,
    path: String*
  ): DataFrame = reader(spark).parquet(path: _*)

  /** sets whether we should merge schemas collected from all Parquet part-files. This will override spark.sql.parquet.mergeSchema.
    *
    * Defaults is the value specified in spark.sql.parquet.mergeSchema(false)
    *
    * @param merge merge schema
    * @return
    */
  def mergeSchema(merge: Boolean): ParquetReader = option("mergeSchema", merge.toString)
}
