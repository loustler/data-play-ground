package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ DataFrame, SparkSession }

final class OrcReader extends ReadFromFile[OrcReader] {

  override def read(
    spark: SparkSession,
    path: String*
  ): DataFrame = reader(spark).orc(path: _*)

  /** sets whether we should merge schemas collected from all ORC part-files. This will override spark.sql.orc.mergeSchema.
    *
    * Defaults is the value specified in spark.sql.orc.mergeSchema(false)
    *
    * @param merge merge schema
    * @return
    */
  def mergeSchema(merge: Boolean): OrcReader = option("mergeSchema", merge.toString)
}
