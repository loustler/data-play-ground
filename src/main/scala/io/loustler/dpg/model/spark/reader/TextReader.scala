package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ DataFrame, SparkSession }

final class TextReader extends ReadFromFile[TextReader] {

  override def read(
    spark: SparkSession,
    path: String*
  ): DataFrame = reader(spark).text(path: _*)

  /** If true, read a file as a single row and not split by "\n".
    *
    * Defaults to false
    *
    * @param whole read file content as single row
    * @return
    */
  def wholetext(whole: Boolean): TextReader = option("wholetext", whole.toString)

  /** defines the line separator that should be used for parsing.
    *
    * Defaults covers all \r, \r\n and \n
    *
    * @param sep sep
    * @return
    */
  def lineSep(sep: String): TextReader = option("lineSep", sep)
}
