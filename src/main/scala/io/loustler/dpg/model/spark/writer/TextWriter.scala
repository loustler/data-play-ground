package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import org.apache.spark.sql.DataFrame

final class TextWriter extends BaseWriter[TextWriter] {
  override def write(df: DataFrame, path: String): Unit = writer(df).text(path)

  /** compression codec to use when saving to file.
    *
    * This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate)
    *
    * Defaults to null.
    *
    * @param compression compression
    * @return
    */
  def compression(compression: CompressionType): TextWriter = option("compression", CompressionType.name(compression))

  /** defines the line separator that should be used for writing.
    *
    * Defaults to \n
    *
    * @param sep line separator
    * @return
    */
  def lineSep(sep: String): TextWriter = option("lineSep", sep)
}
