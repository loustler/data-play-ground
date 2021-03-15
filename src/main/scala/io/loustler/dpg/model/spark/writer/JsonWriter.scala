package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import org.apache.spark.sql.DataFrame

final class JsonWriter extends BaseWriter[JsonWriter] {

  override def write(df: DataFrame, path: String): Unit = writer(df).parquet(path)

  /** compression codec to use when saving to file.
    *
    * This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
    *
    * Defaults to null
    *
    * @param compression compression
    * @return
    */
  def compression(compression: CompressionType): JsonWriter = option("compression", CompressionType.name(compression))

  /** sets the string that indicates a date format.
    *
    * Custom date formats follow the formats at Datetime Patterns.
    *
    * This applies to date type.
    *
    * Defaults to yyyy-MM-dd
    *
    * @param format format
    * @return
    * @see https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    */
  def dateFormat(format: String): JsonWriter = option("dateFormat", format)

  /** sets the string that indicates a timestamp format.
    *
    * Custom date formats follow the formats at Datetime Patterns.
    *
    * This applies to timestamp type.
    *
    * Defaults to yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]
    *
    * @param format format
    * @return
    */
  def timestampFormat(format: String): JsonWriter = option("timestampFormat", format)

  /** specifies encoding (charset) of saved json files.
    *
    * If it is not set, the UTF-8 charset will be used.
    *
    * @param encoding encoding
    * @return
    */
  def encoding(encoding: String): JsonWriter = option("encoding", encoding)

  /** defines the line separator that should be used for writing.
    *
    * Defaults to \n
    *
    * @param sep line separator
    * @return
    */
  def lineSep(sep: String): JsonWriter = option("lineSep", sep)

  /** Whether to ignore null fields when generating JSON objects.
    *
    * Defaults to true
    *
    * @param ignore ignore null fields?
    * @return
    */
  def ignoreNullFields(ignore: Boolean): JsonWriter = option("ignoreNullFields", ignore.toString)
}
