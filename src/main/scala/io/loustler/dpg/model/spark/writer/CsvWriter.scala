package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import org.apache.spark.sql.DataFrame

final class CsvWriter extends BaseWriter[CsvWriter] {
  override def write(df: DataFrame, path: String): Unit = writer(df).csv(path)

  /** sets a single character as a separator for each field and value.
    *
    * Defaults to ,
    *
    * @param sep sep
    * @return
    */
  def sep(sep: String): CsvWriter = option("sep", sep)

  /** sets a single character used for escaping quoted values where the separator can be part of the value.
    *
    * If an empty string is set, it uses u0000 (null character).
    *
    * Defaults to "
    *
    * @param quote quote
    * @return
    */
  def quote(quote: String): CsvWriter = option("quote", quote)

  /** sets a single character used for escaping quotes inside an already quoted value.
    *
    * Defaults to \
    *
    * @param escape escape
    * @return
    */
  def escape(escape: String): CsvWriter = option("escape", escape)

  /** sets a single character used for escaping the escape for the quote character.
    *
    * The default value is escape character when escape and quote characters are different, \0 otherwise.
    *
    * Defaults to \0
    *
    * @param escape escape
    * @return
    */
  def charToEscapeQuoteEscaping(escape: String): CsvWriter = option("charToEscapeQuoteEscaping", escape)

  /** a flag indicating whether values containing quotes should always be enclosed in quotes.
    *
    * Default is to escape all values containing a quote character.
    *
    * Defaults to true
    *
    * @param escapeQuote escape quote
    * @return
    */
  def escapeQuotes(escapeQuote: Boolean): CsvWriter = option("escapeQuotes", escapeQuote.toString)

  /** a flag indicating whether all values should always be enclosed in quotes.
    *
    * Default is to only escape values containing a quote character.
    *
    * Defaults to false
    *
    * @param all all quote?
    * @return
    */
  def quoteAll(all: Boolean): CsvWriter = option("quoteAll", all.toString)

  /** writes the names of columns as the first line.
    *
    * Defaults to false
    *
    * @param includeHeader include header at the first line
    * @return
    */
  def header(includeHeader: Boolean): CsvWriter = option("header", includeHeader.toString)

  /** sets the string representation of a null value.
    *
    * Defaults to empty string
    *
    * @param representation representation of a null value
    * @return
    */
  def nullValue(representation: String): CsvWriter = option("nullValue", representation)

  /** sets the string representation of an empty value.
    *
    * Defaults to empty string
    *
    * @param representation representation of a empty value
    * @return
    */
  def emptyValue(representation: String): CsvWriter = option("emptyValue", representation)

  /** specifies encoding (charset) of saved csv files. If it is not set, the UTF-8 charset will be used.
    *
    * Defaults to not set.
    *
    * @param encoding encoding
    * @return
    */
  def encoding(encoding: String): CsvWriter = option("encoding", encoding)

  /** compression codec to use when saving to file.
    *
    * This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
    *
    * Defaults to null
    *
    * @param compression compression
    * @return
    */
  def compression(compression: CompressionType): CsvWriter = option("compression", CompressionType.name(compression))

  /** sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns.
    *
    * This applies to date type.
    *
    * Defaults to yyyy-MM-dd
    *
    * @param format date format
    * @return
    */
  def dateFormat(format: String): CsvWriter = option("dateFormat", format)

  /** sets the string that indicates a timestamp format.
    *
    * Custom date formats follow the formats at Datetime Patterns.
    *
    * This applies to timestamp type.
    *
    * Defaults to yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]
    *
    * @param format timestamp format
    * @return
    */
  def timestampFormat(format: String): CsvWriter = option("timestampFormat", format)

  /** a flag indicating whether or not leading whitespaces from values being written should be skipped.
    *
    * Defaults to true
    *
    * @param ignore ignore leading whitespace
    * @return
    */
  def ignoreLeadingWhiteSpace(ignore: Boolean): CsvWriter = option("ignoreLeadingWhiteSpace", ignore.toString)

  /** a flag indicating defines whether or not trailing whitespaces from values being written should be skipped.
    *
    * Defaults to true
    *
    * @param ignore ignore trailing white space
    * @return
    */
  def ignoreTrailingWhiteSpace(ignore: Boolean): CsvWriter = option("ignoreTrailingWhiteSpace", ignore.toString)

  /** defines the line separator that should be used for writing. Maximum length is 1 character.
    *
    * Defaults to \n
    *
    * @param sep line separator
    * @return
    */
  def lineSep(sep: String): CsvWriter = option("lineSep", sep)
}
