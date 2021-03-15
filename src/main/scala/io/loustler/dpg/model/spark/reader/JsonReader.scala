package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ DataFrame, SparkSession }

final class JsonReader extends ReadFromFile[JsonReader] {

  override def read(
    spark: SparkSession,
    path: String*
  ): DataFrame = reader(spark).json(path: _*)

  /** infers all primitive values as a string type
    *
    * Defaults to false
    *
    * @param asString primitive value as a string type
    * @return
    */
  def primitivesAsString(asString: Boolean): JsonReader = option("primitivesAsString", asString.toString)

  /** infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.
    *
    * Defaults to false
    *
    * @param prefer prefer all flaoting point value as a decimal type
    * @return
    */
  def prefersDecimal(prefer: Boolean): JsonReader = option("prefersDecimal", prefer.toString)

  /** ignores Java/C++ style comment in JSON records
    *
    * Defaults to false
    *
    * @param allow allow
    * @return
    */
  def allowComments(allow: Boolean): JsonReader = option("allowComments", allow.toString)

  /** allows unquoted JSON field names
    *
    * Defaults to false
    *
    * @param allow allow
    * @return
    */
  def allowUnquotedFieldNames(allow: Boolean): JsonReader = option("allowUnquotedFieldNames", allow.toString)

  /** allows single quotes in addition to double quotes
    *
    * Defaults to false
    *
    * @param allow allow
    * @return
    */
  def allowSingleQuotes(allow: Boolean): JsonReader = option("allowSingleQuotes", allow.toString)

  /** allows leading zeros in numbers (e.g. 00012)
    *
    * Defaults to false
    *
    * @param allow allow
    * @return
    */
  def allowNumericLeadingZeros(allow: Boolean): JsonReader = option("allowNumericLeadingZeros", allow.toString)

  /** allows accepting quoting of all character using backslash quoting mechanism
    *
    * Defaults to false
    *
    * @param allow allow
    * @return
    */
  def allowBackslashEscapingAnyCharacter(allow: Boolean): JsonReader =
    option("allowBackslashEscapingAnyCharacter", allow.toString)

  /** allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.
    *
    * Defaults to false
    *
    * @param allow allow
    * @return
    */
  def allowUnquotedControlChars(allow: Boolean): JsonReader = option("allowUnquotedControlChars", allow.toString)

  /** allows a mode for dealing with corrupt records during parsing.
    *
    * Defaults to PERMISSIVE
    *
    * <pre>
    *   See available options:
    *    - PERMISSIVE : when it meets a corrupted record, puts the malformed string into a field configured by
    *      columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set
    *      a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have
    *      the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds
    *      a columnNameOfCorruptRecord field in an output schema.
    *    - DROPMALFORMED : ignores the whole corrupted records.
    *    - FAILFAST : throws an exception when it meets corrupted records.
    * </pre>
    *
    * @param mode mode
    * @return
    */
  def mode(mode: String): JsonReader = option("mode", mode)

  /** allows renaming the new field having malformed string created by PERMISSIVE mode.
    * This overrides spark.sql.columnNameOfCorruptRecord.
    *
    * Defaults to the value specified in spark.sql.columnNameOfCorruptRecord(_corrupt_record)
    *
    * @param columnName column name
    * @return
    */
  def columnNameOfCorruptRecord(columnName: String): JsonReader = option("columnNameOfCorruptRecord", columnName)

  /** sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns. This applies to date type.
    *
    * Defaults to yyyy-MM-dd
    *
    * @param format date format
    * @return
    */
  def dateFormat(format: String): JsonReader = option("dateFormat", format)

  /** sets the string that indicates a timestamp format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp type.
    *
    * Defaults to yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]
    *
    * @param format timestamp format
    * @return
    */
  def timestampFormat(format: String): JsonReader = option("timestampFormat", format)

  /** parse one record, which may span multiple lines, per file
    *
    * Defaults to false
    *
    * @param multiLine multi line
    * @return
    */
  def multiLine(multiLine: Boolean): JsonReader = option("multiLine", multiLine.toString)

  /** allows to forcibly set one of standard basic or extended encoding for the JSON files.
    * For example UTF-16BE, UTF-32LE. If the encoding is not specified and multiLine is set to true,
    * it will be detected automatically.
    *
    * Defaults to not set
    *
    * @param encoding encoding
    * @return
    */
  def encoding(encoding: String): JsonReader = option("encoding", encoding)

  /** defines the line separator that should be used for parsing.
    *
    * Defaults covers all \r, \r\n and \n
    *
    * @param sep sep
    * @return
    */
  def lineSep(sep: String): JsonReader = option("lineSep", sep)

  /** defines fraction of input JSON objects used for schema inferring.
    *
    * Defaults to 1.0
    *
    * @param ratio fraction ratio for sampling
    * @return
    */
  def samplingRatio(ratio: Double): JsonReader = option("samplingRatio", ratio.toString)

  /** whether to ignore column of all null values or empty array/struct during schema inference.
    *
    * Defaults to false
    *
    * @param drop drop null field
    * @return
    */
  def dropFieldIfAllNull(drop: Boolean): JsonReader = option("dropFieldIfAllNull", drop.toString)

  /** sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.
    *
    * Defaults to en-US
    *
    * @param locale locale
    * @return
    */
  def locale(locale: String): JsonReader = option("locale", locale)

  /** <pre>
    * allows JSON parser to recognize set of "Not-a-Number" (NaN) tokens as legal floating number values:
    *  - +INF for positive infinity, as well as alias of +Infinity and Infinity.
    *  - -INF for negative infinity), alias -Infinity.
    *  - NaN for other not-a-numbers, like result of division by zero.
    * </pre>
    *
    * Defaults to true
    *
    * @param allow allow
    * @return
    */
  def allowNonNumericNumbers(allow: Boolean): JsonReader = option("allowNonNumericNumbers", allow.toString)
}
