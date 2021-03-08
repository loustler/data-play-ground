package io.loustler.dpg.model.spark.reader

import org.apache.spark.sql.{ DataFrame, SparkSession, DataFrameReader => SparkReader }

import scala.collection.mutable.{ Map => MutableMap }

sealed trait DataFrameReader {
  def read(spark: SparkSession, path: String*): DataFrame

  def options: Map[String, String]
}

object DataFrameReader {
  def csv: CsvReader = new CsvReader

  def json: JsonReader = new JsonReader

  def orc: OrcReader = new OrcReader

  def parquet: ParquetReader = new ParquetReader

  def text: TextReader = new TextReader

  private[reader] abstract class BaseReader[T <: BaseReader[T]] extends DataFrameReader { self: T =>
    private val readOptions: MutableMap[String, String] = MutableMap.empty

    override def options: Map[String, String] = readOptions.toMap

    protected def reader(spark: SparkSession): SparkReader =
      options.foldLeft(spark.read) { case (reader, (key, value)) =>
        reader.option(key, value)
      }

    protected def option(key: String, value: String): T = {
      readOptions.update(key, value)
      self
    }
  }

  private[reader] abstract class ReadFromFile[T <: ReadFromFile[T]] extends BaseReader[T] { self: T =>

    /** an optional glob pattern to only include files with paths matching the pattern.
      * The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
      *
      * @param filter path glob filter
      * @return
      */
    def pathGlobFilter(filter: String): T = option("pathGlobFilter", filter)

    /** an optional timestamp to only include files with modification times occurring before the specified Time.
      * The provided timestamp must be in the following form: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
      *
      * Batch Only
      *
      * @param timestamp timestamp
      * @return
      */
    def modifiedBefore(timestamp: String): T = option("modifiedBefore", timestamp)

    /** an optional timestamp to only include files with modification times occurring after the specified Time.
      * The provided timestamp must be in the following form: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
      *
      * Batch Only
      *
      * @param timestamp timestamp
      * @return
      */
    def modifiedAfter(timestamp: String): T = option("modifiedAfter", timestamp)

    /** recursively scan a directory for files. Using this option disables partition discovery
      *
      * @param recursive recursive scan
      * @return
      */
    def recursiveFileLookup(recursive: Boolean): T = option("recursiveFileLookup", recursive.toString)
  }

  private[reader] final class CsvReader extends ReadFromFile[CsvReader] {

    override def read(
      spark: SparkSession,
      path: String*
    ): DataFrame = reader(spark).csv(path: _*)

    /** sets a separator for each field and value. This separator can be one or more characters.
      *
      * Defaults to ,
      *
      * @param sep sep
      * @return
      */
    def sep(sep: String): CsvReader = option("sep", sep)

    /** decodes the CSV files by the given encoding type.
      *
      * Defaults to UTF-8
      *
      * @param encoding encoding
      * @return
      */
    def encoding(encoding: String): CsvReader = option("encoding", encoding)

    /** sets a single character used for escaping quoted values where the separator can be part of the value.
      * If you would like to turn off quotations, you need to set not null but an empty string.
      * This behaviour is different from com.databricks.spark.csv.
      *
      * Defaults to "
      *
      * @param quote quote
      * @return
      */
    def quote(quote: String): CsvReader = option("quote", quote)

    /** sets a single character used for escaping quotes inside an already quoted value.
      *
      * Defaults to \
      *
      * @param escape escape
      * @return
      */
    def escape(escape: String): CsvReader = option("escape", escape)

    /** sets a single character used for escaping the escape for the quote character.
      * The default value is escape character when escape and quote characters are different, \0 otherwise.
      *
      * Defaults to escape or \0
      *
      * @param escape escape
      * @return
      */
    def charToEscapeQuoteEscaping(escape: String): CsvReader = option("charToEscapeQuoteEscaping", escape)

    /** sets a single character used for skipping lines beginning with this character. By default, it is disabled.
      *
      * Defaults to empty string
      *
      * @param comment comment
      * @return
      */
    def comment(comment: String): CsvReader = option("comment", comment)

    /** uses the first line as names of columns.
      *
      * Defaults to false
      *
      * @param header header
      * @return
      */
    def header(header: Boolean): CsvReader = option("header", header.toString)

    /** If it is set to true, the specified or inferred schema will be forcibly applied to datasource files,
      * and headers in CSV files will be ignored.
      *
      * If the option is set to false, the schema will be validated against all headers in CSV files in the case when
      * the header option is set to true.
      *
      * Field names in the schema and column names in CSV headers are checked by their positions taking into account spark.sql.caseSensitive.
      * Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results.
      *
      * Defaults to true
      *
      * @param enforce enforce
      * @return
      */
    def enforceSchema(enforce: Boolean): CsvReader = option("enforceSchema", enforce.toString)

    /** infers the input schema automatically from data. It requires one extra pass over the data.
      *
      * Defaults to false
      *
      * @param infer infer schema
      * @return
      */
    def inferSchema(infer: Boolean): CsvReader = option("inferSchema", infer.toString)

    /** defines fraction of rows used for schema inferring.
      *
      * Defaults to 1.0
      *
      * @param ratio sample fraction ratio
      * @return
      */
    def samplingRatio(ratio: Double): CsvReader = option("samplingRatio", ratio.toString)

    /** a flag indicating whether or not leading whitespaces from values being read should be skipped.
      *
      * Defaults to false
      *
      * @param ignore ignore
      * @return
      */
    def ignoreLeadingWhiteSpace(ignore: Boolean): CsvReader = option("ignoreLeadingWhiteSpace", ignore.toString)

    /** a flag indicating whether or not trailing whitespaces from values being read should be skipped.
      *
      * Defaults to false
      *
      * @param ignore ignore
      * @return
      */
    def ignoreTrailingWhiteSpace(ignore: Boolean): CsvReader = option("ignoreTrailingWhiteSpace", ignore.toString)

    /** sets the string representation of a null value. Since 2.0.1, this applies to all supported types including the string type.
      *
      * Defaults to empty string
      *
      * @param representation representation of null value
      * @return
      */
    def nullValue(representation: String): CsvReader = option("nullValue", representation)

    /** sets the string representation of an empty value.
      *
      * Defaults to empty string
      *
      * @param representation representation of empty value
      * @return
      */
    def emptyValue(representation: String): CsvReader = option("emptyValue", representation)

    /** sets the string representation of a non-number" value.
      *
      * Defaults to NaN
      *
      * @param representation representation of NaN
      * @return
      */
    def nanValue(representation: String): CsvReader = option("nanValue", representation)

    /** sets the string representation of a positive infinity value.
      *
      * Defaults to Inf
      *
      * @param representation representation of positive infinity value
      * @return
      */
    def positiveInf(representation: String): CsvReader = option("positiveInf", representation)

    /** sets the string representation of a negative infinity value.
      *
      * Defaults to -Inf
      *
      * @param representation representation negative infinity value
      * @return
      */
    def negativeInf(representation: String): CsvReader = option("negativeInf", representation)

    /** sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns. This applies to date type.
      *
      * Defaults to yyyy-MM-dd
      *
      * @param format date format
      * @return
      */
    def dateFormat(format: String): CsvReader = option("dateFormat", format)

    /** sets the string that indicates a timestamp format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp type.
      *
      * Defaults to yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]
      *
      * @param format timestamp format
      * @return
      */
    def timestampFormat(format: String): CsvReader = option("timestampFormat", format)

    /** defines a hard limit of how many columns a record can have.
      *
      * Defaults to 20480
      *
      * @param max hard limit of column rows
      * @return
      */
    def maxColumns(max: Int): CsvReader = option("maxColumns", max.toString)

    /** defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
      *
      * Defaults to -1
      *
      * @param max max chars per column
      * @return
      */
    def maxCharsPerColumn(max: Int): CsvReader = option("maxCharsPerColumn", max.toString)

    /** defines how the CsvParser will handle values with unescaped quotes.
      *
      * Defaults to STOP_AT_DELIMITER.
      *
      * <pre>
      *   See available options:
      *   - STOP_AT_CLOSING_QUOTE: If unescaped quotes are found in the input,
      *       accumulate the quote character and proceed parsing the value as a quoted value, until a closing quote is found.
      *   - BACK_TO_DELIMITER: If unescaped quotes are found in the input,
      *       consider the value as an unquoted value. This will make the parser accumulate all characters of
      *       the current parsed value until the delimiter is found. If no delimiter is found in the value,
      *       the parser will continue accumulating characters from the input until a delimiter or line ending is found.
      *   - STOP_AT_DELIMITER: If unescaped quotes are found in the input, consider the value as an unquoted value.
      *       This will make the parser accumulate all characters until the delimiter or a line ending is found in the input.
      *   - STOP_AT_DELIMITER: If unescaped quotes are found in the input, the content parsed for the given value will
      *        be skipped and the value set in nullValue will be produced instead.
      *    - RAISE_ERROR: If unescaped quotes are found in the input, a TextParsingException will be thrown.
      * </pre>
      *
      * @param escape escape
      * @return
      */
    def unescapedQuoteHandling(escape: String): CsvReader = option("unescapedQuoteHandling", escape)

    /** allows a mode for dealing with corrupt records during parsing.
      * It supports the following case-insensitive modes.
      * Note that Spark tries to parse only required columns in CSV under column pruning.
      * Therefore, corrupt records can be different based on required set of fields.
      * This behavior can be controlled by spark.sql.csv.parser.columnPruning.enabled (enabled by default)
      *
      * Defaults to PERMISSIVE
      *
      * <pre>
      *   See available options:
      *    - PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by
      *        columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set
      *        a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have
      *        the field, it drops corrupt records during parsing. A record with less/more tokens than schema is not
      *        a corrupted record to CSV. When it meets a record having fewer tokens than the length of the schema,
      *        sets null to extra fields. When the record has more tokens than the length of the schema,
      *        it drops extra tokens.
      *    - DROPMALFORMED: ignores the whole corrupted records.
      *    - FAILFAST: throws an exception when it meets corrupted records.
      * </pre>
      *
      * @param mode
      * @return
      */
    def mode(mode: String): CsvReader = option("mode", mode)

    /** allows renaming the new field having malformed string created by PERMISSIVE mode.
      * This overrides spark.sql.columnNameOfCorruptRecord.
      *
      * Defaults to the value specified in spark.sql.columnNameOfCorruptRecord(_corrupt_record)
      *
      * @param name column name
      * @return
      */
    def columnNameOfCorruptRecord(name: String): CsvReader = option("columnNameOfCorruptRecord", name)

    /** parse one record, which may span multiple lines.
      *
      * Defaults to false
      *
      * @param multiline multi line
      * @return
      */
    def multiLine(multiline: Boolean): CsvReader = option("multiLine", multiline.toString)

    /** sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.
      *
      * Defaults to en-US
      *
      * @param locale locale
      * @return
      */
    def locale(locale: String): CsvReader = option("locale", locale)

    /** defines the line separator that should be used for parsing. Maximum length is 1 character.
      *
      * Defaults covers all \r, \r\n and \n
      *
      * @param sep sep
      * @return
      */
    def lineSep(sep: String): CsvReader = option("lineSep", sep)
  }

  private[reader] final class JsonReader extends ReadFromFile[JsonReader] {

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

  private[reader] final class OrcReader extends ReadFromFile[OrcReader] {

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

  private[reader] final class ParquetReader extends ReadFromFile[ParquetReader] {

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

  private[reader] final class TextReader extends ReadFromFile[TextReader] {

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
}
