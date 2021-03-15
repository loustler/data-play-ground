package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class DataFrameReaderTest extends SharedSparkSpec {
  describe("DataFrameReader") {
    it("Success create correct writer instance") {
      DataFrameReader.csv shouldBe a[CsvReader]
      DataFrameReader.json shouldBe a[JsonReader]
      DataFrameReader.orc shouldBe a[OrcReader]
      DataFrameReader.parquet shouldBe a[ParquetReader]
      DataFrameReader.text shouldBe a[TextReader]
    }

    describe("checks available options") {
      it("CsvReader can use all available options") {
        val reader = DataFrameReader.csv
        reader.options should be(empty)

        reader.sep(",")
        reader.options should have size 1

        reader.encoding("UTF-8")
        reader.options should have size 2

        reader.quote("\"")
        reader.options should have size 3

        reader.escape("\\")
        reader.options should have size 4

        reader.charToEscapeQuoteEscaping("\\0")
        reader.options should have size 5

        reader.comment("")
        reader.options should have size 6

        reader.header(false)
        reader.options should have size 7

        reader.enforceSchema(true)
        reader.options should have size 8

        reader.inferSchema(false)
        reader.options should have size 9

        reader.samplingRatio(1.0d)
        reader.options should have size 10

        reader.ignoreLeadingWhiteSpace(false)
        reader.options should have size 11

        reader.ignoreTrailingWhiteSpace(false)
        reader.options should have size 12

        reader.nullValue("")
        reader.options should have size 13

        reader.emptyValue("")
        reader.options should have size 14

        reader.nanValue("NaN")
        reader.options should have size 15

        reader.positiveInf("Inf")
        reader.options should have size 16

        reader.negativeInf("-Inf")
        reader.options should have size 17

        reader.dateFormat("yyyy-MM-dd")
        reader.options should have size 18

        reader.timestampFormat("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
        reader.options should have size 19

        reader.maxColumns(20480)
        reader.options should have size 20

        reader.maxCharsPerColumn(-1)
        reader.options should have size 21

        reader.unescapedQuoteHandling("STOP_AT_DELIMITER")
        reader.options should have size 22

        reader.mode("PERMISSIVE")
        reader.options should have size 23

        reader.columnNameOfCorruptRecord("_corrupt_record")
        reader.options should have size 24

        reader.multiLine(false)
        reader.options should have size 25

        reader.locale("en-US")
        reader.options should have size 26

        reader.pathGlobFilter("")
        reader.options should have size 27

        reader.modifiedBefore("2020-06-01T13:00:00")
        reader.options should have size 28

        reader.modifiedAfter("2020-06-01T13:00:00")
        reader.options should have size 29

        reader.recursiveFileLookup(true)
        reader.options should have size 30
      }
      it("JsonReader can use all available options") {
        val reader = DataFrameReader.json
        reader.options should be(empty)

        reader.primitivesAsString(false)
        reader.options should have size 1

        reader.prefersDecimal(false)
        reader.options should have size 2

        reader.allowComments(false)
        reader.options should have size 3

        reader.allowUnquotedFieldNames(false)
        reader.options should have size 4

        reader.allowSingleQuotes(false)
        reader.options should have size 5

        reader.allowNumericLeadingZeros(false)
        reader.options should have size 6

        reader.allowBackslashEscapingAnyCharacter(false)
        reader.options should have size 7

        reader.allowUnquotedControlChars(false)
        reader.options should have size 8

        reader.mode("PERMISSIVE")
        reader.options should have size 9

        reader.columnNameOfCorruptRecord("_corrupt_record")
        reader.options should have size 10

        reader.dateFormat("yyyy-MM-dd")
        reader.options should have size 11

        reader.timestampFormat("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
        reader.options should have size 12

        reader.multiLine(false)
        reader.options should have size 13

        reader.encoding("")
        reader.options should have size 14

        reader.lineSep("")
        reader.options should have size 15

        reader.samplingRatio(1.0d)
        reader.options should have size 16

        reader.dropFieldIfAllNull(false)
        reader.options should have size 17

        reader.locale("en-US")
        reader.options should have size 18

        reader.pathGlobFilter("")
        reader.options should have size 19

        reader.modifiedBefore("2020-06-01T13:00:00")
        reader.options should have size 20

        reader.modifiedAfter("2020-06-01T13:00:00")
        reader.options should have size 21

        reader.recursiveFileLookup(true)
        reader.options should have size 22
      }
      it("OrcReader can use all available options") {
        val reader = DataFrameReader.orc
        reader.options should be(empty)

        reader.mergeSchema(false)
        reader.options should have size 1

        reader.pathGlobFilter("")
        reader.options should have size 2

        reader.modifiedBefore("2020-06-01T13:00:00")
        reader.options should have size 3

        reader.modifiedAfter("2020-06-01T13:00:00")
        reader.options should have size 4

        reader.recursiveFileLookup(true)
        reader.options should have size 5
      }
      it("ParquetReader can use all available options") {
        val reader = DataFrameReader.parquet
        reader.options should be(empty)

        reader.mergeSchema(false)
        reader.options should have size 1

        reader.pathGlobFilter("")
        reader.options should have size 2

        reader.modifiedBefore("2020-06-01T13:00:00")
        reader.options should have size 3

        reader.modifiedAfter("2020-06-01T13:00:00")
        reader.options should have size 4

        reader.recursiveFileLookup(true)
        reader.options should have size 5
      }
      it("TextReader can use all available options") {
        val reader = DataFrameReader.text
        reader.options should be(empty)

        reader.wholetext(false)
        reader.options should have size 1

        reader.lineSep("")
        reader.options should have size 2

        reader.pathGlobFilter("")
        reader.options should have size 3

        reader.modifiedBefore("2020-06-01T13:00:00")
        reader.options should have size 4

        reader.modifiedAfter("2020-06-01T13:00:00")
        reader.options should have size 5

        reader.recursiveFileLookup(true)
        reader.options should have size 6
      }
    }
  }
}
