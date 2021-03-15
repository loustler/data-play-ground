package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class CsvReaderTest extends SharedSparkSpec {
  describe("CsvReader") {
    it("Checks all options") {
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

      reader.lineSep("\\n")
      reader.options should have size 27
    }
  }
}
