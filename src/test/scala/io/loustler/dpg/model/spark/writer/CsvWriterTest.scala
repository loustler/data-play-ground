package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import io.loustler.dpg.testing.SharedSparkSpec

final class CsvWriterTest extends SharedSparkSpec {
  describe("CsvWriter") {
    it("checks available options") {
      val writer = DataFrameWriter.csv
      writer.options should be(empty)

      writer.compression(CompressionType.LZ4)
      writer.options should have size 1

      writer.charToEscapeQuoteEscaping("\\0")
      writer.options should have size 2

      writer.dateFormat("yyyy-MM-dd")
      writer.options should have size 3

      writer.emptyValue("")
      writer.options should have size 4

      writer.encoding("UTF-8")
      writer.options should have size 5

      writer.escape("\\")
      writer.options should have size 6

      writer.header(true)
      writer.options should have size 7

      writer.ignoreLeadingWhiteSpace(true)
      writer.options should have size 8

      writer.ignoreTrailingWhiteSpace(true)
      writer.options should have size 9

      writer.quoteAll(true)
      writer.options should have size 10

      writer.nullValue("")
      writer.options should have size 11

      writer.lineSep("\\n")
      writer.options should have size 12

      writer.sep(",")
      writer.options should have size 13

      writer.timestampFormat("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      writer.options should have size 14

      writer.escapeQuotes(true)
      writer.options should have size 15

      writer.quote("\"")
      writer.options should have size 16
    }
  }
}
