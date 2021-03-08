package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import io.loustler.dpg.testing.SharedSparkSpec

final class DataFrameWriterTest extends SharedSparkSpec {
  describe("DataFrameWriter") {
    it("Success create correct writer instance") {
      DataFrameWriter.parquet shouldBe a[DataFrameWriter.ParquetWriter]
      DataFrameWriter.json shouldBe a[DataFrameWriter.JsonWriter]
      DataFrameWriter.csv shouldBe a[DataFrameWriter.CsvWriter]
      DataFrameWriter.orc shouldBe a[DataFrameWriter.OrcWriter]
      DataFrameWriter.text shouldBe a[DataFrameWriter.TextWriter]
    }

    describe("checks available options") {
      it("ParquetWriter can use all available options") {
        val writer = DataFrameWriter.parquet
        writer.options should be(empty)

        writer.compression(CompressionType.GZIP)
        writer.options should have size 1
      }
      it("JsonWriter can use all available options") {
        val writer = DataFrameWriter.json
        writer.options should be(empty)

        writer.compression(CompressionType.Snappy)
        writer.options should have size 1

        writer.dateFormat("yyyy-MM-dd")
        writer.options should have size 2

        writer.encoding("UTF-8")
        writer.options should have size 3

        writer.ignoreNullFields(true)
        writer.options should have size 4

        writer.lineSep("\\n")
        writer.options should have size 5

        writer.timestampFormat("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
        writer.options should have size 6
      }
      it("CsvWriter can use all available options") {
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
      it("OrcWriter can use all available options") {
        val writer = DataFrameWriter.orc
        writer.options should be(empty)

        writer.compression(CompressionType.Snappy)
        writer.options should have size 1
      }
      it("TextWriter can use all available options") {
        val writer = DataFrameWriter.text
        writer.options should be(empty)

        writer.compression(CompressionType.Snappy)
        writer.options should have size 1

        writer.lineSep("\\n")
        writer.options should have size 2
      }
    }
  }
}
