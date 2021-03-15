package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import io.loustler.dpg.testing.SharedSparkSpec

final class JsonWriterTest extends SharedSparkSpec {
  describe("JsonWriter") {
    it("checks available options") {
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
  }
}
