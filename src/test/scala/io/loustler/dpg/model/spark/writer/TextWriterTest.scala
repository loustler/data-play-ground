package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import io.loustler.dpg.testing.SharedSparkSpec

final class TextWriterTest extends SharedSparkSpec {
  describe("TextWriter") {
    it("checks available options") {
      val writer = DataFrameWriter.text
      writer.options should be(empty)

      writer.compression(CompressionType.Snappy)
      writer.options should have size 1

      writer.lineSep("\\n")
      writer.options should have size 2
    }
  }
}
