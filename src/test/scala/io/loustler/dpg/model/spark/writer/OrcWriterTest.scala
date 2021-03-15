package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import io.loustler.dpg.testing.SharedSparkSpec

final class OrcWriterTest extends SharedSparkSpec {
  describe("OrcWriter") {
    it("checks available options") {
      val writer = DataFrameWriter.orc
      writer.options should be(empty)

      writer.compression(CompressionType.Snappy)
      writer.options should have size 1
    }
  }
}
