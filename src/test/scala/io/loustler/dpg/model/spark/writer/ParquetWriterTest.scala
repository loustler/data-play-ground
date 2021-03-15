package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import io.loustler.dpg.testing.SharedSparkSpec

final class ParquetWriterTest extends SharedSparkSpec {
  describe("ParquetWriter") {
    it("checks available options") {
      val writer = DataFrameWriter.parquet
      writer.options should be(empty)

      writer.compression(CompressionType.GZIP)
      writer.options should have size 1
    }
  }
}
