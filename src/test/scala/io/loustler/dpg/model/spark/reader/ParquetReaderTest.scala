package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class ParquetReaderTest extends SharedSparkSpec {
  describe("ParquetReader") {
    it("Checks all options") {
      val reader = DataFrameReader.parquet
      reader.options should be(empty)

      reader.mergeSchema(false)
      reader.options should have size 1
    }
  }
}
