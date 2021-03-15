package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class OrcReaderTest extends SharedSparkSpec {
  describe("OrcReader") {
    it("Checks all options") {
      val reader = DataFrameReader.orc
      reader.options should be(empty)

      reader.mergeSchema(false)
      reader.options should have size 1
    }
  }
}
