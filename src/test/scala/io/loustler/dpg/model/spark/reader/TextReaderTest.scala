package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class TextReaderTest extends SharedSparkSpec {
  describe("TextReader") {
    it("Checks all options") {
      val reader = DataFrameReader.text
      reader.options should be(empty)

      reader.wholetext(false)
      reader.options should have size 1

      reader.lineSep("")
      reader.options should have size 2
    }
  }
}
