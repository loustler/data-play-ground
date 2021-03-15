package io.loustler.dpg.model.spark.reader

import io.loustler.dpg.testing.SharedSparkSpec

final class JsonReaderTest extends SharedSparkSpec {
  describe("JsonReader") {
    it("Checks all options") {
      val reader = DataFrameReader.json
      reader.options should be(empty)

      reader.primitivesAsString(false)
      reader.options should have size 1

      reader.prefersDecimal(false)
      reader.options should have size 2

      reader.allowComments(false)
      reader.options should have size 3

      reader.allowUnquotedFieldNames(false)
      reader.options should have size 4

      reader.allowSingleQuotes(false)
      reader.options should have size 5

      reader.allowNumericLeadingZeros(false)
      reader.options should have size 6

      reader.allowBackslashEscapingAnyCharacter(false)
      reader.options should have size 7

      reader.allowUnquotedControlChars(false)
      reader.options should have size 8

      reader.mode("PERMISSIVE")
      reader.options should have size 9

      reader.columnNameOfCorruptRecord("_corrupt_record")
      reader.options should have size 10

      reader.dateFormat("yyyy-MM-dd")
      reader.options should have size 11

      reader.timestampFormat("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      reader.options should have size 12

      reader.multiLine(false)
      reader.options should have size 13

      reader.encoding("")
      reader.options should have size 14

      reader.lineSep("")
      reader.options should have size 15

      reader.samplingRatio(1.0d)
      reader.options should have size 16

      reader.dropFieldIfAllNull(false)
      reader.options should have size 17

      reader.locale("en-US")
      reader.options should have size 18

      reader.allowNonNumericNumbers(true)
      reader.options should have size 18
    }
  }
}
