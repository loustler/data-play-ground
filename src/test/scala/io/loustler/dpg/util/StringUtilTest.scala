package io.loustler.dpg.util

import io.loustler.dpg.testing.FunSpec

final class StringUtilTest extends FunSpec {

  val strings = List(
    "Hello",
    "안녕",
    "こんにちは",
    "Привет",
    "你好",
    "ياخشىمۇسىز"
  )

  describe("StringUtil") {
    describe("utf8Bytes") {
      it("Success encode string to UTF-8 byte array") {
        strings.foreach(s => StringUtil.utf8Bytes(s) should not be empty)
      }
    }

    describe("fromUtf8Bytes") {
      it("Success create string from UTF-8 byte array") {
        strings.foreach { s =>
          val bytes = StringUtil.utf8Bytes(s)

          val actual = StringUtil.fromUtf8Bytes(bytes)

          actual should ===(s)
        }
      }
    }
  }
}
