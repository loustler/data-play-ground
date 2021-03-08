package io.loustler.dpg.security.hash

import io.loustler.dpg.testing.FunSpec
import io.loustler.dpg.util.StringUtil

final class SHA256Test extends FunSpec {
  val hashing: Hashing = SHA256()

  describe("SHA256") {
    describe("Success hashing") {
      it("when array provide") {
        val origin = StringUtil.utf8Bytes("hello, world!")

        val actual = hashing.hash(origin)

        actual should not be empty
      }
      it("when string provide") {
        val origin = "hello, world!"

        val actual = hashing.hash(origin)

        actual should not be empty
      }
      it("returns base64 encoded string if string provide") {
        val origin = "hello, world!"

        val actual = hashing.hashToString(origin)

        actual should not be empty
      }

      it("when empty array provide") {
        val actual = hashing.hash(Array.emptyByteArray)

        actual should not be empty
      }
      it("when empty string provide") {
        val origin = ""

        val actual = hashing.hash(origin)

        actual should not be empty
      }
      it("returns base64 encoded string if empty string provide") {
        val origin = ""

        val actual = hashing.hashToString(origin)

        actual should not be empty
      }
    }

  }
}
