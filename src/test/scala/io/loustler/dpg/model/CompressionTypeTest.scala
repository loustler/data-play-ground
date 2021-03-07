package io.loustler.dpg.model

import io.loustler.dpg.testing.FunSpec

final class CompressionTypeTest extends FunSpec {
  describe("CompressionType") {
    describe("name") {
      it("Success find out correct name of type") {
        CompressionType.name(CompressionType.None) should ===("none")
        CompressionType.name(CompressionType.GZIP) should ===("gzip")
        CompressionType.name(CompressionType.BZIP2) should ===("bzip2")
        CompressionType.name(CompressionType.LZ4) should ===("lz4")
        CompressionType.name(CompressionType.Snappy) should ===("snappy")
        CompressionType.name(CompressionType.Deflate) should ===("deflate")
        CompressionType.name(CompressionType.LZO) should ===("lzo")
        CompressionType.name(CompressionType.Brotli) should ===("brotli")
        CompressionType.name(CompressionType.Zstd) should ===("zstd")
        CompressionType.name(CompressionType.Zlib) should ===("zlib")
        CompressionType.name(CompressionType.XZ) should ===("xz")
      }
    }
  }
}
