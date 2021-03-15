package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import org.apache.spark.sql.DataFrame

final class ParquetWriter extends BaseWriter[ParquetWriter] {
  override def write(df: DataFrame, path: String): Unit = writer(df).parquet(path)

  /** compression codec to use when saving to file.
    *
    * This can be one of the known case-insensitive shorten names(none, uncompressed, snappy, gzip, lzo, brotli, lz4, and zstd).
    *
    * This will override spark.sql.parquet.compression.codec.
    *
    * default is the value specified in spark.sql.parquet.compression.codec
    *
    * @param compression compression
    * @return
    */
  def compression(compression: CompressionType): ParquetWriter =
    option("compression", CompressionType.name(compression))

}
