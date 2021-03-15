package io.loustler.dpg.model.spark.writer

import io.loustler.dpg.model.CompressionType
import org.apache.spark.sql.DataFrame

final class OrcWriter extends BaseWriter[OrcWriter] {
  override def write(df: DataFrame, path: String): Unit = writer(df).orc(path)

  /** compression codec to use when saving to file.
    *
    * This can be one of the known case-insensitive shorten names(none, snappy, zlib, and lzo).
    *
    * This will override orc.compress and spark.sql.orc.compression.codec.
    *
    * If orc.compress is given, it overrides spark.sql.orc.compression.codec.
    *
    * Default value is the value specified in spark.sql.orc.compression.codec
    *
    * @param compression compression
    * @return
    */
  def compression(compression: CompressionType): OrcWriter = option("compression", CompressionType.name(compression))

}
