package io.loustler.dpg.model

/** Compression Type ADT(Algebraic Data Types)
  */
sealed trait CompressionType extends Product with Serializable

object CompressionType {

  /** The name of compression type
    *
    * @param compression compression type
    * @return
    */
  def name(compression: CompressionType): String =
    compression match {
      case None    => "none"
      case GZIP    => "gzip"
      case BZIP2   => "bzip2"
      case LZ4     => "lz4"
      case Snappy  => "snappy"
      case Deflate => "deflate"
      case LZO     => "lzo"
      case Brotli  => "brotli"
      case Zstd    => "zstd"
      case Zlib    => "zlib"
      case XZ      => "xz"
    }

  case object None    extends CompressionType
  case object GZIP    extends CompressionType
  case object BZIP2   extends CompressionType
  case object LZ4     extends CompressionType
  case object Snappy  extends CompressionType
  case object Deflate extends CompressionType
  case object LZO     extends CompressionType
  case object Brotli  extends CompressionType
  case object Zstd    extends CompressionType
  case object Zlib    extends CompressionType
  case object XZ      extends CompressionType

}
