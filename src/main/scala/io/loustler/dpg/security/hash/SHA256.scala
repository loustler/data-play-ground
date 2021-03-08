package io.loustler.dpg.security.hash

import java.security.MessageDigest

final class SHA256 private (digest: MessageDigest) extends Hashing {
  override def hash(bytes: Array[Byte]): Array[Byte] = digest.digest(bytes)
}

object SHA256 {

  def apply(): SHA256 =
    new SHA256(
      MessageDigest.getInstance("SHA-256")
    )
}
