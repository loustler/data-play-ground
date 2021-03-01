package io.loustler.dpg.security.hash

import io.loustler.dpg.util.StringUtil

import java.util.Base64

trait Hashing {
  def hash(bytes: Array[Byte]): Array[Byte]

  def hash(s: String): Array[Byte] = hash(StringUtil.utf8Bytes(s))

  def hashToString(s: String): String = StringUtil.fromUtf8Bytes(Base64.getEncoder.encode(hash(s)))
}
