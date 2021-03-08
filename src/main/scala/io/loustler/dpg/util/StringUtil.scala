package io.loustler.dpg.util

import java.nio.charset.StandardCharsets

object StringUtil {

  /** Create UTF-8 encoding byte array from string
    *
    * @param s string
    * @return
    */
  def utf8Bytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  /** Create string from array bytes utf-8 encoding
    *
    * @param bytes utf-8 encoding array bytes
    * @return
    */
  def fromUtf8Bytes(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
}
