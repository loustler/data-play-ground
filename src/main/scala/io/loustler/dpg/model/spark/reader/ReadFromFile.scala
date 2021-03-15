package io.loustler.dpg.model.spark.reader

private[reader] abstract class ReadFromFile[T <: ReadFromFile[T]] extends BaseReader[T] { self: T =>

  /** an optional glob pattern to only include files with paths matching the pattern.
    * The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
    *
    * @param filter path glob filter
    * @return
    */
  def pathGlobFilter(filter: String): T = option("pathGlobFilter", filter)

  /** an optional timestamp to only include files with modification times occurring before the specified Time.
    * The provided timestamp must be in the following form: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
    *
    * Batch Only
    *
    * @param timestamp timestamp
    * @return
    */
  def modifiedBefore(timestamp: String): T = option("modifiedBefore", timestamp)

  /** an optional timestamp to only include files with modification times occurring after the specified Time.
    * The provided timestamp must be in the following form: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
    *
    * Batch Only
    *
    * @param timestamp timestamp
    * @return
    */
  def modifiedAfter(timestamp: String): T = option("modifiedAfter", timestamp)

  /** recursively scan a directory for files. Using this option disables partition discovery
    *
    * @param recursive recursive scan
    * @return
    */
  def recursiveFileLookup(recursive: Boolean): T = option("recursiveFileLookup", recursive.toString)
}
