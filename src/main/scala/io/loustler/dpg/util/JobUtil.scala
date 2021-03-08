package io.loustler.dpg.util

import io.loustler.dpg.config.StorageConfig
import io.loustler.dpg.model.{ DataFormat, JobType }

object JobUtil {

  /** Get path
    *
    * @example {{{
    *    val df = spark.createDataFrame(???) // data frame
    *
    *    val path = JobUtil.path(appConfig.storage, JobType.RawDataJob, DataFormat.Parquet, "/path/to/my-table")
    *
    *    df.write.parquet(path)
    * }}}
    *
    * @param config storage config
    * @param jobType job type
    * @param format data format
    * @param filePath file path
    * @return
    */
  def path(config: StorageConfig, jobType: JobType, format: DataFormat, filePath: String): String = {
    val directory: String = jobType match {
      case JobType.RawDataJob  => s"${config.dataLake.path}/raw"
      case JobType.AnalyticJob => s"${config.dataLake.path}/analytics"
    }

    val fileLocation = DataFormat.fullFileName(filePath, format)

    s"$directory$fileLocation"
  }
}
