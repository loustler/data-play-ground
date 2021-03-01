package io.loustler.dpg.job

import com.typesafe.config.ConfigFactory
import io.loustler.dpg.config.AppConfig
import io.loustler.dpg.model.DataFormat

import java.nio.file.Paths

trait SparkJob {
  def loadAppConfig(): AppConfig = AppConfig.load(ConfigFactory.load())

  /** @param tpe datasource type
    * @param name file name without extension
    */
  def resolveDataSourcePath(tpe: DataFormat, name: String): String = {
    val projectRoot = Paths.get(System.getProperty("user.dir"))

    val fileType = DataFormat.getFileExtension(tpe)

    projectRoot
      .resolve("datasource")
      .resolve(fileType)
      .resolve(s"$name.$fileType")
      .toAbsolutePath
      .toString
  }
}
