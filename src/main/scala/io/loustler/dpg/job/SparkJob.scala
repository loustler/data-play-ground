package io.loustler.dpg.job

import com.typesafe.config.ConfigFactory
import io.loustler.dpg.config.AppConfig
import io.loustler.dpg.model.DataFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

object SparkJob {

  def simpleSparkSession(appName: String, master: String = "local"): SparkSession =
    sparkSession(appName = appName, master = master)(new SparkConf())

  def sparkSession(appName: String, master: String = "local")(config: SparkConf): SparkSession =
    SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .config(config)
      .getOrCreate()
}
