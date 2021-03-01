package io.loustler.dpg.job

import io.loustler.dpg.config.StorageConfig
import io.loustler.dpg.model.DataFormat

package object analytics {

  def destination(config: StorageConfig, filePath: String, format: DataFormat): String =
    s"${config.dataLake.path}/analytics/${DataFormat.fullFileName(filePath, format)}"
}
