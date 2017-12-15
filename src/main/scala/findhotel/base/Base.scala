package findhotel.base

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.apache.spark.sql.SparkSession

class Base {

  val session: SparkSession = SparkSession
    .builder()
    .appName("FindHotel")
    .master("local[*]")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 4096)
    .getOrCreate()

  val tempFolder = "s3n://path/for/temp/data"

  val customConfig: Config = loadConfig("/credentials.properties")

  def loadConfig(filename: String): Config = {
    val in = getClass.getResourceAsStream(filename)
    if (in == null) return null
    val file = File.createTempFile(String.valueOf(in.hashCode()), ".conf")
    file.deleteOnExit()

    val out = new FileOutputStream(file)
    val buffer: Array[Byte] = new Array(1024)
    var bytesRead = in.read(buffer)

    while (bytesRead != -1) {
      out.write(buffer, 0, bytesRead)
      bytesRead = in.read(buffer)
    }
    out.close()

    ConfigFactory.parseFile(
      file,
      ConfigParseOptions.defaults().setAllowMissing(false).setOriginDescription("Merged with " + filename)
    )
  }

  def readConfig(name: String): String = {
    new String(Base64.getDecoder.decode(customConfig.getString(name)), StandardCharsets.UTF_8)
  }

}
