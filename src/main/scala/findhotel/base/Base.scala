package findhotel.base

import java.nio.charset.StandardCharsets
import java.util.Base64
import org.apache.spark.sql.SparkSession

class Base {

  val configs = Map(
    "toydb.password" -> "OURiQmlMX1NWN2ZUdFpHaXZ4c2dRdkRWRg==",
    "toydb.user" -> "Y2FuZGlkYXRl",
    "toydb.host" -> "dG95ZGItaW5zdGFuY2UuY2JtMHB2anR6Y2dsLmV1LXdlc3QtMS5yZHMuYW1hem9uYXdzLmNvbQ==",
    "warehouse.password" -> "OURiQmlMX1NWN2ZUdFpHaXZ4c2dRdkRWRg==",
    "warehouse.user" -> "Y2FuZGlkYXRl",
    "warehouse.host" -> "dG95ZGItaW5zdGFuY2UuY2JtMHB2anR6Y2dsLmV1LXdlc3QtMS5yZHMuYW1hem9uYXdzLmNvbQ=="
  )

  val session: SparkSession = SparkSession
    .builder()
    .appName("FindHotel")
    .master("local[*]")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 4096)
    .getOrCreate()

  val tempFolder = "s3n://path/for/temp/data"

  def parseConfig(configName: String): String = {
    new String(Base64.getDecoder.decode(configs(configName)), StandardCharsets.UTF_8)
  }

}
