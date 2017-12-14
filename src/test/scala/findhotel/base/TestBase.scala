package findhotel.base

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class TestBase extends FunSuite with BeforeAndAfterEach with Matchers {

  var sparkSession : SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }
}
