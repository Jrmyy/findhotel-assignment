package findhotel.data

import findhotel.base.Base
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class DatabaseSource(name: String, engine: String, host: String, port: Int, database: String, user: String,
                          password: String) extends Base {

  val MYSQL_ENGINE = "mysql"
  val POSTGRES_ENGINE = "postgresql"

  val ENGINES = Seq(MYSQL_ENGINE, POSTGRES_ENGINE)

  val reader: DataFrameReader = session.read
    .format("jdbc")
    .option("url", "jdbc:"+ this.engine +"://" + this.host + ":" + this.port + "/" + this.database)
    .option("driver", this.engine match {
      case MYSQL_ENGINE => "com.mysql.jdbc.Driver"
      case POSTGRES_ENGINE => "org.postgresql.Driver"
    })
    .option("user", this.user)
    .option("password", this.password)

  def loadFullTable(tableName: String): DataFrame = {
    reader.option("dbtable", tableName).load()
  }
}
