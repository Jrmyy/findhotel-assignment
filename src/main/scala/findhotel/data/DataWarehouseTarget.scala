package findhotel.data

import java.util.Properties

import findhotel.base.Base
import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWarehouseTarget extends Base {

  def upsert(df: DataFrame, tableName: String, primaryKeys: Seq[String]): Unit = {

    val tempTableName = s"temp_$tableName"
    val condition = primaryKeys.map(s => s"$tableName.$s = $tempTableName.$s") mkString " AND "

    if (session.conf.get("spark.is_local") != "true") {
      val host = parseConfig("warehouse.host")
      val database = parseConfig("warehouse.database")
      val user = parseConfig("warehouse.user")
      val password = parseConfig("warehouse.password")
      df.write
        .format("com.databricks.spark.redshift")
        .option("url", s"jdbc:redshift://$host/$database?user=$user&password=$password")
        .option("dbtable", tempTableName)
        .option("tempdir", tempFolder)
        .option(
          "postactions",
          s"""
             |DELETE FROM $tableName USING $tempTableName WHERE $condition;
             |INSERT INTO $tableName SELECT * FROM $tempTableName;
             |DROP TABLE $tempTableName;
         """.stripMargin
        )
        .mode("error")
        .save()
    } else {
      val dbProperties = new Properties
      dbProperties.setProperty("driver", "org.postgresql.Driver")
      val jdbcUrl = s"jdbc:postgresql://localhost/findhotel"
      df.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, tableName, dbProperties)
    }

  }

}