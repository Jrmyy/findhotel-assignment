package findhotel.data

import java.util.Properties

import findhotel.base.Base
import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWarehouseTarget extends Base {

  def upsert(df: DataFrame, tableName: String, primaryKeys: Seq[String]): Unit = {

    val tempTableName = s"temp_$tableName"
    val condition = primaryKeys.map(s => s"$tableName.$s = $tempTableName.$s") mkString " AND "

    if (session.conf.get("spark.is_local") != "true") {
      val host = readConfig("warehouse.host")
      val database = readConfig("warehouse.database")
      val user = readConfig("warehouse.user")
      val password = readConfig("warehouse.password")
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
      df.write.format("jdbc")
        .option("url", "jdbc:sqlite:findhotel.db")
        .option("driver", "org.sqlite.JDBC")
        .option("dbtable", tableName)
        .mode(SaveMode.Append)
        .save()
    }

  }

}