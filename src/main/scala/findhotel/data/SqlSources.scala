package findhotel.data

import findhotel.base.Base

object SqlSources extends Base {

  val toydb = DatabaseSource(
      name = "Toydb",
      engine = "mysql",
      host = readConfig("toydb.host"),
      port = 3306,
      database = readConfig("toydb.database"),
      user = readConfig("toydb.user"),
      password = readConfig("toydb.password")
  )

}
