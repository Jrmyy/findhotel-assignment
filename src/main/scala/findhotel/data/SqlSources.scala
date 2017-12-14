package findhotel.data

import findhotel.base.Base

object SqlSources extends Base {

  val toydb = DatabaseSource(
      name = "Toydb",
      engine = "mysql",
      host = parseConfig("toydb.host"),
      port = 3306,
      database = "toydb",
      user = parseConfig("toydb.user"),
      password = parseConfig("toydb.password")
  )

}
