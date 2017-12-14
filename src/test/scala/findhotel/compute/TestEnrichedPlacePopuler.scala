package findhotel.compute

import org.apache.spark.sql.{DataFrame, Row}
import findhotel.base.TestBase
import org.apache.spark.sql.types._

class TestEnrichedPlacePopuler extends TestBase {

  def initExplodedData(): Seq[Row] = {
    Seq(
      Row(4, 138386),
      Row(4, 21538),
      Row(4, 73712),
      Row(4, 4),
      Row(5, 251678),
      Row(5, 5),
      Row(6, 6),
      Row(21538, 138386),
      Row(21538, 21538),
      Row(73712, 21538),
      Row(73712, 73712),
      Row(73712, 138386),
      Row(138386, 138386),
      Row(251678, 251678)
    )
  }

  test("Median unit test"){
    EnrichedPlacePopuler.median(Seq(22, 35, 31)) shouldBe 31.0
    EnrichedPlacePopuler.median(Seq(22, 35, 31, 12)) shouldBe 26.5
    EnrichedPlacePopuler.median(Seq(null, null)) shouldBe -1
  }

  test("getPreviousElementsInPath unit test") {
    EnrichedPlacePopuler.getPreviousElementsInPath(12, "/0/1/121/12/24/32/") shouldBe Array(0, 1, 121, 12)
    EnrichedPlacePopuler.getPreviousElementsInPath(0, "/0/") shouldBe Array(0)
  }

  test("calculate distance from longitude latitude to kilometers") {
    // First 2 are latitude and longitude for Paris
    // The last 2 are latitude and longitude for New York
    (EnrichedPlacePopuler.calculateDistance(48.866667, 2.333333, 40.7830603, -73.97124880000001) - 5828.28).abs should
      be < 0.5
  }

  test("get exploded parents dataframe") {

    val schema = StructType(Seq(
      StructField("place_id", IntegerType),
      StructField("navigation_path", StringType)
    ))
    val testDF = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(
      Row(4, "/138386/21538/73712/4/"),
      Row(5, "/251678/5/"),
      Row(21538, "/138386/21538/"),
      Row(73712, "/138386/21538/73712/"),
      Row(138386, "/138386/"),
      Row(251678, "/251678/"),
      Row(6, "/6/")
    )), schema)

    val explodedParentsDF = EnrichedPlacePopuler.getExplodedParentsDF(testDF)

    explodedParentsDF.collect() should contain theSameElementsAs initExplodedData()

  }

  test("create enriched place df") {
    val explodedParentSchema = StructType(Seq(
      StructField("place_id", IntegerType),
      StructField("parent_place_id", IntegerType)
    ))

    val explodedParentsDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(initExplodedData()), explodedParentSchema
    )

    val hotelSchema = StructType(Seq(
      StructField("hotel_id", IntegerType),
      StructField("place_id", IntegerType),
      StructField("nightly_price", DoubleType)
    ))

    val hotelDF = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(
      Row(1, 4, 12.0),
      Row(2, 4, 22.0),
      Row(3, 4, 13.0),
      Row(4, 5, 65.0),
      Row(5, 5, 77.0),
      Row(15, 6, null),
      Row(6, 21538, 60.0),
      Row(7, 21538, 48.0),
      Row(8, 73712, 47.0),
      Row(9, 138386, 44.0),
      Row(10, 138386, 42.0),
      Row(11, 251678, 88.0),
      Row(12, 251678, 60.0),
      Row(13, 251678, 44.0),
      Row(14, 251678, 66.0)
    )), hotelSchema)

    val enrichedPlaceDF = EnrichedPlacePopuler.getMedianEnrichedPlaceDF(explodedParentsDF, hotelDF)

    enrichedPlaceDF.collect() should contain theSameElementsAs Seq(
      Row(4, 13.0),
      Row(5, 71.0),
      Row(6, null),
      Row(21538, 34.5),
      Row(73712, 17.5),
      Row(138386, 43.0),
      Row(251678, 65.5)
    )

  }

}