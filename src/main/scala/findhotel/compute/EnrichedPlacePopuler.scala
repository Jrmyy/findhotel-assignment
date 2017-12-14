package findhotel.compute

// 3rd
import findhotel.data.DataWarehouseTarget
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

// prj
import findhotel.data.SqlSources.toydb
import findhotel.base.Base

object EnrichedPlacePopuler extends Base {

  val log: Logger = Logger.getLogger(getClass.getName)

  import session.implicits._

  def main(args: Array[String]) {

    // First we are going to load the data we need, so the content of Hotel and Place tables
    val hotelDF = toydb.loadFullTable("Hotel").where($"place_id" <= 210000)
    val placeDF = toydb.loadFullTable("Place").where($"place_id" <= 210000)

    // First we are going to create a dataframe, in which we have one line for every place id with all of their parents.
    // So if a place has 2 more levels in the navigation path, we will have 2 lines for this place id
    val explodedParentsDF = getExplodedParentsDF(placeDF).persist()

    // Then we get the medianEnrichedPlace dataframe, which gives for every place id the representative nightly price
    // by calculating the median of all the hotels in the zone (for the parent we take the hotels of the zone and the
    // hotels of all the children places)
    val medianEnrichedPlaceDF = getMedianEnrichedPlaceDF(explodedParentsDF, hotelDF).persist()

    // Then we create a dataframe in which we put every place id without representative nightly price which are close
    // to a place with a representative nightly price
    val distanceDF = getNeighboursDF(placeDF, medianEnrichedPlaceDF).persist()

    // Finally we set the representative nightly price of the place without a nightly price to the same rate as the
    // closest one with a nightly price
    val addedEnrichedPlaceDF = fillInForNeighbours(medianEnrichedPlaceDF, distanceDF).persist()

    // Finally we upsert the content in the data warehouse
    DataWarehouseTarget.upsert(addedEnrichedPlaceDF, "EnrichedPlace", Seq("place_id"))

  }

  /**
    * This function is going to return one line for every place id and for all its parents. For instance for place_id 1
    * and navigation_path 3/2/1 the dataframe will create 3 lines with 1 1, 1 2 and 1 3
    * @param placeDF
    * @return
    */
  def getExplodedParentsDF(placeDF: DataFrame): DataFrame = {

    val getParents = udf(
      (placeId: Int, navigationPath: String) => getPreviousElementsInPath(placeId, navigationPath)
    )

    placeDF
      .select(
        $"place_id",
        getParents($"place_id", $"navigation_path") as "parents"
      )
      .withColumn("parent_place_id", explode($"parents"))
      .drop($"parents")
      .dropDuplicates()
  }

  /**
    * This is going to take the explodedParents dataframe and the hotels df, join them on the place id, and group by
    * parent_place_id by aggregating all the hotel nightly_rate and take the median of this set
    * @param explodedParentsDF
    * @param hotelDF
    * @return
    */
  def getMedianEnrichedPlaceDF(explodedParentsDF: DataFrame, hotelDF: DataFrame): DataFrame = {

    val getMedian = udf((prices: Seq[Any]) => median(prices))

    explodedParentsDF.as("r")
      .join(hotelDF.as("h"), $"h.place_id" === $"r.place_id", "left")
      .groupBy($"r.parent_place_id" as "place_id")
      .agg(getMedian(collect_list($"h.nightly_price")) as "representative_nightly_price")
      .withColumn("representative_nightly_price",
        when($"representative_nightly_price" === -1, null) otherwise $"representative_nightly_price"
      )
  }

  def getNeighboursDF(placeDF: DataFrame, enrichedPlaceDF: DataFrame): DataFrame = {

    session.conf.set("spark.sql.crossJoin.enabled", "true")

    val distance = udf((la1: Double, lo1: Double, la2: Double, lo2: Double) => calculateDistance(la1, lo1, la2, lo2))
    val getMedian = udf((prices: Seq[Any]) => median(prices))

    val representedDF = broadcast(enrichedPlaceDF
      .join(placeDF, enrichedPlaceDF("place_id") === placeDF("place_id"))
      .where($"representative_nightly_price" isNotNull)
      .select(
        enrichedPlaceDF("place_id"),
        placeDF("longitude"),
        placeDF("latitude"),
        enrichedPlaceDF("representative_nightly_price")
      )
    )

    val unrepresentedDF = enrichedPlaceDF
      .join(placeDF, enrichedPlaceDF("place_id") === placeDF("place_id"))
      .where($"representative_nightly_price" isNull)
      .select(enrichedPlaceDF("place_id"), placeDF("longitude"), placeDF("latitude"))
      .persist()

    unrepresentedDF.as("u")
      .join(representedDF.as("r"),  distance($"u.latitude", $"u.longitude", $"r.latitude", $"r.longitude") <= 5)
      .select(
        $"u.place_id" as "unfilled_place_id",
        $"r.representative_nightly_price" as "filled_representative_nightly_price"
      )
      .groupBy($"unfilled_place_id")
      .agg(getMedian(collect_list($"filled_representative_nightly_price")) as "unfilled_representative_nightly_price")
      .withColumn("unfilled_representative_nightly_price",
        when($"unfilled_representative_nightly_price" === -1, null) otherwise $"unfilled_representative_nightly_price"
      )
  }

  def fillInForNeighbours(enrichedPlaceDF: DataFrame, neighboursDF: DataFrame) : DataFrame = {
    enrichedPlaceDF
      .join(neighboursDF, enrichedPlaceDF("place_id") === neighboursDF("unfilled_place_id"), "left")
      .withColumn("representative_nightly_price",
        when($"representative_nightly_price" isNull, $"unfilled_representative_nightly_price")
          otherwise $"representative_nightly_price"
      )
      .select(
        $"place_id",
        $"representative_nightly_price"
      )
  }

  /**
    * Return all the elements in the navigation path before and equal to the place id
    * @param placeId
    * @param navigationPath
    * @return
    */
  def getPreviousElementsInPath(placeId: Int, navigationPath: String): Array[Int] = {
    val reversedPathParts = navigationPath.split("/").filter(!_.isEmpty).map(_.toInt)
    reversedPathParts.take(reversedPathParts.indexOf(placeId) + 1)
  }

  /**
    * Return the median of a set of data
    * @param prices
    * @return
    */
  def median(prices: Seq[Any]): Double = {
    val sorted = prices.filter(_ != null).map(_.toString).map(_.toDouble).sorted
    val length = sorted.length
    if (length == 0) {
      return -1
    }
    if (length % 2 == 1) {
      sorted(length / 2)
    } else {
      (sorted(length / 2 - 1) + sorted(length / 2))/2
    }
  }

  /**
    * Calculate the distance between 2 points defined by longitude and latitude
    * @param firstLat
    * @param firstLong
    * @param secondLat
    * @param secondLong
    * @return
    */
  def calculateDistance(firstLat: Double, firstLong: Double, secondLat: Double, secondLong: Double): Double = {
    val R = 6371; // Radius of the earth in km
    val dLat = Math.toRadians(Math.abs(secondLat - firstLat))
    val dLon = Math.toRadians(Math.abs(secondLong - firstLong))
    val a = Math.sin(dLat / 2) * Math.sin( dLat / 2) + Math.cos(Math.toRadians(firstLat)) *
      Math.cos(Math.toRadians(secondLat)) * Math.sin(dLon / 2) * Math.sin(dLon / 2)
    2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
  }

}
