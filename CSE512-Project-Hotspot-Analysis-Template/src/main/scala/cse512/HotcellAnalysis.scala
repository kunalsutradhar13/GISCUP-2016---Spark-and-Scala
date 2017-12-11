package cse512

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import java.util.Calendar

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0))))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1))))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2))))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("pickupdetails")

    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    val filterPointsDF = spark.sql("select x,y,z from pickupdetails where x>=" + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x").persist();
    filterPointsDF.createOrReplaceTempView("filteredPointsData")

    val filteredPointsWithCount = spark.sql("select x,y,z,count(*) as totalvalue from filteredPointsData group by z,y,x order by z,y,x").persist();
    filteredPointsWithCount.createOrReplaceTempView("filteredPointsWithCount")

    spark.udf.register("square", (inputX: Int) => ((HotcellUtils.squareFunction(inputX))))
    val passengerCount = spark.sql("select count(*) as sumTotal1, sum(totalvalue) as sumTotal,sum(square(totalvalue)) as sumTotalSqr from filteredPointsWithCount");
    passengerCount.createOrReplaceTempView("passengerCount")
    passengerCount.show()

    val sumPassengerCount = passengerCount.first().getLong(1);
    val sumOfSquarePassengerCount = passengerCount.first().getDouble(2);
    val sumOfTotalSquares = passengerCount.first().getLong(0);

    val MEANValue = (sumPassengerCount.toDouble / numCells.toDouble).toDouble;
    val Standard_Dev = math.sqrt(((sumOfSquarePassengerCount.toDouble / numCells.toDouble) - (MEANValue.toDouble * MEANValue.toDouble))).toDouble


    spark.udf.register("FindNeighbors", (inputX: Int, inputY: Int, inputZ: Int, maxX: Int, maxY: Int, maxZ: Int, minX: Int, minY: Int, minZ: Int) => ((HotcellUtils.FindAllNeighborsCount(inputX, inputY, inputZ, maxX, maxY, maxZ, minX, minY, minZ))))
    val NeighborQuery = "select FindNeighbors(neigh1.x, neigh1.y, neigh1.z," + maxX + "," + maxY + "," + maxZ + "," + minX + "," + minY + "," + minZ + ") as nCount, count(*) as county, neigh1.x as x, neigh1.y as y, neigh1.z as z, sum(neigh2.totalvalue) as sumtotalvalneigh from filteredPointsWithCount as neigh1, filteredPointsWithCount as neigh2 where (neigh2.x = neigh1.x+1 or neigh2.x = neigh1.x or neigh2.x =neigh1.x-1) and (neigh2.y = neigh1.y+1 or neigh2.y = neigh1.y or neigh2.y =neigh1.y-1) and (neigh2.z = neigh1.z+1 or neigh2.z = neigh1.z or neigh2.z =neigh1.z-1) group by neigh1.z, neigh1.y, neigh1.x order by neigh1.z, neigh1.y, neigh1.x"    
    val NeighBors = spark.sql(NeighborQuery).persist()
    NeighBors.createOrReplaceTempView("FinalNeighborhood");

    spark.udf.register("GetGstat", (x: Int, y: Int, z: Int, MEANValue: Double, Standard_Dev: Double, noNeighbor: Int, sumNeighbor: Int, numcells: Int) => ((HotcellUtils.GetGOrderStatistic(x, y, z, MEANValue, Standard_Dev, noNeighbor, sumNeighbor, numcells))))
    val queryForGstat = "select GetGstat(x,y,z," + MEANValue + "," + Standard_Dev + ",ncount,sumtotalvalneigh," + numCells + ") as GStatistic,x, y, z from FinalNeighborhood order by GStatistic desc"
    val GStatOfNeighbors = spark.sql(queryForGstat);
    GStatOfNeighbors.createOrReplaceTempView("GstatWithXYZ")
    GStatOfNeighbors.show()

    val finalQuery = "select x,y,z from GstatWithXYZ order by GStatistic desc"
    val finalresult = spark.sql(finalQuery)
    finalresult.createOrReplaceTempView("ResultantTable")
    return finalresult.coalesce(1) // YOU NEED TO CHANGE THIS PART
  }
}