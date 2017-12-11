package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
    {
      // Configuration variable:
      // Coordinate step is the size of each cell on x and y
      var result = 0
      coordinateOffset match {
        case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
        case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
        case 2 => {
          val timestamp = HotcellUtils.timestampParser(inputString)
          result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
        }
      }
      return result
    }

  def squareFunction(a: Int): Double = { return (a * a).toDouble; }

  def FindAllNeighborsCount(inputX: Int, inputY: Int, inputZ: Int, maxX: Int, maxY: Int, maxZ: Int, minX: Int, minY: Int, minZ: Int): Int =
    {
      var noofTermindexes = 0;
      val val8 = 8
      val val18 = 18
      val val27 = 27
      val val12 = 12

      if (inputX == minX || inputX == maxX) {
        noofTermindexes += 1;
      }

      if (inputY == minY || inputY == maxY) {
        noofTermindexes += 1;
      }

      if (inputZ == minZ || inputZ == maxZ) {
        noofTermindexes += 1;
      }

      if (noofTermindexes == 1) {

        return val18;
      } else if (noofTermindexes == 2) {
        return val12;
      } else if (noofTermindexes == 3) {
        return val8;
      } else {
        return val27;
      }

    }
  def GetGOrderStatistic(x: Int, y: Int, z: Int, mean: Double, sd: Double, noNeighbor: Int, sumNeighbor: Int, numcells: Int): Double =
    {
      val numerator = (sumNeighbor.toDouble - (mean * noNeighbor.toDouble))
      val denominator = sd * math.sqrt((((numcells.toDouble * noNeighbor.toDouble) - (noNeighbor.toDouble * noNeighbor.toDouble)) / (numcells.toDouble - 1.0).toDouble).toDouble).toDouble
      return (numerator / denominator).toDouble
    }

  def timestampParser(timestampString: String): Timestamp =
    {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val parsedDate = dateFormat.parse(timestampString)
      val timeStamp = new Timestamp(parsedDate.getTime)
      return timeStamp
    }
  def dayOfYear(timestamp: Timestamp): Int =
    {
      val calendar = Calendar.getInstance
      calendar.setTimeInMillis(timestamp.getTime)
      return calendar.get(Calendar.DAY_OF_YEAR)
    }

  def dayOfMonth(timestamp: Timestamp): Int =
    {
      val calendar = Calendar.getInstance
      calendar.setTimeInMillis(timestamp.getTime)
      return calendar.get(Calendar.DAY_OF_MONTH)
    }
}