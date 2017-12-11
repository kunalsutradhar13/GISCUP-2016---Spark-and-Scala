package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    var strArray: Array[String] = new Array[String](4)
    var strArray1: Array[String] = new Array[String](2)
    strArray1 = queryRectangle.split(',')
    strArray = pointString.split(',')
    var x0: Double = strArray1(0).toDouble
    var x1: Double = strArray1(2).toDouble
    var y0: Double = strArray1(1).toDouble
    var y1: Double = strArray1(3).toDouble

    var x4: Double = strArray(0).toDouble
    var y4: Double = strArray(1).toDouble

    var maxx: Double = 0;
    var minx: Double = 0;
    var maxy: Double = 0;
    var miny: Double = 0;
    if (x0 > x1) {
      maxx = x0;
      minx = x1;
    } else {
      maxx = x1;
      minx = x0;
    }
    if (y0 > y1) {
      maxy = y0;
      miny = y1;
    } else {
      maxy = y1;
      miny = y0;
    }
    if (x4 >= minx && x4 <= maxx && y4 >= miny && y4 <= maxy) {
      return true
    } else {
      return false
    }

  }

  // YOU NEED TO CHANGE THIS PART

}
