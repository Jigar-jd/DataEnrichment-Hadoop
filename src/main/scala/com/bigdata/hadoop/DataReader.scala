package com.bigdata.hadoop


import org.apache.hadoop.fs.Path


/**
 *         This class is responsible for reading the data from the data files and storing them to the data structures
 */
class DataReader extends HadoopConfiguration {

  val filePathRoutes = "/user/winter2020/jigar/routes.txt"
  val filePathCalender = "/user/winter2020/jigar/calendar.txt"
  val filePathTrips = "/user/winter2020/jigar/trips.txt"

  /**
   * method to get the route list
   *
   * @return List of routes
   */
  def getRouteList: List[Routes] = {
    val routeStream = fileSystem.open(new Path(filePathRoutes))
    val routeList: List[Routes] = Iterator.continually(routeStream.readLine()).takeWhile(_ != null)
      .drop(1)
      .map(line => line.split(","))
      .map(a => Routes(a(0).toInt, a(1), a(2), a(3), a(4), a(5), a(6))).toList
    routeStream.close()
    routeList
  }

  /**
   * method to get the calendar list
   *
   * @return List of calendar
   */
  def getCalenderList: List[Calender] = {
    val calenderStream = fileSystem.open(new Path(filePathCalender))
    val calenderList: List[Calender] = Iterator.continually(calenderStream.readLine()).takeWhile(_ != null)
      .drop(1)
      .map(line => line.split(","))
      .map(a => Calender(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9))).toList
    calenderStream.close()
    calenderList
  }

  /**
   * method to get the trip list
   *
   * @return List of trips
   */
  def getTripList: List[Trips] = {
    val tripStream = fileSystem.open(new Path(filePathTrips))
    val tripList: List[Trips] = Iterator.continually(tripStream.readLine()).takeWhile(_ != null)
      .drop(1)
      .map(line => line.split(","))
      .map(a => Trips(a(0).toInt, a(1), a(2), a(3), a(4), a(5), a(6))).toList
    tripStream.close()
    tripList
  }
}