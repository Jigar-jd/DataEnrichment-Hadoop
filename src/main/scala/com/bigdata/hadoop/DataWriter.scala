package com.bigdata.hadoop

import org.apache.hadoop.fs.{FSDataOutputStream, Path}


class DataWriter(enrichedList: List[EnrichedTrip]) extends HadoopConfiguration {


  val outputFolderPath = "/user/winter2020/jigar/course3"
  val outputFilePath = "/user/winter2020/jigar/course3/finaloutput.csv"
  val comma = ","

  val csvSchema: String = "Route_Id" + comma +
    "Service_Id" + comma +
    "Trip_Id" + comma +
    "Trip_Head_Sign" + comma +
    "Direction_Id" + comma +
    "Shape Id" + comma +
    "Wheelchair_Accessible" + comma +
    "Note_FR" + comma +
    "Note_En" + comma +
    "Agency_Id" + comma +
    "Route_Short_Name" + comma +
    "Route_Long_Name" + comma +
    "Route_ype" + comma +
    "Route_Url" + comma +
    "Route_Colour" + comma +
    "Monday"


  def writeData(): Unit = {
    if (!fileSystem.exists(new Path(outputFolderPath))) fileSystem.mkdirs(new Path(outputFolderPath))

    //Create a path
    val writePath = new Path(outputFilePath)

    //Init output stream
    val outputStream: FSDataOutputStream = fileSystem.create(writePath, true)

    outputStream.writeChars(csvSchema)

    enrichedList.foreach(element => {
      val data: String = element.tripRoute.routes.route_id.toString + comma +
        element.calender.service_id.toString + comma +
        element.tripRoute.trips.trip_id.toString + comma +
        element.tripRoute.trips.trip_headsign.toString + comma +
        element.tripRoute.trips.direction_id.toString + comma +
        element.tripRoute.trips.shape_id.toString + comma +
        element.tripRoute.trips.wheelchair_accessible.toString + comma +
        element.tripRoute.trips.note_fr.toString + comma +
        element.tripRoute.trips.note_en.toString + comma +
        element.tripRoute.routes.agency_id.toString + comma +
        element.tripRoute.routes.route_short_name.toString + comma +
        element.tripRoute.routes.route_long_name.toString + comma +
        element.tripRoute.routes.route_type.toString + comma +
        element.tripRoute.routes.route_url.toString + comma +
        element.tripRoute.routes.route_color.toString + comma +
        element.calender.monday.toString

      outputStream.writeChars("\n")
      outputStream.writeChars(data)

    })

    // closing writer connection
    outputStream.close()

  }

}
