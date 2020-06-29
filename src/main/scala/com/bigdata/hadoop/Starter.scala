package com.mcit.scala.AppStarter
import com.bigdata.hadoop.{CalendarLookup, Calender, DataReader, DataWriter, EnrichedTrip, RouteLookup, Routes, TripRoute, Trips}



object Starter extends App {

  val readerData : DataReader = new DataReader

  val tripList: List[Trips] = readerData.getTripList
  val routeList: List[Routes] = readerData.getRouteList
  val calendarList: List[Calender] = readerData.getCalenderList

  val routeLookup = new RouteLookup(routeList)
  val calenderLookUp = new CalendarLookup(calendarList)

  val enrichedTripRoute: List[TripRoute] = tripList.map(trip => TripRoute(trip,
    routeLookup.lookup(trip.route_id)))

  val enrichedTrip: List[EnrichedTrip] = enrichedTripRoute.map(tripRoute => EnrichedTrip(tripRoute,
    calenderLookUp.lookup(tripRoute.trips.service_id)))

  val finalList: List[EnrichedTrip] = enrichedTrip.filter(a => a.calender.monday.equals("1") && a.tripRoute.routes.route_type.equals("1"))
  val writer = new DataWriter(finalList)
  writer.writeData()
}