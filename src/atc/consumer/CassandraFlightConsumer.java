package atc.consumer;

import atc.beans.FlightInstance;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class CassandraFlightConsumer extends AbstractCassandraWriter<FlightInstance> {

    private DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private String flightColumnFamily;
    private String flightRouteColumnFamily;
    private String flightDepartureColumnFamily;

    @Override
    public void send(FlightInstance flight) {

        long ts = System.currentTimeMillis();

        String flightId = flight.getId();

        // get the unique key which contains this flight
        // computed as: ${takeoffYear}${takeoffMonth}${takeoffDay}-${departureAirport}-${arrivalAirport}

        StringBuffer sb = new StringBuffer();
        sb.append(df.format(flight.getTakeoffTime())).append("-")
          .append(flight.getDepartureAirport());

        // get the departure id (departure airport only)
        String departureId = sb.toString();

        sb.append("-")
          .append(flight.getArrivalAirport());

        // get the route id (departure and arrival
        String routeId = sb.toString();

        try {

            // record flight pk in column family for departures
            insertColumn(keyspace,flightDepartureColumnFamily , departureId,    flightId, flight.getTakeoff(), ts);
                    
            // record flight pk in column family for routes (key on day, departure airport, arrival airport)
            insertColumn(keyspace,flightRouteColumnFamily     , routeId,        flightId, flight.getTakeoff(), ts);

            // record flight details in separate column family
            insertColumn(keyspace,flightColumnFamily,flightId,"carrier",flight.getCarrier(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"flight",flight.getFlight(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"departureAirport",flight.getDepartureAirport(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"departureCity",flight.getDepartureCity(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"departureCountry",flight.getDepartureCountry(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"arrivalAirport",flight.getArrivalAirport(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"arrivalCity",flight.getArrivalCity(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"arrivalCountry",flight.getArrivalCountry(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"takeoff",flight.getTakeoff(),ts);
            insertColumn(keyspace,flightColumnFamily,flightId,"landing",flight.getLanding(),ts);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setFlightColumnFamily(String columnFamily) {
        this.flightColumnFamily = columnFamily;
    }

    public void setFlightRouteColumnFamily(String flightRouteColumnFamily) {
        this.flightRouteColumnFamily = flightRouteColumnFamily;
    }

    public void setFlightDepartureColumnFamily(String flightDepartureColumnFamily) {
        this.flightDepartureColumnFamily = flightDepartureColumnFamily;
    }
}
