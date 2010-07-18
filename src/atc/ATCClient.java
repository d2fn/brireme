package atc;

import atc.beans.FlightInstance;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.transport.TTransportException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ATCClient {

    private CassandraThriftEndPoint cassandra;
    private Cassandra.Client client;

    public ATCClient(CassandraThriftEndPoint cassandra) {
        this.cassandra = cassandra;
        try {
            this.cassandra.init();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
        this.client = cassandra.getClient();
    }

    public void go() {
        try {
            describeKeyspaces();
            //showRoutes("20100809", "LAX", "LHR", true, 2);
            showRoutes("20100809", "SGF", "LHR", true, 3);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            cassandra.cleanup();
        }
    }

    private List<List<FlightInstance>> getFlights(String day, String dep, String dest, boolean sameCarrier, int hops) throws Exception {

        // holds all verified routes
        List<List<FlightInstance>> options = new ArrayList<List<FlightInstance>>();

        // temporary data structure for passing connecting information
        List<FlightInstance> legs = new ArrayList<FlightInstance>(1);

        List<String> flightIds = getFlights(day, dep);
        for (String flightId : flightIds) {
            String arrivalAirport = getArrivalAirport(flightId);
            if(arrivalAirport.equals(dest)) {
                // build new connection list with only this flight
                List<FlightInstance> flights = new ArrayList<FlightInstance>();
                flights.add(getFlightById(flightId));
                options.add(flights);
            }
            else if(hops > 1) {
                // look at possible destinations connecting from this flight
                FlightInstance flight = getFlightById(flightId);
                legs.add(flight);
                traverseFlights(options, legs, day, dest, sameCarrier, 2, hops);
                legs.remove(flight);
            }
        }
        return options;
    }

    private void traverseFlights(List<List<FlightInstance>> optionList, List<FlightInstance> legs, String day, String arr, boolean sameCarrier, int level, int hops) throws Exception {

        // get the connection information from the last flight and search all outbound flights in search of our ultimate destination
        FlightInstance connectingFlight = legs.get(legs.size()-1);
        String arrivingAt = connectingFlight.getArrivalAirport();
        List<String> flightIds = getFlights(day, arrivingAt);
        for (String flightId : flightIds) {
            FlightInstance flight = getFlightById(flightId);
            FlightInstance lastLeg = legs.get(legs.size()-1);
            if (flight.getArrivalAirport().equals(arr) && flight.happensAfter(lastLeg) && (!sameCarrier || flight.hasSameCarrier(lastLeg))) {
                // build new route with all prior legs, adding this flight to the end
                List<FlightInstance> route = new ArrayList<FlightInstance>(legs.size()+1);
                route.addAll(legs);
                route.add(getFlightById(flightId));
                // copy this route to the verified set that go from dep -> arr
                optionList.add(route);
            }
            else {
                if (level < hops) {
                    legs.add(flight);
                    traverseFlights(optionList,legs,day,arr,sameCarrier,level+1,hops);
                    legs.remove(flight);
                }
            }
        }
    }

    private void showRoutes(String day, String dep, String dest, boolean sameCarrier, int hops) {
        try {
            List<List<FlightInstance>> routes = getFlights(day,dep,dest,sameCarrier,hops);
            for(List<FlightInstance> route : routes) {
                StringBuilder routeStr = new StringBuilder();
                for(int i = 0; i < route.size(); i++) {
                    if(i > 0) {
                        routeStr.append(" -> ");
                    }
                    routeStr.append(route.get(i).toString());
                }
                System.out.println(routeStr);
            }
        } catch (Exception e) {
            System.out.println("Error finding routes: "+e.getMessage());
            e.printStackTrace();
        }
    }

//    private void showRoutes(String day, String departureAirport, String destinationAirport, boolean sameCarrier, int hops) throws Exception {
//        assert (hops > 0);
//
//        for (String flight : getFlights(day, departureAirport)) {
//            String arrivalAirport = getArrivalAirport(flight);
//            for (String secFlight : getFlights(day, arrivalAirport)) {
//                String nextArrival = getArrivalAirport(secFlight);
//                if (nextArrival.equals(destinationAirport)) {
//                    FlightInstance leg2 = getFlightById(secFlight);
//                    FlightInstance leg1 = getFlightById(flight);
//                    if (leg2.getTakeoffTime().after(leg1.getLandingTime()) && (!sameCarrier || leg2.getCarrier().equals(leg1.getCarrier()))) {
//                        showRoute(leg1, leg2);
//                    }
//                }
//            }
//        }
//    }
//
//    private void showRoute(FlightInstance leg1, FlightInstance leg2) {
//
//        DateFormat day = new SimpleDateFormat("MMMM dd");
//        DateFormat time = new SimpleDateFormat("hh:mm a");
//
//        System.out.println(
//                "On " +
//                        day.format(leg1.getTakeoffTime()) +
//                        " at " +
//                        time.format(leg1.getTakeoffTime()) +
//                        " take " +
//                        leg1.getCarrier() + leg1.getFlight() +
//                        " from " + leg1.getDepartureAirport() + " to " + leg1.getArrivalAirport() +
//                        " arriving at " + time.format(leg1.getLandingTime()));
//
//        long layoverms = leg2.getTakeoffTime().getTime() - leg1.getLandingTime().getTime();
//        float hrs = (float) layoverms / 1000f / 60f / 60f;
//        DecimalFormat lyvr = new DecimalFormat("#.##");
//
//        System.out.println("Layover: " + lyvr.format(hrs) + " h");
//
//        System.out.println(
//                "Then at " + time.format(leg2.getTakeoffTime()) + " take " + leg2.getCarrier() + leg2.getFlight() +
//                        " to " + leg2.getArrivalAirport() + " arriving at " + time.format(leg2.getLandingTime()));
//
//        System.out.println("=============================================================================");
//    }

    private void describeKeyspaces() {
        try {
            Set<String> keyspaces = client.describe_keyspaces();
            for (String keyspace : keyspaces) {
                if (keyspace.equals("system")) {
                    continue;
                }
                System.out.println("\n===========================================");
                System.out.println("Keyspace: " + keyspace);
                List<TokenRange> ranges = client.describe_ring(keyspace);
                int i = 1;
                for (TokenRange tokenRange : ranges) {
                    System.out.println("Token Range " + (i++) + ": " + tokenRange.start_token + " -> " + tokenRange.end_token);
                    System.out.println();
                }
            }
            System.out.println();
            String jsonServerList = client.get_string_property("token map");
            System.out.println("token map: " + jsonServerList);
            System.out.println();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 0123456789012345678901234567
     *
     * @param flight = "201010090600-DCA-PHL-US-3692"
     * @return the departure airport code of the given flight
     */
    private String getArrivalAirport(String flight) {
        return flight.substring(17, 20);
    }

    /**
     * @param day              - day in yyyyMMdd
     * @param departureAirport - departure airport code
     * @return a list of flights in the format 201010090600-DCA-PHL-US-3692
     */
    private List<String> getFlights(String day, String departureAirport) {

        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        List<String> connections = new ArrayList<String>();

        try {
            List<ColumnOrSuperColumn> row = client.get_slice("OAG", day + "-" + departureAirport, new ColumnParent("FlightDeparture"), predicate, ConsistencyLevel.ONE);
            for (ColumnOrSuperColumn csc : row) {
                Column c = csc.column;
                String key = new String(c.name, UTF8);
                connections.add(key);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return connections;
    }

    /**
     * => (column=takeoff, value=201010090600, timestamp=1278476592145)
     * => (column=landing, value=201010090705, timestamp=1278476592145)
     * => (column=flight, value=5900, timestamp=1278476592145)
     * => (column=departureCountry, value=US, timestamp=1278476592145)
     * => (column=departureCity, value=WAS, timestamp=1278476592145)
     * => (column=departureAirport, value=DCA, timestamp=1278476592145)
     * => (column=carrier, value=DL, timestamp=1278476592145)
     * => (column=arrivalCountry, value=US, timestamp=1278476592145)
     * => (column=arrivalCity, value=NYC, timestamp=1278476592145)
     * => (column=arrivalAirport, value=LGA, timestamp=1278476592145)
     */
    private FlightInstance getFlightById(String id) throws Exception {

        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        FlightInstance flight = new FlightInstance();
        List<ColumnOrSuperColumn> row = client.get_slice("OAG", id, new ColumnParent("Flight"), predicate, ConsistencyLevel.ONE);
        for (ColumnOrSuperColumn csc : row) {
            Column c = csc.column;
            String name = new String(c.name, UTF8);
            String value = new String(c.value, UTF8);
            BeanUtil.setObjectAttribute(flight, name, value);
        }

        return flight;
    }

    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext("atc/cassandra.xml");
        CassandraThriftEndPoint cassandra = (CassandraThriftEndPoint) context.getBean("cassandra");
        new ATCClient(cassandra).go();
    }

    public static final String UTF8 = "UTF8";
}
