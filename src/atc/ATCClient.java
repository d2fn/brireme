package atc;

import atc.beans.FlightInstance;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.transport.TTransportException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class ATCClient {

    private Cassandra.Client client;

    public ATCClient(String contextPath, String cassandraBean, String day, String dep, String arr, boolean sameCarrier, int hops) {

        ApplicationContext context = new ClassPathXmlApplicationContext(contextPath);
        CassandraThriftEndPoint cassandra = (CassandraThriftEndPoint)context.getBean(cassandraBean);

        try {
            cassandra.init();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
        this.client = cassandra.getClient();

        showRoutes(day,dep,arr,sameCarrier,hops);

        cassandra.cleanup();
    }

    private List<List<FlightInstance>> getFlights(String day, String dep, String dest, boolean sameCarrier, int hops) throws Exception {

        // holds all verified routes
        List<List<FlightInstance>> options = new ArrayList<List<FlightInstance>>();

        // temporary data structure for passing connecting information
        Stack<FlightInstance> legs = new Stack<FlightInstance>();

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
                legs.push(getFlightById(flightId));
                traverseFlights(options, legs, day, dest, sameCarrier, 2, hops);
                legs.pop();
            }
        }
        return options;
    }

    private void traverseFlights(List<List<FlightInstance>> optionList, Stack<FlightInstance> legs, String day, String arr, boolean sameCarrier, int level, int hops) throws Exception {

        // get the connection information from the last flight and search all outbound flights in search of our ultimate destination
        FlightInstance lastLeg = legs.get(legs.size()-1);
        String arrivingAt = lastLeg.getArrivalAirport();
        List<String> flightIds = getFlights(day, arrivingAt);
        for (String flightId : flightIds) {
            FlightInstance flight = getFlightById(flightId);
            if(flight.happensAfter(lastLeg)) {
                if (flight.getArrivalAirport().equals(arr) && (!sameCarrier || flight.hasSameCarrier(lastLeg))) {
                    // build new route with all prior legs, adding this flight to the end
                    List<FlightInstance> route = new ArrayList<FlightInstance>(legs.size()+1);
                    route.addAll(legs);
                    route.add(flight);
                    // copy this route to the verified set that go from dep -> arr
                    optionList.add(route);
                }
                else if (level < hops) {
                    legs.push(flight);
                    traverseFlights(optionList,legs,day,arr,sameCarrier,level+1,hops);
                    legs.pop();
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
                    routeStr.append(route.get(i).getRouteString());
                }
                System.out.println(routeStr);
            }
        } catch (Exception e) {
            System.out.println("Error finding routes: "+e.getMessage());
            e.printStackTrace();
        }
    }

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

        new ATCClient(
                args[0], // spring context resource
                args[1], // cassandra endpoint bean id
                args[2], // day
                args[3], // departure airport code
                args[4], // arrival airport code
                Boolean.valueOf(args[5]), // search same carrier
                Integer.parseInt(args[6]) // number of hops
        );
    }

    public static final String UTF8 = "UTF8";
}
