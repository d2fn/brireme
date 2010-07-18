package atc.consumer;

import atc.beans.Airport;
import org.apache.cassandra.thrift.*;

import java.util.List;

public class CassandraAirportConsumer extends AbstractCassandraWriter<Airport> {

    private String columnFamily;

    public void send(Airport airport) {

        long timestamp = System.currentTimeMillis();

        insertColumn(keyspace,columnFamily,airport.getCode(),"name",airport.getName(),timestamp);
        insertColumn(keyspace,columnFamily,airport.getCode(),"country",airport.getCountry(),timestamp);
        insertColumn(keyspace,columnFamily,airport.getCode(),"state",airport.getState(),timestamp);
        insertColumn(keyspace,columnFamily,airport.getCode(),"lat",airport.getLat(),timestamp);
        insertColumn(keyspace,columnFamily,airport.getCode(),"lng",airport.getLng(),timestamp);
        insertColumn(keyspace,columnFamily,airport.getCode(),"gmtVar",airport.getGmtVar(),timestamp);
        insertColumn(keyspace,columnFamily,airport.getCode(),"timeDiv",airport.getTimeDiv(),timestamp);
    }

    public void end() {

        // print some of the data...

        SlicePredicate predicate = new SlicePredicate();
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(new byte[0]);
                sliceRange.setFinish(new byte[0]);
                predicate.setSlice_range(sliceRange);

        System.out.println("\nrow:");
        ColumnParent parent = new ColumnParent(columnFamily);

        try {
            List<ColumnOrSuperColumn> results = client.get_slice(keyspace,"SEA", parent, predicate, ConsistencyLevel.ONE);
            for (ColumnOrSuperColumn result : results) {
                Column column = result.column;
                System.out.println(new String(column.name, UTF8) + " -> " + new String(column.value, UTF8));
            }
        }
        catch(Exception e) { }
        finally { super.end(); }
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
