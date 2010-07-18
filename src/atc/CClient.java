package atc;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CClient {

        public static final String UTF8 = "UTF8";

        public static void main(String[] args) throws UnsupportedEncodingException,
                        InvalidRequestException, UnavailableException, TimedOutException,
                        TException, NotFoundException {
                TTransport tr = new TSocket("192.168.205.139", 9160);
                TProtocol proto = new TBinaryProtocol(tr);
                Cassandra.Client client = new Cassandra.Client(proto);
                tr.open();

                String keyspace = "Keyspace1";
                String columnFamily = "Standard1";
                String keyUserID = "1";

                // insert data
                long timestamp = System.currentTimeMillis();

                ColumnPath colPathName = new ColumnPath(columnFamily);
                colPathName.setColumn("fullName".getBytes(UTF8));

                client.insert(keyspace, keyUserID, colPathName, "Chris Goffinet"
                                .getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

                ColumnPath colPathAge = new ColumnPath(columnFamily);
                colPathAge.setColumn("age".getBytes(UTF8));

                client.insert(keyspace, keyUserID, colPathAge, "24".getBytes(UTF8),
                                timestamp, ConsistencyLevel.ONE);

                // read single column
                System.out.println("single column:");
                Column col = client.get(keyspace, keyUserID, colPathName,
                                ConsistencyLevel.ONE).getColumn();

                System.out.println("column name: " + new String(col.name, UTF8));
                System.out.println("column value: " + new String(col.value, UTF8));
                System.out.println("column timestamp: " + new Date(col.timestamp));

                // read entire row
                SlicePredicate predicate = new SlicePredicate();
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(new byte[0]);
                sliceRange.setFinish(new byte[0]);
                predicate.setSlice_range(sliceRange);

                System.out.println("\nrow:");
                ColumnParent parent = new ColumnParent(columnFamily);
                List<ColumnOrSuperColumn> results = client.get_slice(keyspace,
                                keyUserID, parent, predicate, ConsistencyLevel.ONE);
                for (ColumnOrSuperColumn result : results) {
                        Column column = result.column;
                        System.out.println(new String(column.name, UTF8) + " -> "
                                        + new String(column.value, UTF8));
                }
                tr.close();
        }
}