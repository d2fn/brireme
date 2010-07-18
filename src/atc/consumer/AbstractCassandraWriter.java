package atc.consumer;

import atc.CassandraThriftEndPoint;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.transport.TTransportException;


public abstract class AbstractCassandraWriter<T> implements Consumer<T> {

    public static final String UTF8 = "UTF8";

    protected Cassandra.Client client;
    protected CassandraThriftEndPoint cassandra;
    protected String keyspace;

    public void begin() {
        try {
            cassandra.init();
            client = cassandra.getClient();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    protected ColumnPath insertColumn(String keyspace, String columnFamily, String id, String column, String value, long timestamp) {

        ColumnPath path = new ColumnPath(columnFamily);
        try {
            path.setColumn(column.getBytes(UTF8));
            client.insert(keyspace,id,path,value.getBytes(UTF8),timestamp, ConsistencyLevel.ONE);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        return path;
    }

    @Override
    public void end() {
        cassandra.cleanup();
    }

    public void setCassandra(CassandraThriftEndPoint cassandra) {
        this.cassandra = cassandra;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
}
