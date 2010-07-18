package atc;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CassandraThriftEndPoint {

    private String  host;
    private int     port;

    private TTransport tr;
    private Cassandra.Client client;

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void init() throws TTransportException {
        tr = new TSocket(host,port);
        TProtocol proto = new TBinaryProtocol(tr);
        client = new Cassandra.Client(proto);
        tr.open();
    }

    public Cassandra.Client getClient() {
        return client;
    }

    public void cleanup() {
        tr.close();
    }
}
