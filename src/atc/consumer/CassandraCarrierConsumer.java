package atc.consumer;

import atc.beans.Carrier;

public class CassandraCarrierConsumer extends AbstractCassandraWriter<Carrier> {

    private String columnFamily;

    @Override
    public void send(Carrier carrier) {

        long ts = System.currentTimeMillis();

        insertColumn(keyspace,columnFamily,carrier.getId(),"name",carrier.getName(),ts);
        insertColumn(keyspace,columnFamily,carrier.getId(),"state",carrier.getState(),ts);
        insertColumn(keyspace,columnFamily,carrier.getId(),"country",carrier.getCountry(),ts);
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
