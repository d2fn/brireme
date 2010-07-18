package atc.emitter;

import atc.SVStream;
import atc.beans.Carrier;
import atc.consumer.Consumer;

import java.io.IOException;

public class CarrierEmitter extends SVStream implements Emitter<Carrier> {

    private Consumer<Carrier> consumer;

    public CarrierEmitter() {
        super("\\|");
    }

    @Override
    public void begin() {
        consumer.begin();
        try {
            super.init();
            while(input.ready()) {
                String[] fields = readLine();
                Carrier c = new Carrier(
                        valueOf("carcode",fields),
                        valueOf("carname",fields),
                        valueOf("ctrycode",fields),
                        valueOf("us_state",fields));
                consumer.send(c);
            }
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            consumer.end();
        }
    }

    public void setConsumer(Consumer<Carrier> consumer) {
        this.consumer = consumer;
    }
}
