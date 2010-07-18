package atc.emitter;

import atc.SVStream;
import atc.beans.Airport;
import atc.consumer.Consumer;

import java.io.IOException;

public class AirportEmitter extends SVStream implements Emitter<Airport> {

    private Consumer<Airport> consumer;

    public AirportEmitter() {
        super("\\|");
    }

    public void setConsumer(Consumer<Airport> consumer) {
        this.consumer = consumer;
    }

    public void begin() {
        try {
            super.init();
            streamToConsumer();
        } catch (IOException e) {
        }
        finally {
            consumer.end();
        }
    }

    private void streamToConsumer() throws IOException {

        consumer.begin();

        while (input.ready()) {
            String[] fields = readLine();
            String type = valueOf("type",fields);
            if (!type.equals("M")) {
                consumer.send(
                        new Airport(
                                valueOf("loc", fields),
                                valueOf("locname", fields),
                                valueOf("latt", fields),
                                valueOf("long", fields),
                                valueOf("country", fields),
                                valueOf("timediv", fields),
                                valueOf("state", fields),
                                valueOf("gmtvar", fields)));
            }
        }
    }
}
