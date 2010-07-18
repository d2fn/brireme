package atc.emitter;

import atc.SVStream;
import atc.beans.FlightInstance;
import atc.consumer.Consumer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class FlightInstanceEmitter extends SVStream implements Emitter<FlightInstance> {

    private DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
    private Consumer<FlightInstance> consumer;

    int count = 0;
    private String minDate;
    private String maxDate;
    private List<String> codes;

    public FlightInstanceEmitter() {
        super("\t");
    }

    public void setConsumer(Consumer<FlightInstance> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void begin() {

        consumer.begin();

        try {
            super.init();
            
            while(input.ready()) {
                String[] fields = readLine();

                if(filter(fields)) {

                    FlightInstance flight =
                            new FlightInstance(
                                    valueOf("carrier",fields),
                                    valueOf("flight",fields),
                                    valueOf("departureAirport",fields),
                                    valueOf("departureCity",fields),
                                    valueOf("departureCountry",fields),
                                    valueOf("arrivalAirport",fields),
                                    valueOf("arrivalCity",fields),
                                    valueOf("arrivalCountry",fields),
                                    df.parse(valueOf("takeoff",fields)),
                                    df.parse(valueOf("landing",fields)));
                    consumer.send(flight);

                    count++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException pe) {
            throw new RuntimeException(pe);
        } finally {
            consumer.end();
        }

    }

    private boolean filter(String[] fields) {

        String takeoff = valueOf("takeoff",fields);

        if(minDate != null && takeoff.compareTo(minDate) < 0) {
            return false;
        }

        if(maxDate != null && takeoff.compareTo(maxDate) > 0) {
            return false;
        }

        String departureAirport = valueOf("departureAirport",fields);
        String arrivalAirport = valueOf("arrivalAirport",fields);
        if(!codes.contains(departureAirport) && !codes.contains(arrivalAirport)) {
            return false;
        }

        return true;
    }

    public void setMinDate(String minDate) {
        this.minDate = minDate;
    }

    public void setMaxDate(String maxDate) {
        this.maxDate = maxDate;
    }

    /**
     * @param codes - only consume flights arriving or departing these airport codes
     *
     */
    public void setAirportFilter(List<String> codes) {
        this.codes = codes;
    }
}
