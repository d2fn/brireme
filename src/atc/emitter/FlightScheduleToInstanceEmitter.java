package atc.emitter;

import atc.DayBitSets;
import atc.FlightScheduleExpander;
import atc.SVStream;
import atc.beans.FlightInstance;
import atc.beans.FlightSchedule;
import atc.consumer.Consumer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FlightScheduleToInstanceEmitter extends SVStream implements Emitter<FlightInstance> {

    private Consumer<FlightInstance> consumer;

    private FlightScheduleExpander expander;
    private DateFormat df;

    public FlightScheduleToInstanceEmitter() {
        super("\\|");
    }

    public void setConsumer(Consumer<FlightInstance> consumer) {
        this.consumer = consumer;
    }

    public void begin() {

        df = new SimpleDateFormat("yyyyMMdd");

        consumer.begin();

        try {
            super.init();
            streamToConsumer();
        } catch (IOException e) { }
        finally { consumer.end(); }
    }

    private void streamToConsumer() throws IOException {

        String[] fields = {};

        while(input.ready()) {

            fields = readLine();
            if(valueOf("duplicate",fields).equals("D") || valueOf("dupcar1",fields).trim().length() == 0) {
                // skip
            }
            else {

                String effectiveFrom = valueOf("effectiveFrom",fields);
                String effectiveTo   = valueOf("effectiveTo",fields);
                String daysScheduled = valueOf("days",fields);

                byte days = DayBitSets.get(daysScheduled);

                Date from = null;
                Date to   = null;
                try {
                    from = df.parse(effectiveFrom);
                    to = df.parse(effectiveTo);
                } catch (ParseException e) {
                    System.out.println("error reading date, skipping line ("+from+","+to+")");
                    continue;
                }

                FlightSchedule sched =
                        new FlightSchedule(
                                valueOf("carrier",fields),
                                valueOf("flightNumber",fields),
                                valueOf("departureAirport",fields),
                                valueOf("departureCity",fields),
                                valueOf("departureCountry",fields),
                                valueOf("arrivalAirport",fields),
                                valueOf("arrivalCity",fields),
                                valueOf("arrivalCountry",fields),
                                valueOf("departureTime",fields),
                                valueOf("arrivalTime",fields),
                                from, to, days );

                expander = new FlightScheduleExpander(sched);

                while(expander.hasNext()) {
                    consumer.send(expander.next());
                }
            }
        }
    }
}
