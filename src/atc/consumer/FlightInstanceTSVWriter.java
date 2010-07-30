package atc.consumer;

import atc.beans.FlightInstance;
import org.apache.log4j.Logger;
import org.springframework.core.io.Resource;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class FlightInstanceTSVWriter implements Consumer<FlightInstance> {

    private Resource output;
    private PrintWriter writer;
    private Consumer<FlightInstance> nextConsumer;
    private boolean append = false;

    public void begin() {

        StringBuilder mdline = new StringBuilder();
        mdline.append("id\t")
                .append("carrier\t")
                .append("flight\t")
                .append("takeoff\t")
                .append("landing\t")
                .append("departureAirport\t")
                .append("departureCity\t")
                .append("departureCountry\t")
                .append("arrivalAirport\t")
                .append("arrivalCity\t")
                .append("arrivalCountry");
        try {
            writer = new PrintWriter(new FileOutputStream(output.getFile(),append));
            writer.println(mdline.toString());
            if(nextConsumer != null) {
                nextConsumer.begin();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            e.printStackTrace(System.err);
        }
    }

    public void send(FlightInstance flight) {

        StringBuilder line = new StringBuilder();
        line.append(flight.toString()).append("\t");
        line.append(flight.getCarrier()).append("\t");
        line.append(flight.getFlight()).append("\t");
        line.append(flight.getTakeoff()).append("\t");
        line.append(flight.getLanding()).append("\t");
        line.append(flight.getDepartureAirport()).append("\t");
        line.append(flight.getDepartureCity()).append("\t");
        line.append(flight.getDepartureCountry()).append("\t");
        line.append(flight.getArrivalAirport()).append("\t");
        line.append(flight.getArrivalCity()).append("\t");
        line.append(flight.getArrivalCountry()).append("\t");
        
        writer.println(line);

        if(nextConsumer != null) {
            nextConsumer.send(flight);
        }
    }

    public void end() {
        writer.close();

        if(nextConsumer != null) {
            nextConsumer.end();
        }
    }

    public void setOutput(Resource output) {
        this.output = output;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public void setConsumer(Consumer<FlightInstance> consumer) {
        this.nextConsumer = consumer;
    }

    private static final Logger logger = Logger.getLogger(FlightInstanceTSVWriter.class); 
}
