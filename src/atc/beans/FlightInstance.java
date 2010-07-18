package atc.beans;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FlightInstance {

    private String carrier;
    private String flight;
    private String departureAirport;
    private String departureCity;
    private String departureCountry;
    private String arrivalAirport;
    private String arrivalCity;
    private String arrivalCountry;
    private Date   takeoff;
    private Date   landing;

    private DateFormat df;

    public FlightInstance() {
        df = new SimpleDateFormat("yyyyMMddHHmm");
    }

    public FlightInstance(String carrier, String flight, String departureAirport, String departureCity, String departureCountry, String arrivalAirport, String arrivalCity, String arrivalCountry, Date takeoff, Date landing) {
        this();
        this.carrier = carrier;
        this.flight = flight;
        this.departureAirport = departureAirport;
        this.departureCity = departureCity;
        this.departureCountry = departureCountry;
        this.arrivalAirport = arrivalAirport;
        this.arrivalCity = arrivalCity;
        this.arrivalCountry = arrivalCountry;
        this.takeoff = takeoff;
        this.landing = landing;
    }

    public String getCarrier() {
        return carrier;
    }

    public String getFlight() {
        return flight;
    }

    public String getDepartureAirport() {
        return departureAirport;
    }

    public String getDepartureCity() {
        return departureCity;
    }

    public String getDepartureCountry() {
        return departureCountry;
    }

    public String getArrivalAirport() {
        return arrivalAirport;
    }

    public String getArrivalCity() {
        return arrivalCity;
    }

    public String getArrivalCountry() {
        return arrivalCountry;
    }

    public Date getTakeoffTime() {
        return takeoff;
    }

    public String getTakeoff() {
        return df.format(takeoff);
    }

    public Date getLandingTime() {
        return landing;
    }

    public String getLanding() {
        return df.format(landing);
    }

    public String getId() {
        return toString();
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public void setFlight(String flight) {
        this.flight = flight;
    }

    public void setDepartureAirport(String departureAirport) {
        this.departureAirport = departureAirport;
    }

    public void setDepartureCity(String departureCity) {
        this.departureCity = departureCity;
    }

    public void setDepartureCountry(String departureCountry) {
        this.departureCountry = departureCountry;
    }

    public void setArrivalAirport(String arrivalAirport) {
        this.arrivalAirport = arrivalAirport;
    }

    public void setArrivalCity(String arrivalCity) {
        this.arrivalCity = arrivalCity;
    }

    public void setArrivalCountry(String arrivalCountry) {
        this.arrivalCountry = arrivalCountry;
    }

    public void setTakeoff(String takeoff) {
        try {
            this.takeoff = df.parse(takeoff);
        } catch (ParseException e) {
            System.out.println("Date not set, takeoff format invalid: "+e);
        }
    }

    public void setLanding(String landing) {
        try {
            this.landing = df.parse(landing);
        } catch (ParseException e) {
            System.out.println("Date not set, landing format invalid: "+e);
        }
    }

    public boolean happensBefore(FlightInstance next) {
        return getLandingTime().before(next.getTakeoffTime());
    }

    public boolean happensAfter(FlightInstance last) {
        return getTakeoffTime().after(last.getLandingTime());
    }

    public boolean hasSameCarrier(FlightInstance f) {
        return f.getCarrier().equals(getCarrier());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getTakeoff());
        sb.append("-");
        sb.append(departureAirport);
        sb.append("-");
        sb.append(arrivalAirport);
        sb.append("-");
        sb.append(carrier);
        sb.append("-");
        sb.append(flight);
        return sb.toString();
    }
}
