package atc.beans;

import java.util.Date;

public class FlightSchedule {

    private String carrier;
    private String flightNumber;
    private String departureAirport;
    private String departureCity;
    private String departureCountry;
    private String arrivalAirport;
    private String arrivalCity;
    private String arrivalCountry;
    private String departureTime;
    private String arrivalTime;
    private Date   effectiveFrom;
    private Date   effectiveTo;
    private byte   days;

    public FlightSchedule(String carrier, String flightNumber, String departureAirport, String departureCity, String departureCountry, String arrivalAirport, String arrivalCity, String arrivalCountry, String departureTime, String arrivalTime, Date effectiveFrom, Date effectiveTo, byte days) {
        this.carrier = carrier;
        this.flightNumber = flightNumber;
        this.departureAirport = departureAirport;
        this.departureCity = departureCity;
        this.departureCountry = departureCountry;
        this.arrivalAirport = arrivalAirport;
        this.arrivalCity = arrivalCity;
        this.arrivalCountry = arrivalCountry;
        this.departureTime = departureTime;
        this.arrivalTime = arrivalTime;
        this.effectiveFrom = effectiveFrom;
        this.effectiveTo = effectiveTo;
        this.days = days;
    }

    public String getCarrier() {
        return carrier;
    }

    public String getFlightNumber() {
        return flightNumber;
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

    public String getDepartureTime() {
        return departureTime;
    }

    public int getTakeoffHrs() {
        return Integer.parseInt(departureTime.substring(0,2));
    }

    public int getTakeoffMin() {
        return Integer.parseInt(departureTime.substring(2));
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public int getLandingHrs() {
        return Integer.parseInt(arrivalTime.substring(0,2));
    }

    public int getLandingMin() {
        return Integer.parseInt(arrivalTime.substring(2));
    }

    public Date getEffectiveFrom() {
        return effectiveFrom;
    }

    public Date getEffectiveTo() {
        return effectiveTo;
    }

    public byte getDays() {
        return days;
    }
}
