package atc;

import atc.beans.FlightInstance;
import atc.beans.FlightSchedule;

import java.util.*;

public class FlightScheduleExpander implements Iterator<FlightInstance> {

    private FlightSchedule sched;
    private Calendar dayPointer;

    private FlightInstance flight;
    private boolean terminated = false;

    public FlightScheduleExpander(FlightSchedule sched) {
        this.sched = sched;
        this.dayPointer = Calendar.getInstance();
        dayPointer.setTime(this.sched.getEffectiveFrom());
    }

    private boolean include() {

        int dayOfWeek = dayPointer.get(Calendar.DAY_OF_WEEK);
        int mask = DayBitSets.getMask(dayOfWeek);

        // increment to next day for next check
        dayPointer.add(Calendar.DAY_OF_YEAR,1);

        return (mask & sched.getDays()) == mask;
    }

    private boolean seek() {
        while(!dayPointer.getTime().after(sched.getEffectiveTo()) && dayPointer.get(Calendar.YEAR) <= 2010) {
            if(include()) {

                Calendar takeoff = Calendar.getInstance();
                takeoff.setTime(dayPointer.getTime());
                takeoff.set(Calendar.HOUR,sched.getTakeoffHrs());
                takeoff.set(Calendar.MINUTE,sched.getTakeoffMin());
                // do normalization to GMT here based on departure airport

                Calendar landing = Calendar.getInstance();
                landing.setTime(dayPointer.getTime());
                landing.set(Calendar.HOUR,sched.getLandingHrs());
                landing.set(Calendar.MINUTE,sched.getLandingMin());
                // do normalization to GMT here based on arrival airport

                flight = new FlightInstance(
                                sched.getCarrier(),
                                sched.getFlightNumber(),
                                sched.getDepartureAirport(),
                                sched.getDepartureCity(),
                                sched.getDepartureCountry(),
                                sched.getArrivalAirport(),
                                sched.getArrivalCity(),
                                sched.getArrivalCountry(),
                                takeoff.getTime(),landing.getTime());
                
                return true;

            }
        }
        return false;
    }

    public boolean hasNext() {

        if(terminated) {
            return false;
        }
        if(flight != null) {
            return true;
        }

        // seek to next flight instance
        while(!terminated) {
            if(seek()) {
                return true;
            }
            else {
                terminated = true;
            }
        }

        return false;
    }

    public FlightInstance next() {
        
        if(terminated) {
            assert(flight == null);
            throw new UnsupportedOperationException("Iterator exhausted");
        }

        FlightInstance ret = flight;
        flight = null;
        return ret;
    }

    public void remove() {
        throw new UnsupportedOperationException("remove() not implemented");
    }
}
