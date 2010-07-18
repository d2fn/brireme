package atc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class DayBitSets {

    public static final byte MONDAY   = 1;
    public static final byte TUESDAY    ;
    public static final byte WEDNESDAY;
    public static final byte THURSDAY;
    public static final byte FRIDAY;
    public static final byte SATURDAY;
    public static final byte SUNDAY;

    private static final Map<Integer,Byte> maskLookup;

    static {
        TUESDAY   = MONDAY           << 1;
        WEDNESDAY = (byte)(TUESDAY   << 1);
        THURSDAY  = (byte)(WEDNESDAY << 1);
        FRIDAY    = (byte)(THURSDAY  << 1);
        SATURDAY  = (byte)(FRIDAY    << 1);
        SUNDAY    = (byte)(SATURDAY  << 1);

        maskLookup = new HashMap<Integer,Byte>();
        maskLookup.put(Calendar.MONDAY    , MONDAY);
        maskLookup.put(Calendar.TUESDAY   , TUESDAY);
        maskLookup.put(Calendar.WEDNESDAY , WEDNESDAY);
        maskLookup.put(Calendar.THURSDAY  , THURSDAY);
        maskLookup.put(Calendar.FRIDAY    , FRIDAY);
        maskLookup.put(Calendar.SATURDAY  , SATURDAY);
        maskLookup.put(Calendar.SUNDAY    , SUNDAY);
    }

    public static byte get(String daysScheduled) {

        byte days = 0;

        char[] c = daysScheduled.toCharArray();
        for(int i = 0; i < c.length; i++) {
            if(i == 0 && c[i] == '1') {
                days |= MONDAY;
            }
            else if(i == 1 && c[i] == '2') {
                days |= TUESDAY;
            }
            else if(i == 2 && c[i] == '3') {
                days |= WEDNESDAY;
            }
            else if(i == 3 && c[i] == '4') {
                days |= THURSDAY;
            }
            else if(i == 4 && c[i] == '5') {
                days |= FRIDAY;
            }
            else if(i == 5 && c[i] == '6') {
                days |= SATURDAY;
            }
            else if(i == 6 && c[i] == '7') {
                days |= SUNDAY;
            }
        }

        return days;
    }

    public static int getMask(int calendarField) {
        return maskLookup.get(calendarField);
    }

    private static String printbits(byte bits) {

        byte mask = 1;
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < 8; i++) {
            sb.insert(0,(mask&bits)==mask?"1":"0");
            mask = (byte)(mask << 1);
        }

        return sb.toString();
    }

    public static final void main(String[] args) throws Exception {

        // visual self test
        System.out.println("Self Test");
        for(int i = 0; i < 8; i++) {
            int bits = 1<<i;
            System.out.println(pad(3,bits)+" => "+printbits((byte)(1<<i)));
        }

        System.out.println();
        System.out.println("Testing possible day combinations");

        BufferedReader input = new BufferedReader(new FileReader("/Users/dietrich/scripts/oag/data/days.txt"));
        while(input.ready()) {
            String line = input.readLine();
            byte bits = get(line);
            System.out.println(fill(7,line) + " => " + printbits(bits));
        }
        input.close();
    }

    private static String pad(int chars, int n) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < chars; i++) {
            if(n < Math.pow(10,i)) {
                sb.append(" ");
            }
        }
        sb.append(n);
        return sb.toString();
    }

    private static String fill(int chars, String s) {
        StringBuilder sb = new StringBuilder(s);
        for(int i = 0; i < chars-s.length(); i++) {
            sb.append(" ");
        }
        return sb.toString();
    }
}
