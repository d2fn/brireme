package atc.beans;

public class Airport {

    private String code;
    private String name;
    private String lat,lng;

    private String country;
    private String timediv;
    private String state;
    private String gmtVar;

    public Airport(String code, String name, String lat, String lng, String country, String timediv, String state, String gmtVar) {
        this.code    = code;
        this.name    = name;
        this.lat     = lat;
        this.lng     = lng;
        this.country = country;
        this.timediv = timediv;
        this.state   = state;
        this.gmtVar  = gmtVar;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getLat() {
        return lat;
    }

    public String getLng() {
        return lng;
    }

    public String getCountry() {
        return country;
    }

    public String getTimeDiv() {
        return timediv;
    }

    public String getState() {
        return state;
    }

    public String getGmtVar() {
        return gmtVar;
    }
}
