package atc.beans;

public class Carrier {

    private String id;
    private String name;
    private String country;
    private String state;

    public Carrier(String id, String name, String country, String state) {
        this.id = id;
        this.name = name;
        this.country = country;
        this.state = state;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }

    public String getState() {
        return state;
    }
}
