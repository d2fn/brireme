package atc;

import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SVStream {

    private Resource resource;
    private Map<String,Integer> fieldPositions = new HashMap<String,Integer>(100);
    private String delim;

    protected BufferedReader input;
    protected String line;

    public SVStream(String delim) {
        this.delim = delim;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public void init() throws IOException {

        input = new BufferedReader(new FileReader(resource.getFile()));

        // begin the metadata line
        String[] fieldNames = readLine();
        for(int i = 0; i < fieldNames.length; i++) {
            fieldPositions.put(fieldNames[i],i);
        }
    }

    protected String[] readLine() throws IOException {
        line = input.readLine();
        return line.split(delim);
    }

    protected String valueOf(String name, String[] fields) {
        int index = fieldPositions.get(name);
        if(index >= fields.length) {
            return "";
        }
        return fields[fieldPositions.get(name)].trim();
    }
}
