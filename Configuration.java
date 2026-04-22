import java.util.HashMap;
import java.util.Map;

public class Configuration {
    private final Map<String, String> properties = new HashMap<>();

    public void set(String key, String value) {
        properties.put(key, value);
    }

    public String get(String key) {
        return properties.get(key);
    }

    public int getInt(String key, int defaultValue) {
        String val = properties.get(key);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }
}
