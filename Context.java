import java.util.ArrayList;
import java.util.List;

public class Context {
    private final List<Pair<?, ?>> outputs = new ArrayList<>();
    private final Configuration conf;

    public Context(Configuration conf) {
        this.conf = conf;
    }

    @SuppressWarnings("unchecked")
    public <K, V> void write(K key, V value) {
        outputs.add(new Pair<>(key, value));
    }

    @SuppressWarnings("unchecked")
    public List<Pair<Object, Object>> getOutputs() {
        return (List<Pair<Object, Object>>) (List<?>) outputs;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public void clear() {
        outputs.clear();
    }
}
