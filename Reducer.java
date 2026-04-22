@FunctionalInterface
public interface Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws Exception;
}
