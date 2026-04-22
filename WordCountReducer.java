public class WordCountReducer implements Reducer<String, Integer, String, Integer> {
    @Override
    public void reduce(String key, Iterable<Integer> values, Context context) {
        int sum = 0;
        for (Integer val : values) {
            sum += val;
        }
        context.write(key, sum);
    }
}
