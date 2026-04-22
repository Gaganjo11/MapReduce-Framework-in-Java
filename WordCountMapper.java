public class WordCountMapper implements Mapper<Long, String, String, Integer> {
    @Override
    public void map(Long key, String value, Context context) {
        String[] words = value.toLowerCase().split("\\W+");
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(word, 1);
            }
        }
    }
}
